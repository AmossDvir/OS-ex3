#include "iostream"
#include <pthread.h>
#include <algorithm>
#include <atomic>
#include <set>
#include <unordered_map>
#include "MapReduceFramework.h"

typedef void *JobHandle;

#define SUCCESS 0
#define ERROR 1

// The Barrier class, creates a barrier in code so all threads must arrive to this point and continue together
class Barrier {
  pthread_mutex_t mutex;
  pthread_cond_t cv;
  int count;
  int numThreads;

 public:
  Barrier (int numThreads)
      : mutex (PTHREAD_MUTEX_INITIALIZER), cv (PTHREAD_COND_INITIALIZER), count (0),
        numThreads (numThreads)
  {
  }

  ~Barrier ()
  {
    int res = 0;
    res = pthread_mutex_destroy (&mutex);
    if (res != 0)
      {
        fprintf (stderr, "[[Barrier]] error on pthread_mutex_destroy");
        exit (ERROR);
      }
    if (pthread_cond_destroy (&cv) != SUCCESS)
      {
        fprintf (stderr, "[[Barrier]] error on pthread_cond_destroy");
        exit (ERROR);
      }
  }

  void barrier ()
  {
    if (pthread_mutex_lock (&mutex) != SUCCESS)
      {
        fprintf (stderr, "[[Barrier]] error on pthread_mutex_lock");
        exit (ERROR);
      }
    if (++count < numThreads)
      {
        if (pthread_cond_wait (&cv, &mutex) != SUCCESS)
          {
            fprintf (stderr, "[[Barrier]] error on pthread_cond_wait");
            exit (ERROR);
          }
      }
    else
      {
        count = 0;
        if (pthread_cond_broadcast (&cv) != SUCCESS)
          {
            fprintf (stderr, "[[Barrier]] error on pthread_cond_broadcast");
            exit (ERROR);
          }
      }
    if (pthread_mutex_unlock (&mutex) != SUCCESS)
      {
        fprintf (stderr, "[[Barrier]] error on pthread_mutex_unlock");
        exit (ERROR);
      }
  }
};

struct JobContext;

struct ThreadContext {
    int threadId{};
    JobContext *job{};
    const MapReduceClient *client{};
    const InputVec *inputVec{};
    IntermediateVec threadVector;
};

void validate (int res, const std::string &message)
{
  if (res != SUCCESS)
    {
      std::cerr << "system error: " << message << " (with code: " << res << ")"
                << std::endl;
      exit (ERROR);
    }
}

bool keysComp (const K2 *&key1, const K2 *&key2)
{
  return *key1 < *key2;
}

struct JobContext {
    ThreadContext *contexts;
    std::vector<IntermediateVec> *interVecSorted;
    std::vector<IntermediateVec> *interVecShuffled;
    OutputVec *outVec;
    pthread_t *threads;
    int threadsNum;
    pthread_mutex_t *stateMutex;
    pthread_mutex_t *sortMutex;
    pthread_mutex_t *outputMutex;
    pthread_mutex_t *waitMutex;
    std::atomic<stage_t> *stage;
    std::atomic<int> *processed;
    std::atomic<unsigned long> *total;
    std::atomic<int> *mapIndex;
    std::atomic<int> *reduceIndex;
    std::atomic<int> *pairsCount;
    std::atomic<bool> *jobAlreadyWaiting;
    Barrier *barrier;

    //  Destructor:
    ~JobContext ()
    {
      delete interVecSorted;
      delete interVecShuffled;
      delete[] threads;

      delete stage;
      delete processed;
      delete total;
      delete mapIndex;
      delete reduceIndex;
      delete pairsCount;
      delete jobAlreadyWaiting;

      for (int i = 0; i < threadsNum; ++i)
        {
          contexts[i].job = nullptr;
        }
      delete[] contexts;
      delete barrier;
      //        Mutex deletion:
      validate (pthread_mutex_destroy (stateMutex), "stateMutex destruction failed");
      validate (pthread_mutex_destroy (sortMutex), "sortMutex destruction failed");
      validate (pthread_mutex_destroy (outputMutex), "outputMutex destruction failed");
      validate (pthread_mutex_destroy (waitMutex), "jobWaitMutex destruction failed");
      contexts = nullptr;
      interVecSorted = nullptr;
      interVecShuffled = nullptr;
      threads = nullptr;
      stateMutex = nullptr;
      sortMutex = nullptr;
      outputMutex = nullptr;
      waitMutex= nullptr;
      stage = nullptr;
      processed = nullptr;
      total = nullptr;
      mapIndex = nullptr;
      reduceIndex = nullptr;
      pairsCount = nullptr;
      jobAlreadyWaiting = nullptr;
      barrier = nullptr;
    }
};


// Helper functions:

K2 *findMax (const std::vector<IntermediateVec> *intermediaryVectors)
{
  K2 *maxKey = nullptr;
  for (const auto &vector: *intermediaryVectors)
    {
      if (!vector.empty ())
        {
          K2 *key = vector.back ().first;
          if (maxKey == nullptr || *maxKey < *key)
            {
              maxKey = key;
            }
        }
    }
  return maxKey;
}

void resetState (JobContext *job, const stage_t &stage, unsigned long total)
{
  pthread_mutex_lock (job->stateMutex);
  *job->stage = stage;
  *job->processed = 0;
  *job->total = total;
  pthread_mutex_unlock (job->stateMutex);
}

// Handler functions:

void handleMap (ThreadContext *threadContext)
{
  unsigned long index = 0;
  while ((index = threadContext->job->mapIndex->fetch_add (1))
         < threadContext->inputVec->size ())
    {
      InputPair inputPair = (*threadContext->inputVec)[index];
      threadContext->job->processed->fetch_add (1);
      threadContext->client->map (inputPair.first, inputPair.second, threadContext);//?
    }
}

void handleSort (ThreadContext *threadContext)
{
  std::sort (threadContext->threadVector.begin (), threadContext->threadVector.end (),
             [] (const IntermediatePair &pair1, const IntermediatePair &pair2) -> bool
             {
                 return *pair1.first < *pair2.first;
             });
  if (!threadContext->threadVector.empty ())
    {
      // ADD MUTEX
      pthread_mutex_lock (threadContext->job->sortMutex);
      // Add the local thread vector to the main vector of the program:
      threadContext->job->interVecSorted->push_back (threadContext->threadVector);
      pthread_mutex_unlock (threadContext->job->sortMutex);
    }
}

void handleShuffle (JobContext &job)
{
  // Shuffling the Thread zero's vector (only after all threads are sorted):
  int sortedVecSize = (int) job.interVecSorted->size ();
  int counter = 0;
  while (counter < sortedVecSize)
    {
      K2 *maxKey = findMax (job.interVecSorted);
      auto tempVec = IntermediateVec ();
      for (int i = 0; i < sortedVecSize; i++)
        {
          IntermediateVec &vec = (*job.interVecSorted)[i];
          if (vec.empty ())
            {
              continue;
            }
          while (!vec.empty () && (!(*(vec.back ().first) < *maxKey)
                                   && !(*maxKey < *(vec.back ().first))))
            {
              tempVec.push_back (vec.back ());
              vec.pop_back ();
              job.processed->fetch_add (1);
            }
          if (vec.empty ())
            {
              counter++;
            }
        }
      if (!tempVec.empty ())
        {
          job.interVecShuffled->push_back (tempVec);
        }
    }
}

void handleReduce (ThreadContext *threadContext)
{

  unsigned long index = 0;
  while ((index = threadContext->job->reduceIndex->fetch_add (1))
         < threadContext->job->interVecShuffled->size ())
    {
      pthread_mutex_lock (threadContext->job->stateMutex);
      const IntermediateVec *vecToReduce = &(*threadContext->job->interVecShuffled)[index];
      int addToProcessed = vecToReduce->size ();

      threadContext->client->reduce (vecToReduce, threadContext);
      *threadContext->job->processed += addToProcessed;
      pthread_mutex_unlock (threadContext->job->stateMutex);
    }
}

void *threadMainFlow (void *arg)
{
  auto *threadContext = (ThreadContext *) arg;

  //  map:
  handleMap (threadContext);

  // sort:
  handleSort (threadContext);

  // shuffle:
  threadContext->job->barrier->barrier ();
  if (threadContext->threadId == 0)
    {
      resetState ((threadContext->job), SHUFFLE_STAGE, (unsigned long) *threadContext->job->pairsCount);
      handleShuffle (*threadContext->job);
      resetState ((threadContext->job), REDUCE_STAGE, (unsigned long) *threadContext->job->pairsCount);
    }
  threadContext->job->barrier->barrier ();

  //    reduce:
  handleReduce (threadContext);
  return nullptr;
}

void
initializeThreadsContexts (JobContext *job, const MapReduceClient &client, const InputVec &inputVec,
                           int threadsNum)
{
  for (int i = 0; i < threadsNum; i++)
    {
      ThreadContext thrCon;
      thrCon.threadId = i;
      thrCon.client = &client;
      thrCon.inputVec = &inputVec;
      thrCon.job = job;
      thrCon.threadVector = IntermediateVec ();
      job->contexts[i] = thrCon;
    }
}

void
initializeThreads (JobContext *job)
{
  // create 'multiThreadLevel' threads:
  for (int i = 0; i < job->threadsNum; i++)
    {
      job->contexts[i].threadId = i;
      validate (pthread_create (&job->threads[i], nullptr, threadMainFlow, &job->contexts[i]),
                "Thread creation failed");
    }
}

void
initializeJobContext (JobContext *job, const InputVec &inputVec, int
multiThreadLevel, OutputVec &outputVec)
{
  job->stateMutex = new pthread_mutex_t ();
  job->sortMutex = new pthread_mutex_t ();

  job->outputMutex = new pthread_mutex_t ();
  job->waitMutex = new pthread_mutex_t ();
//  job->jobWaitMutex = new pthread_mutex_t ();
  job->stage = new std::atomic<stage_t> (UNDEFINED_STAGE);
  job->processed = new std::atomic<int> (0);
  job->total = new std::atomic<unsigned long> (inputVec.size ());
  job->mapIndex = new std::atomic<int> (0);
  job->reduceIndex = new std::atomic<int> (0);
  job->pairsCount = new std::atomic<int> (0);
  job->interVecSorted = new std::vector<IntermediateVec> ();
  job->interVecShuffled = new std::vector<IntermediateVec> ();
  job->jobAlreadyWaiting = new std::atomic<bool> (false);
  job->outVec = &outputVec;//todo i change here instead of creating a new
  // one
  pthread_mutex_init (job->stateMutex, nullptr);
  pthread_mutex_init (job->sortMutex, nullptr);
  pthread_mutex_init (job->outputMutex, nullptr);
  pthread_mutex_init (job->waitMutex, nullptr);
//  pthread_mutex_init (job->jobWaitMutex, nullptr);
  job->barrier = new Barrier (multiThreadLevel);
  job->contexts = new ThreadContext[multiThreadLevel];
  job->threads = new pthread_t[multiThreadLevel];
  job->threadsNum = multiThreadLevel;
}

void emit2 (K2 *key, V2 *value, void *context)
{
  auto *threadContext = (ThreadContext *) context;
  auto &threadVec = threadContext->threadVector;
  threadContext->job->pairsCount->fetch_add (1);
  threadVec.emplace_back (key, value);
}

void emit3 (K3 *key, V3 *value, void *context)
{
  auto *threadContext = (ThreadContext *) context;
  pthread_mutex_lock (threadContext->job->outputMutex);
  threadContext->job->outVec->emplace_back (key, value);
  pthread_mutex_unlock (threadContext->job->outputMutex);
}

JobHandle
startMapReduceJob (const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
{
  // initializing the JobContext:
  auto *job = new JobContext ();
  initializeJobContext (job, inputVec, multiThreadLevel, outputVec);
  initializeThreadsContexts (job, client, inputVec, multiThreadLevel);
  *job->stage = MAP_STAGE;
  initializeThreads (job);
  return job;
}

void waitForJob (JobHandle job)
{
  auto *jobContext = static_cast<JobContext *>(job);
  bool isJobWaiting;
  pthread_mutex_lock (jobContext->waitMutex);
  isJobWaiting = *jobContext->jobAlreadyWaiting;
  if (!isJobWaiting)
    {
      *jobContext->jobAlreadyWaiting = true;
    }
  pthread_mutex_unlock (jobContext->waitMutex);
  if (!isJobWaiting)
    {
      for (int i = 0; i < jobContext->threadsNum; ++i)
        {
          validate (pthread_join (jobContext->threads[i], nullptr), "pthread_join failed");
        }
    }
//  // Check if already called -> if so return immediately
//  if (*jobContext->jobAlreadyWaiting)
//    {
//      return;
//    }
//  *jobContext->jobAlreadyWaiting = true;
////  pthread_mutex_unlock (jobContext->jobWaitMutex);
//
//  for (int i = 0; i < jobContext->threadsNum; ++i)
//    {
//      validate (pthread_join (jobContext->threads[i], nullptr), "pthread_join failed");
//    }
}

void getJobState (JobHandle job, JobState *state)
{
  auto *jobCon = (JobContext *) (job);
  pthread_mutex_lock (jobCon->stateMutex);
  state->percentage = (
      ((float) (*jobCon->processed) / *jobCon->total) * 100);
  state->stage = *jobCon->stage;
  pthread_mutex_unlock (jobCon->stateMutex);
}

void closeJobHandle (JobHandle job)
{
  JobContext *jobPtr = (JobContext *) job;
  waitForJob (job);
  delete jobPtr;
}