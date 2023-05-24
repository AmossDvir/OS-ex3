//
// Created by amossdvir on 5/9/23.
//
#include "iostream"
#include <pthread.h>
#include <algorithm>
#include <atomic>
#include <mutex>
#include "MapReduceFramework.h"

typedef void *JobHandle;
static std::vector<std::vector<IntermediatePair>> sharedDB = {};

static std::vector<OutputPair> sharedDBOutput = {};

std::mutex mutex;

// Atomic variables:
/// division: 2bits for stage, 31 bits for total size, 31 bits for already processed
/// todo: in reduce phase: don't ++, instead- add the vector size
//std::atomic<uint64_t>* atomic_stage;
std::atomic<stage_t> stage;
std::atomic<int> completed;
std::atomic<unsigned long> total;

std::atomic<int> currentIndex (0);

typedef struct ThreadContext {
    int threadId{};
//    std::vector<IntermediatePair *> dbMap;
    std::vector<IntermediatePair> dbMap;

    const MapReduceClient *client{};
    const InputVec *inputVec{};
} ThreadContext;

typedef struct JobContext {
    std::vector<ThreadContext> contexts;
    pthread_t *threads{};
    int threadsNum{};
} JobContext;

void shuffle (std::vector<ThreadContext> &contexts)
{
  // Shuffling the Thread zero's vector (only after all threads are sorted):
  for (unsigned long i = 0; i < contexts[0].dbMap.size (); i++)
    {
      std::vector<IntermediatePair> tempVector;
      for (auto &context: contexts)
        {

          IntermediatePair pair = context.dbMap.back ();
          context.dbMap.pop_back ();
          tempVector.push_back (pair);
        }
      sharedDB.push_back (tempVector);
    }
}

bool sort_pairs (const IntermediatePair &pair1, const IntermediatePair &pair2)
{
  return ((*pair1.first) < (*pair2.first));
}

void *threadEntryPoint (void *arg)
{
  auto *threadData = static_cast<std::pair<int, std::vector<ThreadContext>> *>(arg);
  int tid = threadData->first;
  std::vector<ThreadContext> contexts = threadData->second;
  Barrier *br = static_cast<Barrier *>(arg) + 2;
  ThreadContext threadContext = contexts[tid];
  const MapReduceClient *client = threadContext.client;
  unsigned long index = 0;
  while ((index = currentIndex.fetch_add (1))
         < (*threadContext.inputVec).size ())
    {
      // Process element at index
      std::pair<K1*,V1*> inputPair = (*threadContext.inputVec)[index];
      //map
      mutex.lock ();
      client->map (inputPair.first, inputPair.second, arg);//?
      completed++;
      mutex.unlock ();
    }
  //sort
  std::sort (threadContext.dbMap.begin (), threadContext.dbMap.end (),
             sort_pairs);//todo sort function
  br->barrier ();

  // only 1 thread will shuffle:
  if (tid == 0)
    {

      mutex.lock ();
      stage = SHUFFLE_STAGE;
      completed = 0.0;
      total = sharedDB.size ();
      mutex.unlock ();

      shuffle (contexts);

      mutex.lock ();
      stage = REDUCE_STAGE;
      completed = 0.0;
      total = sharedDB.size ();
      mutex.unlock ();
    }
  br->barrier ();
  ///

  //    reduce:
  const IntermediateVec *pairs = &sharedDB.back ();
  sharedDB.pop_back ();
  client->reduce (pairs, arg);
  br->barrier ();
  return nullptr;
}

void
initializeContexts (std::vector<ThreadContext> &contexts, const MapReduceClient &client, const InputVec &inputVec,
                    int threadsNum)
{
  // Initializing contexts for threads:
  for (int i = 0; i < threadsNum; i++)
    {
      auto *thrCon = new ThreadContext;
      thrCon->threadId = i;
      thrCon->dbMap = std::vector<IntermediatePair> ();
      thrCon->client = &client;
      thrCon->inputVec = &inputVec;
      contexts[i] = *thrCon;
    }
}

void
initializeThreads (pthread_t *threads, int threadsNum, std::vector<ThreadContext> &contexts)
{
  // create "multiThreadLevel threads:
  int res = 0;
  stage = MAP_STAGE;
  for (int i = 1; i < threadsNum; i++)
    {
      auto *threadData = new std::pair<int, std::vector<ThreadContext> > (i, contexts);
      res = pthread_create (&threads[i], nullptr, threadEntryPoint, threadData);
      if (res < 0)
        {
          std::cerr << "system error: pthread_create returned <0 result"
                    << std::endl;
        }
    }
}

void emit2 (K2 *key, V2 *value, void *context)
{
  auto *con = static_cast<ThreadContext *>(context);
  auto pair = new IntermediatePair (key, value);
  con->dbMap.push_back (*pair);
  //vector initialize?
}

void emit3 (K3 *key, V3 *value, void *context)
{//todo
  auto *con = static_cast<ThreadContext *>(context);
  auto pair = new OutputPair (key, value);
  sharedDBOutput.push_back (*pair);
}

JobHandle
startMapReduceJob (const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
{
  // initializing the JobContext:
  JobState jobState = {UNDEFINED_STAGE, 0};
  stage = UNDEFINED_STAGE;
  completed = 0;
  total = inputVec.size ();
  auto *job = new JobContext;
  job->contexts = std::vector<ThreadContext> (multiThreadLevel);
  job->threads = new pthread_t[multiThreadLevel];
  job->threadsNum = multiThreadLevel;
  initializeContexts (job->contexts, client, inputVec, multiThreadLevel);
  initializeThreads (job->threads, multiThreadLevel, job->contexts);


  // return jobHandle
  return (static_cast<JobHandle>(job));
}

void waitForJob (JobHandle job)
{
  JobContext jobContext = *static_cast<JobContext *>(job);

  // todo: Check if already called -> if so return immediately
  for (int i = 0; i < jobContext.threadsNum; ++i)
    {
      int res = pthread_join (jobContext.threads[i], nullptr);
      if (res < 0)
        {
          std::cerr << "system error: join returned <0 result" << std::endl;
        }
    }
}

void getJobState (JobHandle job, JobState *state)
{
  state->percentage = completed / total;
  state->stage = stage;
  JobContext jobContext = *static_cast<JobContext *>(job);

}

void closeJobHandle (JobHandle job)
{
//   todo: wait for all threads and delete all allocated elements
}
