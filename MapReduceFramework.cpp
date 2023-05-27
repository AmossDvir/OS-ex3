//
// Created by amossdvir on 5/9/23.
//
#include "iostream"
#include <pthread.h>
#include <algorithm>
#include <atomic>
#include <unordered_map>
#include "MapReduceFramework.h"

typedef void *JobHandle;

// Atomic variables:
/// todo: in reduce phase: don't ++, instead- add the vector size


// The Barrier class, creates a barrier in code so all threads must arrive to this point and continue together
class Barrier
{
    pthread_mutex_t mutex;
    pthread_cond_t cv;
    int count;
    int numThreads;

public:
    Barrier(int numThreads) : mutex(PTHREAD_MUTEX_INITIALIZER), cv(PTHREAD_COND_INITIALIZER), count(0), numThreads(numThreads)
    {
    }

    ~Barrier()
    {
        if (pthread_mutex_destroy(&mutex) != 0)
        {
            fprintf(stderr, "[[Barrier]] error on pthread_mutex_destroy");
            exit(1);
        }
        if (pthread_cond_destroy(&cv) != 0)
        {
            fprintf(stderr, "[[Barrier]] error on pthread_cond_destroy");
            exit(1);
        }
    }

    void barrier()
    {
        if (pthread_mutex_lock(&mutex) != 0)
        {
            fprintf(stderr, "[[Barrier]] error on pthread_mutex_lock");
            exit(1);
        }
        if (++count < numThreads)
        {
            if (pthread_cond_wait(&cv, &mutex) != 0)
            {
                fprintf(stderr, "[[Barrier]] error on pthread_cond_wait");
                exit(1);
            }
        } else
        {
            count = 0;
            if (pthread_cond_broadcast(&cv) != 0)
            {
                fprintf(stderr, "[[Barrier]] error on pthread_cond_broadcast");
                exit(1);
            }
        }
        if (pthread_mutex_unlock(&mutex) != 0)
        {
            fprintf(stderr, "[[Barrier]] error on pthread_mutex_unlock");
            exit(1);
        }
    }
};


struct JobContext;

struct ThreadContext
{
    int threadId{};
    JobContext *job{};
    const MapReduceClient *client{};
    const InputVec *inputVec{};
    IntermediateVec threadVector;
};

struct JobContext
{
    ThreadContext *contexts;
    std::unordered_map<int, IntermediateVec> *interVecSorted;
    //    std::vector<IntermediateVec *> *interVecShuffled;
    std::vector<IntermediateVec> *interVecShuffled;
    OutputVec *outVec;
    pthread_t *threads;
    int threadsNum;
    pthread_mutex_t *stateMutex;
    pthread_mutex_t *reduceMutex;
    pthread_mutex_t *outputMutex;
    pthread_mutex_t *jobWaitMutex;
    std::atomic<stage_t> *stage;
    std::atomic<int> *processed;
    std::atomic<unsigned long> *total;
    std::atomic<int> *mapIndex;
    std::atomic<int> *reduceIndex;
    std::atomic<int> *pairsCount;
    std::atomic<bool> *jobAlreadyWaiting;
    Barrier *barrier;

    //  Destructor:
    ~JobContext()
    {
        delete interVecSorted;
        delete interVecShuffled;
        delete outVec;
        delete[] threads;
        delete stateMutex;
        delete reduceMutex;
        delete outputMutex;
        delete jobWaitMutex;
        delete stage;
        delete processed;
        delete total;
        delete mapIndex;
        delete reduceIndex;
        delete pairsCount;
        delete jobAlreadyWaiting;
        delete barrier;
        for (int i = 0; i < threadsNum; ++i)
        {
            contexts[i].job = nullptr;
        }
        delete[] contexts;

        contexts = nullptr;
        interVecSorted = nullptr;
        interVecShuffled = nullptr;
        outVec = nullptr;
        threads = nullptr;
        stateMutex = nullptr;
        reduceMutex = nullptr;
        outputMutex = nullptr;
        jobWaitMutex = nullptr;
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

K2 *findMax(const std::unordered_map<int, IntermediateVec> *intermediaryVectors)
{
    K2 *maxKey = nullptr;
    for (const auto &vector: *intermediaryVectors)
    {
        if (!vector.second.empty())
        {
            K2 *key = vector.second.back().first;
            if (maxKey == nullptr || *maxKey < *key)
            {
                maxKey = key;
            }
        }
    }
    return maxKey;
}

void validateThreadCreation(int res)
{
    if (res < 0)
    {
        std::cerr << "system error: pthread_create returned <0 result" << std::endl;
    }
}


void resetState(JobContext &job, const stage_t &stage, unsigned long total)
{
    pthread_mutex_lock(job.stateMutex);
    *job.stage = stage;
    *job.processed = 0;
    *job.total = total;
    pthread_mutex_unlock(job.stateMutex);
}

// Handler functions:

void handleMap(ThreadContext *threadContext)
{
    unsigned long index = 0;
    while ((index = threadContext->job->mapIndex->fetch_add(1)) < threadContext->inputVec->size())
    {
        InputPair inputPair = (*threadContext->inputVec)[index];
        threadContext->job->processed->fetch_add(1);
        threadContext->client->map(inputPair.first, inputPair.second, threadContext);//?
    }
}

void freeMemory()
{

}

void handleSort(ThreadContext *threadContext)
{
    std::sort(threadContext->threadVector.begin(), threadContext->threadVector.end(),
              [](const IntermediatePair &pair1, const IntermediatePair &pair2) -> bool
              {
                  return *pair1.first < *pair2.first;
              });
}

void handleShuffle(JobContext &job)
{
    // Shuffling the Thread zero's vector (only after all threads are sorted):
    while (!job.interVecSorted->empty())
    {
        auto tempVec = IntermediateVec();
        K2 *maxKey = findMax(job.interVecSorted);


        for (auto &pair: *job.interVecSorted)
        {
            IntermediateVec &vec = pair.second;
            while (!vec.empty() && (!(*(vec.back().first) < *maxKey) && !(*maxKey < *(vec.back().first))))
            {
                tempVec.push_back(vec.back());
                vec.pop_back();
                job.processed->fetch_add(1);
            }
            if (vec.empty())
            {
                job.interVecSorted->erase(pair.first);
                break;
            }
        }
        job.interVecShuffled->push_back(tempVec);
    }
}


void handleReduce(ThreadContext *threadContext)
{

    unsigned long index = 0;

    while ((index = threadContext->job->reduceIndex->fetch_add(1)) < *threadContext->job->total)
    {
        pthread_mutex_lock(threadContext->job->reduceMutex);
        const IntermediateVec *vecToReduce = &(*threadContext->job->interVecShuffled)[index];
        threadContext->client->reduce(vecToReduce, threadContext);
        threadContext->job->processed->fetch_add(1);
//        threadContext->job->interVecShuffled->erase(threadContext->job->interVecShuffled->begin() + int(index));
        pthread_mutex_unlock(threadContext->job->reduceMutex);
    }
}

void *threadMainFlow(void *arg)
{
    auto *threadContext = (ThreadContext *) arg;

    //  map:
    handleMap(threadContext);

    // sort:
    handleSort(threadContext);

    // ADD MUTEX

    // Add the local thread vector to the main vector of the program:
    threadContext->job->interVecSorted->insert({threadContext->threadId, threadContext->threadVector});

    // shuffle:
    threadContext->job->barrier->barrier();
    if (threadContext->threadId == 0)
    {
        resetState(*(threadContext->job), SHUFFLE_STAGE, (unsigned long) *threadContext->job->pairsCount);
        handleShuffle(*threadContext->job);
        resetState(*(threadContext->job), REDUCE_STAGE, (unsigned long) *threadContext->job->pairsCount);
    }
    threadContext->job->barrier->barrier();

    //    reduce:
    handleReduce(threadContext);
    return nullptr;
}

void
initializeThreadsContexts(JobContext *job, const MapReduceClient &client, const InputVec &inputVec,
                          int threadsNum)
{
    for (int i = 0; i < threadsNum; i++)
    {
        ThreadContext thrCon;
        thrCon.threadId = i;
        thrCon.client = &client;
        thrCon.inputVec = &inputVec;
        thrCon.job = job;
        thrCon.threadVector = IntermediateVec();
        job->contexts[i] = thrCon;
    }
}

void
initializeThreads(JobContext *job)
{
    // create 'multiThreadLevel' threads:
    for (int i = 0; i < job->threadsNum; i++)
    {
        job->contexts[i].threadId = i;
        validateThreadCreation(pthread_create(&job->threads[i], nullptr, threadMainFlow, &job->contexts[i]));
    }
}

void initializeJobContext(JobContext *job, const InputVec &inputVec, int multiThreadLevel)
{
    job->stateMutex = new pthread_mutex_t();
    job->reduceMutex = new pthread_mutex_t();
    job->outputMutex = new pthread_mutex_t();
    job->jobWaitMutex = new pthread_mutex_t();
    job->stage = new std::atomic<stage_t>(UNDEFINED_STAGE);
    job->processed = new std::atomic<int>(0);
    job->total = new std::atomic<unsigned long>(inputVec.size());
    job->mapIndex = new std::atomic<int>(0);
    job->reduceIndex = new std::atomic<int>(0);
    job->pairsCount = new std::atomic<int>(0);
    job->interVecSorted = new std::unordered_map<int, IntermediateVec>();
    job->interVecShuffled = new std::vector<IntermediateVec>();
    job->jobAlreadyWaiting = new std::atomic<bool>(false);
    job->outVec = new OutputVec();
    pthread_mutex_init(job->stateMutex, nullptr);
    pthread_mutex_init(job->reduceMutex, nullptr);
    pthread_mutex_init(job->outputMutex, nullptr);
    pthread_mutex_init(job->jobWaitMutex, nullptr);
    job->barrier = new Barrier(multiThreadLevel);
    job->contexts = new ThreadContext[multiThreadLevel];
    job->threads = new pthread_t[multiThreadLevel];
    job->threadsNum = multiThreadLevel;
}

void emit2(K2 *key, V2 *value, void *context)
{
    auto *threadContext = (ThreadContext *) context;
    auto &threadVec = threadContext->threadVector;
    threadContext->job->pairsCount->fetch_add(1);
    threadVec.emplace_back(key, value);
}

void emit3(K3 *key, V3 *value, void *context)
{
    auto *threadContext = (ThreadContext *) context;
    pthread_mutex_lock(threadContext->job->outputMutex);
    threadContext->job->outVec->emplace_back(key, value);
    pthread_mutex_unlock(threadContext->job->outputMutex);
}

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
{
    // initializing the JobContext:
    auto *job = new JobContext();
    initializeJobContext(job, inputVec, multiThreadLevel);
    initializeThreadsContexts(job, client, inputVec, multiThreadLevel);
    *job->stage = MAP_STAGE;
    initializeThreads(job);
    return job;
}

void waitForJob(JobHandle job)
{
    auto *jobContext = static_cast<JobContext *>(job);
    pthread_mutex_lock(jobContext->jobWaitMutex);
    if (!jobContext->jobAlreadyWaiting)
    {
        *jobContext->jobAlreadyWaiting = true;
    }
    pthread_mutex_unlock(jobContext->jobWaitMutex);

    // Check if already called -> if so return immediately
    if (!jobContext->jobAlreadyWaiting)
    {
        return;
    }

    for (int i = 0; i < jobContext->threadsNum; ++i)
    {
        int res = pthread_join(jobContext->threads[i], nullptr);
        if (res < 0)
        {
            std::cerr << "system error: join returned <0 result" << std::endl;
        }
    }


}

void getJobState(JobHandle job, JobState *state)
{
    auto *jobCon = static_cast<JobContext *> (job);

    state->percentage = ((static_cast<double>(*jobCon->processed) / *jobCon->total) * 100);
    state->stage = *jobCon->stage;
}

void closeJobHandle(JobHandle job)
{
    JobContext *jobPtr = (JobContext *) job;
    waitForJob(job);
    delete jobPtr;
}