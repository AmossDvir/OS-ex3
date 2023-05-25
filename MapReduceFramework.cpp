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

// Atomic variables:
/// todo: in reduce phase: don't ++, instead- add the vector size

typedef struct JobContext;

typedef struct ThreadContext {
    int threadId;

    IntermediateVec dbMap;
    JobContext *job;
    const MapReduceClient *client{};
    const InputVec *inputVec{};
    pthread_mutex_t *threadMutex;
} ThreadContext;

typedef struct JobContext {
    std::vector<ThreadContext> contexts;
    std::vector<IntermediateVec> *interVecSorted;
    std::vector<IntermediateVec> *interVecShuffled;
    OutputVec *outVec;
    pthread_t *threads{};
    int threadsNum{};
    pthread_mutex_t *mutex;
    pthread_mutex_t *outputMutex;
    std::atomic<stage_t> stage;
    std::atomic<int> processed;
    std::atomic<unsigned long> total;
    std::atomic<int> currentIndex;
    std::atomic<int> pairsCount;
} JobContext;


void handleSort(IntermediateVec &threadVec) {
    if (!threadVec.empty()) {
        std::sort(threadVec.begin(), threadVec.end(),
                  [](const IntermediatePair &pair1, const IntermediatePair &pair2) -> bool {
                      return *pair1.first < *pair2.first;
                  }
        );
    }
}

void *threadEntryPoint(void *arg) {
    auto *threadData = static_cast<std::pair<int, std::vector<ThreadContext>> *>(arg);
    int tid = threadData->first;
    IntermediateVec threadVec = IntermediateVec();
    std::vector<ThreadContext> contexts = threadData->second;
    ThreadContext threadContext = contexts[tid];
    auto *br = new Barrier(threadContext.job->threadsNum);

    const MapReduceClient *client = threadContext.client;
    unsigned long index = 0;
    while ((index = threadContext.job->currentIndex.fetch_add(1))
           < (threadContext.inputVec->size())) {
        // Process element at index
        std::pair<K1 *, V1 *> inputPair = (*threadContext.inputVec)[index];
        //map
        auto *emitData = new std::pair<IntermediateVec* , JobContext*>(&threadVec, threadContext.job);
        client->map(inputPair.first, inputPair.second, emitData);//?
        threadContext.job->processed++;
    }

    //sort
    handleSort(threadVec);

    if (!threadVec.empty()) {
        threadContext.job->interVecSorted->push_back(threadVec);
    }


    pthread_mutex_lock(threadContext.threadMutex);
    threadContext.job->stage = SHUFFLE_STAGE;
    threadContext.job->processed = 0.0;
    threadContext.job->total = threadContext.job->pairsCount;
    pthread_mutex_unlock(threadContext.threadMutex);

    br->barrier(*threadContext.job->interVecSorted, *threadContext.job->interVecShuffled, tid, threadContext.job->processed);

    pthread_mutex_lock(threadContext.threadMutex);
    threadContext.job->stage = REDUCE_STAGE;
    threadContext.job->processed = 0.0;
    threadContext.job->total = threadContext.job->pairsCount;
    pthread_mutex_unlock(threadContext.threadMutex);


    //
    //    reduce:
//    const IntermediateVec *pairs = &sharedDB.back();
//    sharedDB.pop_back();
//    client->reduce(pairs, arg);
    return nullptr;
}

void
initializeContexts(JobContext *job, const MapReduceClient &client, const InputVec &inputVec,
                   int threadsNum, pthread_mutex_t *mutex) {
    // Initializing contexts for threads:
    for (int i = 0; i < threadsNum; i++) {
        ThreadContext thrCon;
        thrCon.threadId = i;
        thrCon.dbMap = IntermediateVec();
        thrCon.client = &client;
        thrCon.inputVec = &inputVec;
        thrCon.threadMutex = mutex;
        thrCon.job = job;
        job->contexts[i] = thrCon;
    }
}

void
initializeThreads(pthread_t *threads, int threadsNum, std::vector<ThreadContext> &contexts) {
    // create "multiThreadLevel threads:
    int res = 0;

    for (int i = 0; i < threadsNum; i++) {
        auto *threadData = new std::pair<int, std::vector<ThreadContext> >(i, contexts);
        res = pthread_create(&threads[i], nullptr, threadEntryPoint, threadData);
        if (res < 0) {
            std::cerr << "system error: pthread_create returned <0 result"
                      << std::endl;
        }
    }
}

void emit2(K2 *key, V2 *value, void *context) {

    auto *threadVec = static_cast<std::pair<IntermediateVec*, JobContext*> *>(context)->first;
    auto *job = static_cast<std::pair<IntermediateVec*, JobContext*> *>(context)->second;
    job->pairsCount++;
    threadVec->emplace_back(key, value);
}

void emit3(K3 *key, V3 *value, void *context) {//todo: lock mutex
    auto *job = static_cast<JobContext *>(context);
    pthread_mutex_lock(job->outputMutex);
    job->outVec->emplace_back(key, value);
//    auto pair = new OutputPair(key, value);
    pthread_mutex_unlock(job->outputMutex);
//    sharedDBOutput.push_back(*pair);
}

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {
    // initializing the JobContext:
    JobState jobState = {UNDEFINED_STAGE, 0};

    auto *job = new JobContext;
    auto *mutex = new pthread_mutex_t();
    job->mutex = new pthread_mutex_t();
    job->outputMutex = new pthread_mutex_t();
    job->stage = UNDEFINED_STAGE;
    job->processed = 0;
    job->total = inputVec.size();
    job->currentIndex = 0;
    job->pairsCount = 0;
    job->interVecSorted = new std::vector<IntermediateVec>();
    job->interVecShuffled = new std::vector<IntermediateVec>();
    job->outVec = new OutputVec();
    pthread_mutex_init(job->mutex, nullptr);//todo check if okay
    pthread_mutex_init(job->outputMutex, nullptr);//todo check if okay

    job->contexts = std::vector<ThreadContext>(multiThreadLevel);
    job->threads = new pthread_t[multiThreadLevel];
    job->threadsNum = multiThreadLevel;
    initializeContexts(job, client, inputVec, multiThreadLevel, job->mutex);
    job->stage = MAP_STAGE;
    initializeThreads(job->threads, multiThreadLevel, job->contexts);

    // return jobHandle
    return (static_cast<JobHandle>(job));
}

void waitForJob(JobHandle job) {
    auto *jobContext = static_cast<JobContext *>(job);

    // todo: Check if already called -> if so return immediately
    for (int i = 0; i < jobContext->threadsNum; ++i) {
        int res = pthread_join(jobContext->threads[i], nullptr);
        if (res < 0) {
            std::cerr << "system error: join returned <0 result" << std::endl;
        }
    }
}

void getJobState(JobHandle job, JobState *state) {
    auto* jobCon = static_cast<JobContext*> (job);
    state->percentage = jobCon->processed / jobCon->total;
    state->stage = jobCon->stage;
}

void closeJobHandle(JobHandle job) {
//   todo: wait for all threads and delete all allocated elements
}