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

//std::mutex mutex;

// Atomic variables:
/// todo: in reduce phase: don't ++, instead- add the vector size
std::atomic<stage_t> stage;
std::atomic<int> proccessed;
std::atomic<unsigned long> total;

std::atomic<int> currentIndex(0);

typedef struct JobContext;

typedef struct ThreadContext {
    int threadId{};
//    std::vector<IntermediatePair *> dbMap;
    IntermediateVec dbMap;
    JobContext *job;
    const MapReduceClient *client{};
    const InputVec *inputVec{};
    pthread_mutex_t *threadMutex;
} ThreadContext;

typedef struct JobContext {
    std::vector<ThreadContext> contexts;
    std::vector<IntermediateVec> *interVecSorted;
    pthread_t *threads{};
    int threadsNum{};
    pthread_mutex_t *mutex;
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

void handleShuffle(std::vector<ThreadContext> &contexts) {
    // Shuffling the Thread zero's vector (only after all threads are sorted):
    for (unsigned long i = 0; i < contexts[0].dbMap.size(); i++) {
        std::vector<IntermediatePair> tempVector;
        for (auto &context: contexts) {
            IntermediatePair pair = context.dbMap.back();
            context.dbMap.pop_back();
            tempVector.push_back(pair);
        }
        sharedDB.push_back(tempVector);
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
    while ((index = currentIndex.fetch_add(1))
           < (threadContext.inputVec->size())) {
        // Process element at index
        std::pair<K1 *, V1 *> inputPair = (*threadContext.inputVec)[index];
        //map
//      pthread_mutex_lock (threadContext.threadMutex);
        client->map(inputPair.first, inputPair.second, &threadVec);//?
        proccessed++;
//      pthread_mutex_unlock (threadContext.threadMutex);
    }
//    br->barrier ();
    //sort

    handleSort(threadVec);
    if (!threadVec.empty()) {
        threadContext.job->interVecSorted->push_back(threadVec);
    }

    br->barrier();

    // only 1 thread will shuffle:
    if (tid == 0) {

        pthread_mutex_lock(threadContext.threadMutex);
        stage = SHUFFLE_STAGE;
        proccessed = 0.0;
        total = sharedDB.size();
        pthread_mutex_unlock(threadContext.threadMutex);
        //todo check if needed here lock
        handleShuffle(contexts);

        pthread_mutex_lock(threadContext.threadMutex);
        stage = REDUCE_STAGE;
        proccessed = 0.0;
        total = sharedDB.size();
        pthread_mutex_unlock(threadContext.threadMutex);
    }
    br->barrier();
    //
    //    reduce:
    const IntermediateVec *pairs = &sharedDB.back();
    sharedDB.pop_back();
    client->reduce(pairs, arg);
    br->barrier();
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
//      contexts[i]=thrCon;
    }
}

void
initializeThreads(pthread_t *threads, int threadsNum, std::vector<ThreadContext> &contexts) {
    // create "multiThreadLevel threads:
    int res = 0;
    stage = MAP_STAGE;
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
//  auto *con = static_cast<ThreadContext *>(context);
//  auto pair = new IntermediatePair (key, value);
//  con->dbMap.push_back (IntermediatePair (key,value));
//  con->job->interVec[con->threadId].push_back(*pair);
    auto *threadVec = static_cast<IntermediateVec *>(context);
    threadVec->emplace_back(key, value);

}

void emit3(K3 *key, V3 *value, void *context) {//todo
    auto *con = static_cast<ThreadContext *>(context);
    auto pair = new OutputPair(key, value);
    sharedDBOutput.push_back(*pair);
}

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {
    // initializing the JobContext:
    JobState jobState = {UNDEFINED_STAGE, 0};
    stage = UNDEFINED_STAGE;
    proccessed = 0;
    total = inputVec.size();
    auto *job = new JobContext;
    auto *mutex = new pthread_mutex_t();
    job->mutex = mutex;
    job->interVecSorted = new std::vector<IntermediateVec>();
    pthread_mutex_init(job->mutex, nullptr);//todo check if okay
    job->contexts = std::vector<ThreadContext>(multiThreadLevel);
    job->threads = new pthread_t[multiThreadLevel];
    job->threadsNum = multiThreadLevel;
//  initializeContexts (job->contexts, client, inputVec, multiThreadLevel,job->mutex);
    initializeContexts(job, client, inputVec, multiThreadLevel, job->mutex);
    initializeThreads(job->threads, multiThreadLevel, job->contexts);


    // return jobHandle
    return (static_cast<JobHandle>(job));
}

void waitForJob(JobHandle job) {
    JobContext jobContext = *static_cast<JobContext *>(job);

    // todo: Check if already called -> if so return immediately
    for (int i = 0; i < jobContext.threadsNum; ++i) {
        int res = pthread_join(jobContext.threads[i], nullptr);
        if (res < 0) {
            std::cerr << "system error: join returned <0 result" << std::endl;
        }
    }
}

void getJobState(JobHandle job, JobState *state) {
    state->percentage = proccessed / total;
    state->stage = stage;
    JobContext jobContext = *static_cast<JobContext *>(job);
}

void closeJobHandle(JobHandle job) {
//   todo: wait for all threads and delete all allocated elements
}