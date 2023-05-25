//
// Created by amossdvir on 5/9/23.
//
#include "iostream"
#include <pthread.h>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include "MapReduceFramework.h"

typedef void *JobHandle;

// Atomic variables:
/// todo: in reduce phase: don't ++, instead- add the vector size

typedef struct JobContext;

typedef struct ThreadContext {
    int threadId;

//    IntermediateVec dbMap;
    JobContext *job;
    const MapReduceClient *client{};
    const InputVec *inputVec{};
    IntermediateVec threadVector;
    pthread_mutex_t *threadMutex;
} ThreadContext;

typedef struct JobContext {
    ThreadContext *contexts;
    std::unordered_map<int, IntermediateVec> *interVecSorted;
    std::vector<IntermediateVec*> *interVecShuffled;
    OutputVec *outVec;
    pthread_t *threads{};
    int threadsNum{};
    pthread_mutex_t *mutex;
    pthread_mutex_t *outputMutex;
    std::atomic<stage_t> stage;
    std::atomic<int> processed;
    std::atomic<unsigned long> total;
    std::atomic<int>* currentIndex;
    std::atomic<int>* pairsCount;
    Barrier *barrier;
} JobContext;


K2* findMax(const std::unordered_map<int, IntermediateVec>* intermediaryVectors) {
    K2* maxKey = nullptr;
    for (const auto& vector : *intermediaryVectors) {
        if (!vector.second.empty()) {
            K2* key = vector.second.back().first;
            if (maxKey == nullptr || *maxKey < *key) {
                maxKey = key;
            }
        }
    }
    return maxKey;
}

void handleShuffle(ThreadContext* threadContext) {
    // Shuffling the Thread zero's vector (only after all threads are sorted):
    while(!threadContext->job->interVecSorted->empty()){
        IntermediateVec* tempVec = new IntermediateVec;
        K2* maxKey = findMax(threadContext->job->interVecSorted);
        // for i in range todo

        for(auto& pair: *threadContext->job->interVecSorted) {
            IntermediateVec &vec = pair.second;
            while (!vec.empty() && (!(*(vec.back().first) < *maxKey) && !(*maxKey < *(vec.back().first)))) {
                tempVec->push_back(vec.back());
                vec.pop_back();
                // TODO: update 64 bit atomic variable - Mutex isn't needed
            }
            if (vec.empty()) {
                threadContext->job->interVecSorted->erase(pair.first);
                break;
            }
        }
         threadContext->job->interVecShuffled->push_back(tempVec);
         tempVec = nullptr;
    }
}

void *threadEntryPoint(void *arg) {

    ThreadContext *threadContext = (ThreadContext*) arg;

    while (true) // TODO: Change the true value
    {
        int index = (*(threadContext->job->currentIndex))++;
        if (index >= threadContext->inputVec->size())
        {
            break;
        }
        InputPair inputPair = (*threadContext->inputVec)[index];
        threadContext->client->map(inputPair.first, inputPair.second, arg);//?
        // TODO: update atomic variable in charge of state and counter of pairs processed
        //total - input vec size , does not change in map
        //atomic counter 64 bit, 2 - state 31 - total - 31 counter
    }

    // sort
    std::sort(threadContext->threadVector.begin(), threadContext->threadVector.end(),
              [](const IntermediatePair &pair1, const IntermediatePair &pair2) -> bool {
                  return *pair1.first < *pair2.first;});
    // ADD MUTEX
    threadContext->job->interVecSorted->insert({threadContext->threadId, threadContext->threadVector});
    //
    threadContext->job->barrier->barrier();

    if(threadContext->threadId == 0) {
        // TODO: update atomic variable 64 bit - todo update total and stage
        *threadContext->job->currentIndex = 0;
        handleShuffle(threadContext);
    }
    threadContext->job->barrier->barrier();
    //todo update total and stage

//    pthread_mutex_lock(threadContext.threadMutex);
//    threadContext.job->stage = REDUCE_STAGE;
//    threadContext.job->processed = 0.0;
//    threadContext.job->total = threadContext.job->pairsCount;
//    pthread_mutex_unlock(threadContext.threadMutex);

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
//        thrCon.dbMap = IntermediateVec();
        thrCon.client = &client;
        thrCon.inputVec = &inputVec;
        thrCon.threadMutex = mutex;
        thrCon.job = job;
        thrCon.threadVector=IntermediateVec ();
        job->contexts[i] = thrCon;
    }
}

void
initializeThreads(JobContext* job) {
    // create "multiThreadLevel threads:
    int res = 0;

    for (int i = 0; i < job->threadsNum; i++) {
        job->contexts[i].threadId = i;
        res = pthread_create(&job->threads[i], nullptr, threadEntryPoint, &job->contexts[i]);
        if (res < 0) {
            std::cerr << "system error: pthread_create returned <0 result"
                      << std::endl;
        }
    }
}

void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext *threadContext = (ThreadContext*) context;
    auto &threadVec = threadContext->threadVector;
    threadContext->job->pairsCount++;
    threadVec.emplace_back(key, value);
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
    job->currentIndex = new std::atomic<int>(0);
    job->pairsCount = new std::atomic<int>(0);
    job->interVecSorted = new std::unordered_map<int,IntermediateVec>();
    job->interVecShuffled = new std::vector<IntermediateVec*>();
    job->outVec = new OutputVec();
    pthread_mutex_init(job->mutex, nullptr);//todo check if okay
    pthread_mutex_init(job->outputMutex, nullptr);//todo check if okay
    job->barrier = new Barrier(multiThreadLevel);


    job->contexts = new ThreadContext[multiThreadLevel];
    job->threads = new pthread_t[multiThreadLevel];
    job->threadsNum = multiThreadLevel;
    initializeContexts(job, client, inputVec, multiThreadLevel, job->mutex);
    job->stage = MAP_STAGE;
    initializeThreads(job);

    // return jobHandle
    return job;
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
    waitForJob(job);
}