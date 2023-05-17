//
// Created by user on 5/11/2023.
//

#include <iostream>
#include "JobContext.h"

#define THREAD_ZERO 0

std::atomic<int> currentIndex(0);

JobContext::JobContext(int multiThreadLevel, Barrier &barrier, const MapReduceClient &client, const InputVec
&inputVec) :
threadsNum
(multiThreadLevel), client(&client), inputVec(&inputVec)
{
    //    Initialize semaphore:
    sem_init(&sharedDBSemaphore, 0, 1);
//    state = {UNDEFINED_STAGE, 0};
    threads = new pthread_t[multiThreadLevel];
    contexts = std::vector<ThreadContext>(multiThreadLevel);
    initializeDB();
    initializeContexts(barrier);
    initializeThreads();


}

void saveToSharedDB(){

}

void sort_by() {

}

void shuffle(std::vector<ThreadContext> &contexts)
{
    // Shuffling the Thread zero's vector (only after all threads are sorted):
    for (unsigned long i = 0; i < contexts[0].dbMap.size(); i++)
    {
        std::vector<IntermediatePair> tempVector;
        for (auto & context : contexts) {

            IntermediatePair *pair = context.dbMap.back();
            context.dbMap.pop_back();
            tempVector.push_back(*pair);
        }
        sharedDB.push_back(tempVector);
    }
}



void *barrierWrap(void *arg)
{
    ThreadContext *threadContext = {(ThreadContext *) arg};
    int tid = threadContext->threadId;
    const MapReduceClient *client = threadContext->client;
    unsigned long index=0;
    while ((index = currentIndex.fetch_add(1)) < (*threadContext->inputVec).size()) {
        // Process element at index
        auto inputPair = (*threadContext->inputVec)[index];
        //map
        client->map(inputPair.first, inputPair.second, arg);

        // ... perform required operations ...
    }
    //sort
    std::sort(threadContext->dbMap.begin(),threadContext->dbMap.end());//todo sort function

    //    reduce:
    const IntermediateVec* pairs = &sharedDB.back();
    sharedDB.pop_back();
    client->reduce(pairs, arg);
    //    threadContext->barrier->barrier();






    return 0;
}


void *barrierWrapForMainThread(void *arg)
{
    std::vector<ThreadContext> threadContexts = *static_cast<std::vector<ThreadContext>*>(arg);

    int tid = threadContexts[0].threadId;
    const MapReduceClient *client = threadContexts[tid].client;
    //map:
    unsigned long index=0;
    while ((index = currentIndex.fetch_add(1)) < (*threadContexts[tid].inputVec).size()) {
        // Process element at index
        auto inputPair = (*threadContexts[tid].inputVec)[index];
        //map
        client->map(inputPair.first, inputPair.second, static_cast<void*>(&threadContexts[tid]));

        // ... perform required operations ...
    }

    //sort
    std::sort(threadContexts[tid].dbMap.begin(),threadContexts[tid].dbMap.end());//todo sort function

    shuffle(threadContexts);
//        sem_post(&sem);

    //    reduce:
    const IntermediateVec* pairs = &sharedDB.back();
    sharedDB.pop_back();
    client->reduce(pairs, arg);
    //    threadContext->barrier->barrier();
    return 0;
}

void JobContext::initializeContexts(Barrier &barrier)
{
    // Initializing contexts for threads:
    for (int i = 0; i < threadsNum; i++)
    {
//        auto tempVec = std::vector<std::vector<std::pair<K2 *, V2 *>>>(threadsNum, std::vector<std::pair<K2 *, V2 *>>());
//        auto tempDB = new std::vector<IntermediatePair*>();
//        contexts[i] = {i, &barrier, std::vector<IntermediatePair*>(), client, inputVec, sharedDBSemaphore}; //
        contexts[i] = ThreadContext{i, &barrier, std::vector<IntermediatePair*>(), client, inputVec, sharedDBSemaphore};

// todo: verify
        // the atomic counter}
    }
}

void JobContext::initializeThreads()
{
    // create "multiThreadLevel threads:
    int res = 0;
    auto contextsPtr = new std::vector<ThreadContext>(threadsNum);
    contexts = *contextsPtr;
    res = pthread_create(&threads[0], nullptr, barrierWrapForMainThread, this);
    if (res < 0)
    {
        std::cerr << "system error: pthread_create returned <0 result" << std::endl;
    }
    for (int i = 1; i < threadsNum; i++)
    {
        res = pthread_create(&threads[i], nullptr, barrierWrap, &contexts[i]);
        if (res < 0)
        {
            std::cerr << "system error: pthread_create returned <0 result" << std::endl;
        }
    }
}

void JobContext::initializeDB() {
//    sharedDB = std::vector<std::vector<std::pair<K2 *, V2 *>>>(threadsNum);
    sharedDB = std::vector<std::vector<std::pair<K2 *, V2 *>>>(threadsNum, std::vector<std::pair<K2 *, V2 *>>());
}


