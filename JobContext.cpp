//
// Created by user on 5/11/2023.
//

#include "iostream"
#include "JobContext.h"
#include <semaphore.h>

#define THREAD_ZERO 0


sem_t sem;

JobContext::JobContext(int multiThreadLevel, Barrier &barrier, const MapReduceClient &client, const InputVec
&inputVec) :
threadsNum
(multiThreadLevel), client(&client), inputVec(&inputVec)
{
    //    Initialize semaphore:
    sem_init(&sem, 0, 1);

    atomic_counter = 0;
//    state = {UNDEFINED_STAGE, 0};
    threads = new pthread_t[multiThreadLevel];
    contexts = new ThreadContext[multiThreadLevel];
    initializeContexts(barrier);
    initializeThreads();
}

void saveToSharedDB(){

}


void sort(int threadId)
{
    // Sorting each vector of each thread and waiting for all of them to finish:
    std::sort(sharedDB[threadId].begin(), sharedDB[threadId].end()); // todo: sort by key?
}

void shuffle()
{
    // Shuffling the Thread zero's vector (only after all threads are sorted):
    for (int i = 0; i < sharedDB[0].size(); i++)
    {
        std::vector<std::pair<K2 *, V2 *>> tempVector;
        for (auto &vector: sharedDB)
        {
            std::pair<K2 *, V2 *> pair = vector.back();
            vector.pop_back();
            tempVector.push_back(pair);
        }
        sharedDBAfterShuffle.push_back(tempVector);
    }
}



void *barrierWrap(void *arg)
{

    ThreadContext *threadContext = {(ThreadContext *) arg};
    int tid = threadContext->threadId;
    const MapReduceClient *client = threadContext->client;
    auto inputPair = (*threadContext->inputVec)[tid];
    client->map(inputPair.first, inputPair.second, arg);
    sort(threadContext->threadId);
    sem_wait(&sem);
    if (threadContext->threadId == THREAD_ZERO)
    {
        shuffle();
        sem_post(&sem);
    }

    //    reduce:
    const IntermediateVec* pairs = &sharedDBAfterShuffle.back();
    sharedDBAfterShuffle.pop_back();
    client->reduce(pairs, arg);
    //    threadContext->barrier->barrier();
    return 0;
}


void JobContext::initializeContexts(Barrier &barrier)
{
    // Initializing contexts for threads:
    for (int i = 0; i < threadsNum; i++)
    {
        contexts[i] = {&atomic_counter, i, &barrier, new std::vector<std::pair<K2 *, V2 *>>(), client, inputVec}; //
        // todo: verify
        // the atomic counter}
    }
}

void JobContext::initializeThreads()
{
    // create "multiThreadLevel threads:
    int res = 0;
    for (int i = 0; i < threadsNum; i++)
    {
        res = pthread_create(&threads[i], nullptr, barrierWrap, contexts + i);
        if (res < 0)
        {
            std::cerr << "system error: pthread_create returned <0 result" << std::endl;
        }
    }
}


