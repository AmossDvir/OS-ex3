//
// Created by user on 5/11/2023.
//

#ifndef EX3_JOBCONTEXT_H
#define EX3_JOBCONTEXT_H

#include <pthread.h>
#include <algorithm>
//#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"
#include <atomic>
#include "MapReduceClient.h"


std::vector<std::vector<std::pair<K2 *, V2 *>>> sharedDB;
std::vector<std::vector<std::pair<K2 *, V2 *>>> sharedDBAfterShuffle;

typedef struct
{
    std::atomic<int> *atomic_counter;
    int threadId;
    Barrier *barrier;
    std::vector<std::pair<K2 *, V2 *>> *dbMap;
    const MapReduceClient *client;
    const InputVec *inputVec;
} ThreadContext;

class JobContext
{
private:
    pthread_t *threads;
//    JobState state;
    ThreadContext *contexts;
    int threadsNum;
    std::atomic<int> atomic_counter;
    std::vector<std::pair<K2*, V2*>> db;

    void initializeContexts(Barrier &barrier);

    void initializeThreads();

    const MapReduceClient *client;
    const InputVec *inputVec;

public:
    inline pthread_t getThread(int index)
    {
        return threads[index];
    }

    JobContext(int multiThreadLevel, Barrier &barrier, const MapReduceClient &client, const InputVec &inputVec);

    inline void *getThreadContext(int index)
    {
        return (void *) &contexts[index];
    }

    inline ThreadContext *getThreadsContexts()
    {
        return contexts;
    }
};


#endif //EX3_JOBCONTEXT_H
