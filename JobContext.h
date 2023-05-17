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
#include <semaphore.h>
#include "MapReduceClient.h"


static std::vector<std::vector<std::pair<K2 *, V2 *>>> sharedDB = {};
//static std::vector<std::vector<std::pair<K2 *, V2 *>>> sharedDBAfterShuffle = {};

typedef struct {
    int threadId;
    Barrier *barrier;
//    std::vector<std::pair<K2 *, V2 *>> *dbMap;
    std::vector<IntermediatePair*> dbMap;
    const MapReduceClient *client;
    const InputVec *inputVec;
    sem_t sharedDBSemaphore;
} ThreadContext;

class JobContext {
private:
    pthread_t *threads;
//    JobState state;

    int threadsNum;
    std::vector<ThreadContext> contexts={};
//    std::vector<std::pair<K2 *, V2 *>> db;
    sem_t sharedDBSemaphore;

    void initializeContexts(Barrier &barrier);

    void initializeThreads();

    void initializeDB() ;

    const MapReduceClient *client;
    const InputVec *inputVec;

public:
    inline pthread_t getThread(int index) {
        return threads[index];
    }

    JobContext(int multiThreadLevel, Barrier &barrier, const MapReduceClient &client, const InputVec &inputVec);

    inline void *getThreadContext(int index) {
        return (void *) &contexts[index];
    }

//    inline ThreadContext *getThreadsContexts() {
//        return contexts;
//    }
};


#endif //EX3_JOBCONTEXT_H
