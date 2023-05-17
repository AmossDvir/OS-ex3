//
// Created by amossdvir on 5/9/23.
//
#include "iostream"
#include <pthread.h>
#include <algorithm>
#include "MapReduceClient.h"
#include "Barrier/Barrier.h"
#include "JobContext.h"
#include "MapReduceFramework.h"

typedef void *JobHandle;


void emit2(K2 *key, V2 *value, void *context)
{
    auto *con = static_cast<ThreadContext *>(context);
    auto pair = new IntermediatePair(key,value);
    con->dbMap.push_back(pair);
}

void emit3(K3 *key, V3 *value, void *context)
{

}

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
{
    // initializing the JobContext:
    auto *jobContext = new JobContext(multiThreadLevel, *new Barrier(multiThreadLevel), client, inputVec);

    int res = 0;
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        res = pthread_join(jobContext->getThread(i), nullptr);
        if (res < 0){
            std::cerr << "system error: join returned <0 result" << std::endl;
        }
    }

    // return jobHandle
    return nullptr;
}

void waitForJob(JobHandle job)
{
    // todo: Check if already called -> if so return immediately
//    pthread_join();
}

void getJobState(JobHandle job, JobState *state)
{
}

void closeJobHandle(JobHandle job)
{
}
