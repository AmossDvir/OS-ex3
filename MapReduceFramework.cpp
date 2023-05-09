//
// Created by amossdvir on 5/9/23.
//

#include <pthread.h>
#include <algorithm>
#include "MapReduceClient.h"
#include "Barrier/Barrier.h"
#define NULL nullptr

typedef void* JobHandle;

enum stage_t {UNDEFINED_STAGE=0, MAP_STAGE=1, SHUFFLE_STAGE=2, REDUCE_STAGE=3};

typedef struct {
    stage_t stage;
    float percentage;
} JobState;

typedef struct {
pthread_t* thread;
JobState state;
    pthread_mutex_t mutex;
} JobContext;


void emit2 (K2* key, V2* value, void* context){}
void emit3 (K3* key, V3* value, void* context){}

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel){
    JobContext context;
    context.state = {UNDEFINED_STAGE, 0};

    context.thread=new pthread_t[multiThreadLevel];
    context.mutex = ;//todo: assign mutex


    // create "multiThreadLevel threads:
    for (int i = 0; i < multiThreadLevel; i++){

        // todo: create thread (pthread_create)
        pthread_create(context.thread[i], NULL, Barrier::barrier);
    }


    for(auto& inputPair : inputVec){
        client.map(inputPair.first, inputPair.second, context);
    }
     std::sort();
    // barrier();
    // std::shuffle();
    // client.reduce();

    // return jobHandle

}

void waitForJob(JobHandle job){
    // todo: Check if already called -> if so return immediately
    pthread_join()
}
void getJobState(JobHandle job, JobState* state){}
void closeJobHandle(JobHandle job){}
