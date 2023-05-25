#include "Barrier.h"
#include <cstdlib>
#include <cstdio>

enum stage_t {UNDEFINED_STAGE=0, MAP_STAGE=1, SHUFFLE_STAGE=2, REDUCE_STAGE=3};


Barrier::Barrier(int numThreads)
		: mutex(PTHREAD_MUTEX_INITIALIZER)
		, cv(PTHREAD_COND_INITIALIZER)
		, count(0)
		, numThreads(numThreads)
{ }


Barrier::~Barrier()
{
	if (pthread_mutex_destroy(&mutex) != 0) {
		fprintf(stderr, "[[Barrier]] error on pthread_mutex_destroy");
		exit(1);
	}
	if (pthread_cond_destroy(&cv) != 0){
		fprintf(stderr, "[[Barrier]] error on pthread_cond_destroy");
		exit(1);
	}
}


K2* findMax(const std::vector<std::vector<std::pair<K2*, V2*>>>& intermediaryVectors) {
    K2* maxKey = nullptr;
    for (const auto& vector : intermediaryVectors) {
        if (!vector.empty()) {
            K2* key = vector.back().first;
            if (maxKey == nullptr || *maxKey< *key) {
                maxKey = key;
            }
        }
    }
    return maxKey;
}


void handleShuffle(std::vector<IntermediateVec> &originVec , std::vector<IntermediateVec> &returnVec,  std::atomic<int> &processed) {
    // Shuffling the Thread zero's vector (only after all threads are sorted):
    while(!originVec.empty()){
        IntermediateVec tempVec;
        K2* maxKey = findMax(originVec);
        for(auto& vec:originVec){
            while(!vec.empty()&&!(*(vec.back().first)<*maxKey)&&(*maxKey<*(vec.back().first))){
                tempVec.push_back(vec.back());
                vec.pop_back();
                processed++;
            }
        }
        returnVec.push_back(tempVec);
    }
}


void Barrier::barrier(std::vector<IntermediateVec> &originVec , std::vector<IntermediateVec> &returnVec, int tid, std::atomic<int> &processed)
{
	if (pthread_mutex_lock(&mutex) != 0){
		fprintf(stderr, "[[Barrier]] error on pthread_mutex_lock");
		exit(1);
	}
	if (++count < numThreads) {
		if (pthread_cond_wait(&cv, &mutex) != 0){
			fprintf(stderr, "[[Barrier]] error on pthread_cond_wait");
			exit(1);
		}
	} else {
		count = 0;
        //Here is the part that the thread 0 will execute:
        // only 1 thread will shuffle:
        if (tid == 0){
            handleShuffle(originVec,returnVec, processed);
        }

        // End of the part
		if (pthread_cond_broadcast(&cv) != 0) {
			fprintf(stderr, "[[Barrier]] error on pthread_cond_broadcast");
			exit(1);
		}
	}
	if (pthread_mutex_unlock(&mutex) != 0) {
		fprintf(stderr, "[[Barrier]] error on pthread_mutex_unlock");
		exit(1);
	}
}
