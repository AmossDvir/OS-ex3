#ifndef BARRIER_H
#define BARRIER_H
#include <pthread.h>
#include "MapReduceClient.h"
#include <atomic>
// a multiple use barrier

class Barrier {
public:
	Barrier(int numThreads);
	~Barrier();
	void barrier(std::vector<IntermediateVec> &originVec , std::vector<IntermediateVec> &returnVec, int tid, std::atomic<int> &processed);

private:
	pthread_mutex_t mutex;
	pthread_cond_t cv;
	int count;
	int numThreads;
};

#endif //BARRIER_H
