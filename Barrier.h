#ifndef BARRIER_H
#define BARRIER_H
#include <pthread.h>
#include "MapReduceClient.h"
#include <atomic>
// a multiple use barrier
#define SUCCESS 0
#define ERROR 1
class Barrier {
public:
	Barrier(int numThreads);
	~Barrier();
	void barrier();

private:
	pthread_mutex_t mutex;
	pthread_cond_t cv;
	int count;
	int numThreads;
};

#endif //BARRIER_H
