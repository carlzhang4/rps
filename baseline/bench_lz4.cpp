#include <fstream>
#include <iostream>
#include <assert.h>
#include <algorithm>
#include <gflags/gflags.h>
#include <libr.hpp>
#include "src/compression.hpp"
using namespace std;
std::mutex IO_LOCK;

void sub_task(int thread_index, void* buf, size_t buf_size, int num_chunck, int chunck_size, size_t ops){
	struct timespec start_timer,end_timer;
	double duration = 0;
	for(int i=0; i<ops ;i++){
		size_t offset = (ops%num_chunck)*chunck_size;
		clock_gettime(CLOCK_MONOTONIC, &start_timer);
		int compressed_size = compress((char*)buf+offset, (char*)buf+offset+buf_size/2,chunck_size);
		clock_gettime(CLOCK_MONOTONIC, &end_timer);
		duration += end_timer.tv_sec-start_timer.tv_sec+1.0*(end_timer.tv_nsec-start_timer.tv_nsec)/1e9;
	}
	duration /= ops;
	std::lock_guard<std::mutex> guard(IO_LOCK);
	LOG_I("Average compression latency on thread [%d] is : [%f]",thread_index, duration);
}
int main(){
	string file_name = "../../dataset/silesia/all";
	size_t iterations = 20;
	int num_threads = 2;
	size_t buf_size = 512*1024*1024;
	int chunck_size = 16*1024;
	void** bufs = new void*[num_threads];
	LOG_I("%-20s : %d","Total threads",num_threads);
	int num_chunck = 0;

	for(int i=0;i<num_threads;i++){
		bufs[i] = malloc_2m_numa(buf_size,0);//pages are on socket 0
	}
	ifstream ifs(file_name, ifstream::binary);
	assert(ifs.is_open());
	ifs.seekg(0,ifs.end);
	int file_size = ifs.tellg();
	assert(file_size<=(buf_size/2));
	num_chunck = (file_size+chunck_size-1)/chunck_size;
	ifs.clear();
	ifs.seekg(0,ios::beg);

	char* data = new char[buf_size/2];
	memset((void*)data,0,buf_size/2);
	ifs.read(data,file_size);
	ifs.close();
	for(int i=0;i<num_threads;i++){
		memcpy(bufs[i], (void*)data, file_size);
	}

	size_t ops = iterations * num_chunck;

	vector<thread> threads(num_threads);
	struct timespec start_timer,end_timer;
	clock_gettime(CLOCK_MONOTONIC, &start_timer);
	for(int i=0;i<num_threads;i++){
		threads[i] = thread(sub_task, i, bufs[i], buf_size, num_chunck, chunck_size, ops);
		set_cpu(threads[i],i*24);
	}
	for(int i=0;i<num_threads;i++){
		threads[i].join();
	}
	clock_gettime(CLOCK_MONOTONIC, &end_timer);
	double duration = end_timer.tv_sec-start_timer.tv_sec+1.0*(end_timer.tv_nsec-start_timer.tv_nsec)/1e9;
	LOG_I("Total Time : [%.3f] s",duration);
	LOG_I("Total Speed : [%.2f] Gb/s",8.0*num_threads*ops*chunck_size/1024/1024/1024/duration);
	return 0;
}