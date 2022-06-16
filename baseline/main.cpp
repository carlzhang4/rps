#include <fstream>
#include <iostream>

#include <lz4.h>
#include <assert.h>
#include <algorithm>
#include <gflags/gflags.h>
#include "libr.hpp"

using namespace std;
std::mutex IO_LOCK;

void set_cpu(thread& t,int cpu_index){
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(cpu_index, &cpuset);
	int rc = pthread_setaffinity_np(t.native_handle(),sizeof(cpu_set_t), &cpuset);
	if (rc != 0) {
		LOG_E("%-20s : %d","Error calling pthread_setaffinity_np",rc);
	}
}
int compress(char* source_str,char* compressed_str,int input_size){
	int compressed_size = LZ4_compress_default((const char *)(source_str), compressed_str, input_size, input_size);
	assert(compressed_size > 0);
	return compressed_size;
}

int decompress(char* compressed_str, char* decompressed_str, int compressed_size, int decompressed_capacity){
	int decompressed_size = LZ4_decompress_safe((const char*)compressed_str, decompressed_str, compressed_size, decompressed_capacity);
	assert(decompressed_size>0);
	return decompressed_size;
}

int init_bufs(int node_id, void** bufs, void* verify_buf, int* compressed_lengths, int num_threads, size_t buf_size, int chunck_size, int is_server_processing, string file_name){
	LOG_I("[Start Init Buffer] size : %ld, total threads : %d", buf_size, num_threads);
	for(int i=0;i<num_threads;i++){
		bufs[i] = myMalloc2MbPage(buf_size);
		if(node_id == 0){
			for(int j=0;j<buf_size/sizeof(int);j++){
				((int**)bufs)[i][j] = 0;
			}
		}
	}
	int num_chunck = 0;
	ifstream ifs(file_name, ifstream::binary);
	assert(ifs.is_open());
	ifs.seekg(0,ifs.end);
	int file_size = ifs.tellg();
	assert(file_size<=(buf_size/2));//buf is split into send/recv
	num_chunck = (file_size+chunck_size-1)/chunck_size;
	LOG_I("%-20s : %d","IsServerProcessing",is_server_processing);
	LOG_I("%-20s : %s","File name",file_name.c_str());
	LOG_I("%-20s : %d","File size",file_size);
	LOG_I("%-20s : %d","Num chunck",num_chunck);
	LOG_I("%-20s : %d","Chunck Size",chunck_size);
	LOG_I("%-20s : %d","Buf size/2",buf_size/2);
	ifs.clear();
	ifs.seekg(0,ios::beg);

	char* data = new char[buf_size/2];//can not be file_size, or last chunck would be different for verify and server compute result
	memset((void*)data,0,buf_size/2);
	ifs.read(data,file_size);
	ifs.close();

	if(node_id == 1){//client
		for(int i=0;i<num_threads;i++){
			memcpy(bufs[i], (void*)data, file_size);
		}
		if(is_server_processing){
			for(int i=0;i<num_chunck;i++){
				int compressed_size = compress(data+i*chunck_size, ((char*)verify_buf)+i*chunck_size, chunck_size);
				compressed_lengths[i] = compressed_size;
			}
		}else{
			memcpy(verify_buf, (void*)data, file_size);
			for(int i=0;i<num_chunck;i++){
				compressed_lengths[i] = chunck_size;
			}
		}	
	}else{
		memcpy(verify_buf, (void*)data, file_size);
	}
	LOG_I("[Init buf done]");
	return num_chunck;
}
void sub_task_server(int thread_index, QpHandler* handler, void* buf, void* verify_buf, size_t buf_size, int num_chunck, int chunck_size, size_t ops, int is_server_processing){
	int ne_send;
	int ne_recv;
	struct ibv_wc *wc_send = NULL;
	struct ibv_wc *wc_recv = NULL;
	ALLOCATE(wc_send ,struct ibv_wc ,CTX_POLL_BATCH);
	ALLOCATE(wc_recv ,struct ibv_wc ,CTX_POLL_BATCH);
	int* compressed_lengths = new int[num_chunck];
	struct timespec start_timer,end_timer;
	int start_timer_flag = 1;

	size_t cur_send = 0;
	size_t cur_send_complete = 0;
	size_t cur_recv = 0;
	size_t cur_recv_complete = 0;

	for(size_t i=0; i<min(size_t(handler->rx_depth),ops);i++){
		size_t offset = (cur_recv%num_chunck)*chunck_size + buf_size/2;
		post_recv(*handler,offset,chunck_size);
		cur_recv+=1;
	}
	while(cur_recv_complete<ops || cur_send_complete<ops){
		ne_recv = poll_recv_cq(*handler,wc_recv);
		if(start_timer_flag==1 && ne_recv!=0){
			clock_gettime(CLOCK_MONOTONIC, &start_timer);
			start_timer_flag=0;
		}
		for(int i=0;i<ne_recv;i++){
			if(cur_recv<ops){
				size_t offset = (cur_recv%num_chunck)*chunck_size + buf_size/2;
				post_recv(*handler,offset,chunck_size);
				cur_recv+=1;
			}
			assert(wc_recv[i].status == IBV_WC_SUCCESS);
			assert(wc_recv[i].byte_len == chunck_size);
			size_t offset =  (cur_recv_complete%num_chunck)*chunck_size;
			if(is_server_processing){
				int compressed_size = compress((char*)buf+buf_size/2+offset, (char*)buf+offset,chunck_size);
				compressed_lengths[cur_recv_complete%num_chunck] = compressed_size;
			}else{
				compressed_lengths[cur_recv_complete%num_chunck] = chunck_size;
			}	
			cur_recv_complete+=1;
		}

		while(cur_send<cur_recv_complete && (cur_send-cur_send_complete)<handler->tx_depth && cur_send<ops){
			size_t offset;
			if(is_server_processing){
				offset = (cur_send%num_chunck)*chunck_size;
			}else{
				offset = (cur_send%num_chunck)*chunck_size + buf_size/2;//if no processing, return received data directly
			}
			post_send(*handler,offset,compressed_lengths[cur_send%num_chunck]);
			cur_send+=1;
		}	
		
		ne_send = poll_send_cq(*handler,wc_send);
		for(int i=0;i<ne_send;i++){
			if(wc_send[i].status != IBV_WC_SUCCESS){
				LOG_I("Thread : %d, wc_send[%d].status = %d, i=%d",thread_index, cur_send_complete, wc_send[i].status, i);
				assert(wc_send[i].status == IBV_WC_SUCCESS);
			}
			cur_send_complete+=1;
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &end_timer);
	for(int i=0;i<num_chunck;i++){
		if(strncmp((char*)verify_buf+i*chunck_size, (char*)buf+buf_size/2+i*chunck_size, chunck_size) != 0){
			LOG_E("Server thread [%d] verify buf failed, index:[%d]",thread_index,i);
			break;
		}
	}
	double duration = end_timer.tv_sec-start_timer.tv_sec+1.0*(end_timer.tv_nsec-start_timer.tv_nsec)/1e9;
	std::lock_guard<std::mutex> guard(IO_LOCK);
	LOG_I("Server thread [%d] verify buf success",thread_index);
	LOG_I("Thread : %d, running on CPU : %d",thread_index,sched_getcpu());
	LOG_I("Time : %.3f s",duration);
	LOG_I("Speed : %.2f Gb/s",8.0*ops*chunck_size/1024/1024/1024/duration);
}
void sub_task_client(int thread_index, QpHandler* handler, void* buf, void* verify_buf, int* compressed_lengths, size_t buf_size, int num_chunck, int chunck_size, size_t ops){
	int ne_send;
	int ne_recv;
	struct ibv_wc *wc_send = NULL;
	struct ibv_wc *wc_recv = NULL;
	ALLOCATE(wc_send ,struct ibv_wc ,CTX_POLL_BATCH);
	ALLOCATE(wc_recv ,struct ibv_wc ,CTX_POLL_BATCH);
	struct timespec start_timer,end_timer;
	int start_timer_flag = 1;
	
	size_t cur_send = 0;
	size_t cur_send_complete = 0;
	size_t cur_recv = 0;
	size_t cur_recv_complete = 0;
	for(size_t i=0; i<min(size_t(handler->tx_depth),ops);i++){
		size_t offset = (cur_send%num_chunck)*chunck_size;
		post_send(*handler,offset,chunck_size);
		cur_send+=1;
	}
	for(size_t i=0; i<min(size_t(handler->rx_depth),ops);i++){
		size_t offset = (cur_recv%num_chunck)*chunck_size + buf_size/2;
		post_recv(*handler,offset,chunck_size);
		cur_recv+=1;
	}
	while(cur_recv_complete<ops || cur_send_complete<ops){
		ne_send = poll_send_cq(*handler, wc_send);
		if(start_timer_flag==1 && ne_send!=0){
			clock_gettime(CLOCK_MONOTONIC, &start_timer);
			start_timer_flag=0;
		}
		for(int i=0;i<ne_send;i++){
			if(cur_send<ops){
				size_t offset = (cur_send%num_chunck)*chunck_size;
				post_send(*handler,offset,chunck_size);
				cur_send+=1;
			}
			assert(wc_send[i].status == IBV_WC_SUCCESS);
			cur_send_complete+=1;
		}

		ne_recv = poll_recv_cq(*handler, wc_recv);
		for(int i=0;i<ne_recv;i++){
			if(cur_recv<ops){
				size_t offset = (cur_recv%num_chunck)*chunck_size + buf_size/2;
				post_recv(*handler,offset,chunck_size);
				cur_recv+=1;
			}
			assert(wc_recv[i].status == IBV_WC_SUCCESS);
			if(wc_recv[i].byte_len != compressed_lengths[cur_recv_complete%num_chunck]){
				LOG_E("Client thread[%d] verify length failed, index:[%d], byte_len:[%d], compressed_lengths:[%d]",thread_index,cur_recv_complete,wc_recv[i].byte_len,compressed_lengths[cur_recv_complete]);
			}
			cur_recv_complete+=1;
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &end_timer);
	for(int i=0;i<num_chunck;i++){
		if(strncmp(((char*)verify_buf)+i*chunck_size, ((char*)buf)+buf_size/2+i*chunck_size, compressed_lengths[i]) != 0){
			LOG_E("Client thread [%d] verify buf failed, index:[%d]",thread_index,i);
			break;
		}
		if(i==num_chunck-1){
			LOG_I("Client thread [%d] verify buf success",thread_index);
		}
	}
	double duration = end_timer.tv_sec-start_timer.tv_sec+1.0*(end_timer.tv_nsec-start_timer.tv_nsec)/1e9;
	std::lock_guard<std::mutex> guard(IO_LOCK);
	LOG_I("Thread : %d, running on CPU : %d",thread_index,sched_getcpu());
	LOG_I("Time : %.3f s",duration);
	LOG_I("Speed : %.2f Gb/s",8.0*ops*chunck_size/1024/1024/1024/duration);
}
void compression_benchmark(NetParam &net_param, int num_threads, int iterations, int chunck_size, int is_server_processing, string file_name){
	int num_cpus = thread::hardware_concurrency();
	assert(num_threads<=num_cpus);
	LOG_I("%-20s : %d","Total threads",num_threads);
	
	
	//init bufs and verify buf
	size_t buf_size = 512*1024*1024;
	void** bufs = new void*[num_threads];
	void* verify_buf = myMalloc2MbPage(buf_size/2);
	int* compressed_lengths = new int[buf_size/2/chunck_size];
	int num_chunck = init_bufs(net_param.nodeId,bufs,verify_buf,compressed_lengths,num_threads,buf_size,chunck_size,is_server_processing, file_name);
	size_t ops = size_t(1) * iterations * num_chunck;
	LOG_I("%-20s : %d","Iterations",iterations);
	LOG_I("%-20s : %d","Ops",ops);

	//connect all qps
	PingPongInfo *info = new PingPongInfo[net_param.numNodes*num_threads]();
	QpHandler** qp_handlers = new QpHandler*[num_threads]();
	for(int i=0;i<num_threads;i++){
		qp_handlers[i] = create_qp_rc(net_param,bufs[i],buf_size,info+i);
	}
	exchange_data(net_param, (char*)info, sizeof(PingPongInfo)*num_threads);
	for(int i=0;i<net_param.numNodes*num_threads;i++){
		print_pingpong_info(info+i);
	}
	int my_id = net_param.nodeId;
	int dest_id = (net_param.nodeId+1)%net_param.numNodes;
	for(int i=0;i<num_threads;i++){
		connect_qp_rc(net_param,*qp_handlers[i],info+dest_id*num_threads+i,info+my_id*num_threads+i);
	}

	vector<thread> threads(num_threads);
	struct timespec start_timer,end_timer;
	clock_gettime(CLOCK_MONOTONIC, &start_timer);
	for(int i=0;i<num_threads;i++){
		if(net_param.nodeId == 0){
			threads[i] = thread(sub_task_server, i, qp_handlers[i], bufs[i], verify_buf, buf_size, num_chunck, chunck_size, ops, is_server_processing);
		}else if(net_param.nodeId == 1){
			threads[i] = thread(sub_task_client, i, qp_handlers[i], bufs[i], verify_buf, compressed_lengths, buf_size, num_chunck, chunck_size, ops);
		}
		set_cpu(threads[i],i);
	}
	for(int i=0;i<num_threads;i++){
		threads[i].join();
	}
	clock_gettime(CLOCK_MONOTONIC, &end_timer);
	double duration = end_timer.tv_sec-start_timer.tv_sec+1.0*(end_timer.tv_nsec-start_timer.tv_nsec)/1e9;
	LOG_I("Total Time : %.3f s",duration);
	LOG_I("Total Speed : %.2f Gb/s",8.0*num_threads*ops*chunck_size/1024/1024/1024/duration);
}

DEFINE_int32(iterations,			1000,	"iterations");
DEFINE_int32(chunckSize,			16384,	"chunck_size");
DEFINE_int32(threads,				0,		"num_threads");
DEFINE_int32(numNodes,				0,		"numNodes");
DEFINE_int32(nodeId,				0,		"nodeId");
DEFINE_int32(isServerProcessing,	1,		"is_server_processing");
DEFINE_string(serverIp,				"",		"serverIp");
DEFINE_string(fileName,				"",		"file_name");
int main(int argc, char *argv[]){
	gflags::ParseCommandLineFlags(&argc, &argv, true); 
	
	int is_server_processing = FLAGS_isServerProcessing;
	int iterations = FLAGS_iterations;
	int chunck_size = FLAGS_chunckSize;
	int num_threads = FLAGS_threads;
	string file_name = "../../dataset/silesia/"+FLAGS_fileName;

	NetParam net_param;
	net_param.numNodes = FLAGS_numNodes;
	net_param.nodeId = FLAGS_nodeId;
	net_param.serverIp = FLAGS_serverIp;
	init_net_param(net_param);
	
	socket_init(net_param);
    roce_init(net_param,num_threads);
	compression_benchmark(net_param, num_threads, iterations, chunck_size, is_server_processing, file_name);
    return 0;
}