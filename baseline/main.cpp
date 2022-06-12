#include <fstream>
#include <iostream>

#include <lz4.h>
#include <assert.h>
#include <algorithm>
#include "libr.hpp"

using namespace std;

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

void compression_benchmark(NetParam &net_param, int num_threads){
	int num_cpus = thread::hardware_concurrency();
	LOG_I("%-20s : %d","HardwareConcurrency",num_cpus);
	assert(num_threads<=num_cpus);
	PingPongInfo *info = new PingPongInfo[net_param.numNodes*num_threads]();
}
int main(int argc, char *argv[]){
	NetParam net_param;
	get_opt(net_param,argc,argv);
	socket_init(net_param);
    roce_init(net_param);

	// int num_threads = 1;
	// int ops = 10;
	// int pack_size = 64;
	// size_t buf_size = 128*1024*1024;
	// PingPongInfo *info = new PingPongInfo[net_param.numNodes*num_threads]();
	// void** bufs = new void*[num_threads];
	// QpHandler** qp_handlers = new QpHandler*[num_threads]();
	// for(int i=0;i<num_threads;i++){
	// 	bufs[i] = myMalloc2MbPage(buf_size);
	// 	for(int j=0;j<buf_size/sizeof(int);j++){
	// 		if(net_param.nodeId == 0){
	// 			((int**)bufs)[i][j] = j;
	// 		}else{
	// 			((int**)bufs)[i][j] = 0;
	// 		}
	// 	}
	// }
	// for(int i=0;i<num_threads;i++){
	// 	qp_handlers[i] = create_qp_rc(net_param,bufs[i],buf_size,info+i);
	// }
	// exchange_data(net_param, (char*)info, sizeof(PingPongInfo)*num_threads);
	// for(int i=0;i<net_param.numNodes*num_threads;i++){
	// 	print_pingpong_info(info+i);
	// }
	// int my_id = net_param.nodeId;
	// int dest_id = (net_param.nodeId+1)%net_param.numNodes;
	// for(int i=0;i<num_threads;i++){
	// 	connect_qp_rc(net_param,*qp_handlers[i],info+dest_id*num_threads+i,info+my_id*num_threads+i);
	// }

	// int ne;
	// struct ibv_wc *wc = NULL;
	// ALLOCATE(wc ,struct ibv_wc ,CTX_POLL_BATCH);
	// struct timespec start_timer,end_timer;
	// int start_timer_flag = 1;
	// if(net_param.nodeId == 0){
	// 	int cur_send = 0;
	// 	int cur_complete = 0;
	// 	for(int i=0;i<min(qp_handlers[0]->tx_depth,ops);i++){
	// 		post_send(*qp_handlers[0],(cur_send*pack_size)%buf_size,pack_size);
	// 		cur_send+=1;
	// 	}
	// 	while(cur_complete<ops){
	// 		ne = poll_send_cq(*qp_handlers[0],wc);
	// 		if(start_timer_flag==1 && ne!=0){
	// 			clock_gettime(CLOCK_MONOTONIC, &start_timer);
	// 			start_timer_flag=0;
	// 		}
	// 		for(int i=0;i<ne;i++){
	// 			if(cur_send<ops){
	// 				post_send(*qp_handlers[0],(cur_send*pack_size)%buf_size,pack_size);
	// 				cur_send+=1;
	// 			}
	// 			assert(wc[i].status == IBV_WC_SUCCESS);
	// 		}
	// 		cur_complete+=ne;
	// 	}
	// 	clock_gettime(CLOCK_MONOTONIC, &end_timer);
	// }else{
	// 	int cur_recv = 0;
	// 	int cur_complete = 0;
	// 	for(int i=0;i<min(qp_handlers[0]->rx_depth,ops);i++){
	// 		post_recv(*qp_handlers[0],(cur_recv*pack_size)%buf_size,pack_size);
	// 		cur_recv+=1;
	// 	}
	// 	while(cur_complete<ops){
	// 		ne = poll_recv_cq(*qp_handlers[0],wc);
	// 		if(start_timer_flag==1 && ne!=0){
	// 			clock_gettime(CLOCK_MONOTONIC, &start_timer);
	// 			start_timer_flag=0;
	// 		}
	// 		for(int i=0;i<ne;i++){
	// 			if(cur_recv<ops){
	// 				post_recv(*qp_handlers[0],(cur_recv*pack_size)%buf_size,pack_size);
	// 				cur_recv+=1;
	// 			}
	// 			assert(wc[i].status == IBV_WC_SUCCESS);
	// 			LOG_D("wc[%d].byte_len : %d",i,wc[i].byte_len);
	// 			for(int j=0;j<wc[i].byte_len/sizeof(int);j++){
	// 				int start_index = (cur_complete+i)*pack_size/sizeof(int);
	// 				cout<<((int**)bufs)[0][start_index+j]<<" ";
	// 			}
	// 			cout<<endl;
	// 		}
	// 		cur_complete+=ne;
	// 	}
	// 	clock_gettime(CLOCK_MONOTONIC, &end_timer);

	// 	int verify_data_size = (ops*pack_size)%buf_size;
	// 	for(int i=0;i<verify_data_size/sizeof(int);i++){
	// 		if(((int*)bufs[0])[i] != i){
	// 			LOG_D("Data verification failed, index:%d data:%d expected_data:%d",i,((int*)bufs[0])[i],i);
	// 			break;
	// 		}
	// 	}
	// 	LOG_I("Data verification success!");
	// }
	// double duration = end_timer.tv_sec-start_timer.tv_sec+1.0*(end_timer.tv_nsec-start_timer.tv_nsec)/1e9;
	// LOG_I("Time : %.3f s",duration);
	// LOG_I("Speed : %.2f Gb/s",8.0*ops*pack_size/1024/1024/1024/duration);
	ifstream ifs( "../../dataset/silesia/dickens", ifstream::binary );
	assert(ifs.is_open());
	ifs.seekg(0,ifs.end);
	int file_size = ifs.tellg();
	ifs.clear();
	ifs.seekg(0,ios::beg);

	char* data = new char[file_size];
	ifs.read(data,file_size);
	ifs.close();

    // char source_data[] = "2013-01-07 00:00:04,0.98644,0.98676 2013-01-07 00:01:19,0.98654,0.98676 2013-01-07 00:01:38,0.98644,0.98696";
    // int source_size = sizeof(source_data);
	char *source_data = data;
	int source_size = file_size;
	char* compressed_data = new char[source_size];
	char* decompressed_data = new char[source_size];
	int compressed_size = compress(source_data, compressed_data, source_size);
	int decompressed_size = decompress(compressed_data,decompressed_data,compressed_size,source_size);

	cout<<"source_size       : "<<source_size<<endl;
	cout<<"compressed_size   : "<<compressed_size<<endl;
	cout<<"decompressed_size : "<<decompressed_size<<endl;
	cout<<"Compare result    : "<<strcmp(source_data,decompressed_data)<<endl;

    return 0;
}