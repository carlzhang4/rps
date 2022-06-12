#include <fstream>
#include <iostream>

#include <lz4.h>
#include <assert.h>
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
int main(int argc, char *argv[]){
	NetParam net_param;
	get_opt(net_param,argc,argv);
	socket_init(net_param);
    roce_init(net_param);
    char source_data[] = "2013-01-07 00:00:04,0.98644,0.98676 2013-01-07 00:01:19,0.98654,0.98676 2013-01-07 00:01:38,0.98644,0.98696";
    int source_size = sizeof(source_data);

	char* compressed_data = new char[source_size];
	char* decompressed_data = new char[source_size];
	int compressed_size = compress(source_data, compressed_data, source_size);
	int decompressed_size = decompress(compressed_data,decompressed_data,compressed_size,source_size);

	cout<<"source_size       : "<<source_size<<endl;
	cout<<"compressed_size   : "<<compressed_size<<endl;
	cout<<"decompressed_size : "<<decompressed_size<<endl;
	cout<<"source_data       : "<<source_data<<endl;
	cout<<"decompressed_data : "<<decompressed_data<<endl;

    return 0;
}