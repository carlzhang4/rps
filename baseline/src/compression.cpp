#include <lz4.h>
#include <assert.h>
#include "compression.hpp"

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