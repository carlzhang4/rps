#ifndef _COMPRESSION_HPP_
#define _COMPRESSION_HPP_

int compress(char* source_str,char* compressed_str,int input_size);

int decompress(char* compressed_str, char* decompressed_str, int compressed_size, int decompressed_capacity);

#endif