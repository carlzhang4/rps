cmake_minimum_required(VERSION 3.0)

set(CMAKE_C_COMPILER "gcc-11")
set(CMAKE_CXX_COMPILER "g++-11")
project(compression_baseline)

add_compile_options(-g -o3 -std=c++20 -static)

aux_source_directory(src SRCS)

add_executable(main ${SRCS} main.cpp)

target_link_libraries(main lz4 r pthread)
