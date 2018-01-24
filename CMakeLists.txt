cmake_minimum_required(VERSION 3.9)
project(chidb C)
set(CMAKE_C_STANDARD 99)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ./cmake-build-debug/)

INCLUDE_DIRECTORIES(
        include
        src/simclist
        )

aux_source_directory( src/libchisql DIR_SRCS)
aux_source_directory( src/libchidb DIR_SRCS)
aux_source_directory( src/shell DIR_SRCS)
aux_source_directory( src/simclist DIR_SRCS)
aux_source_directory( src/editline DIR_SRCS)


add_executable(chidb ${DIR_SRCS} )

#add_executable(testTokenizer ${DIR_SRCS} src_test/parser/testTokenizer.c)