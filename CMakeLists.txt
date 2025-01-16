cmake_minimum_required(VERSION 3.10)
project(Root)

set(CMAKE_CXX_FLAGS "-fopenmp")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

add_subdirectory(Hecatoncheir)
add_subdirectory(driver)
