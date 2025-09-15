cmake_minimum_required(VERSION 3.5)
project(Root)

set(CMAKE_CXX_FLAGS "-fopenmp")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O1 -fsanitize=address,leak -fno-omit-frame-pointer -fno-common")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

add_subdirectory(Hecatoncheir)
add_subdirectory(driver)
