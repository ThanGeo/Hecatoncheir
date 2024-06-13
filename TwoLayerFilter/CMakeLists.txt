cmake_minimum_required(VERSION 3.5.0)
project(TwoLayerFilter VERSION 1.0.0)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# supress warnings
add_definitions(-w)

set(ProjectType SHARED) # make STATIC or SHARED

include_directories(SpatialLib)
if(NOT TARGET SpatialLib)
    add_subdirectory(SpatialLib)
endif()

set(SrcFiles
    src/relation.cpp
    src/controller.cpp
    src/partitioning.cpp
    src/filter.cpp
    src/def.cpp
)

include_directories(include)
add_library(${PROJECT_NAME} ${ProjectType} ${SrcFiles})