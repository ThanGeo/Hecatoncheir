cmake_minimum_required(VERSION 3.5.0)
project(Rasterizer VERSION 1.0.0)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

include_directories(SpatialLib)
if(NOT TARGET SpatialLib)
    add_subdirectory(SpatialLib)
endif()

set(ProjectType STATIC) # make STATIC or SHARED

set(SrcFiles
    src/def.cpp
    src/generate.cpp
    src/rasterization.cpp
    src/intervalization.cpp
    src/utils.cpp
)

# supress warnings
add_definitions(-w)

# find boost
find_package(Boost 1.50 REQUIRED COMPONENTS)
if(Boost_FOUND)
    add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")
    add_definitions("-DBOOST_BIND_GLOBAL_PLACEHOLDERS")
    include_directories(${Boost_INCLUDE_DIRS})
endif()

include_directories(include)
add_library(${PROJECT_NAME} ${ProjectType} ${SrcFiles})