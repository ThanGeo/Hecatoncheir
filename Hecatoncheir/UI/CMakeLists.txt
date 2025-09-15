# ignore BOOST deprecated headers
add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")

set(CMAKE_CXX_FLAGS "-fopenmp")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O1 -fsanitize=address,leak -fno-omit-frame-pointer -fno-common")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

add_executable(
    hec hec.cpp

    backend/hec.cpp

)
# Include directories for the driver
target_include_directories(test PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/
    ${CMAKE_CURRENT_SOURCE_DIR}/backend
)

find_package(MPI REQUIRED)
if(MPI_FOUND)
    target_link_libraries(test Hecatoncheir ${MPI_CXX_LIBRARIES})
endif()
