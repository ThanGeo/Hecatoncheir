# ignore BOOST deprecated headers
add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")

set(CMAKE_CXX_FLAGS "-fopenmp")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

add_executable(driver driver.cpp)
# Include directories for the driver
target_include_directories(driver PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../Hecatoncheir/API  # Include the API directory
)

find_package(MPI REQUIRED)
if(MPI_FOUND)
    target_link_libraries(driver Hecatoncheir ${MPI_CXX_LIBRARIES})
endif()
