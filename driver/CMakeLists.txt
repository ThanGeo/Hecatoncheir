# ignore BOOST deprecated headers
add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")



add_executable(driver driver.cpp)
# Include directories for the driver
target_include_directories(driver PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../Hecatoncheir  # Include the header directory
)

find_package(MPI REQUIRED)
if(MPI_FOUND)
    target_link_libraries(driver Hecatoncheir ${MPI_CXX_LIBRARIES})
endif()
