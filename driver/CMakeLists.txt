add_executable(driver driver.cpp)
# Include directories for the driver
target_include_directories(driver PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../Hecatoncheir  # Include the header directory
)
target_link_libraries(driver Hecatoncheir ${MPI_LIBRARIES})
