#include "storage/read.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace storage
{   
    namespace reader
    {
        namespace csv
        {
            static DB_STATUS calculateDatasetMetadata(Dataset* dataset) {
                DB_STATUS ret = DBERR_OK;
                // open file
                std::ifstream fin(dataset->metadata.path);
                if (!fin.is_open()) {
                    logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
                    return DBERR_MISSING_FILE;
                }
                std::string line;
                // read total objects (first line of file)
                std::getline(fin, line);
                fin.close();
                size_t objectCount = (size_t) std::stoull(line);
                // dataset global bounds
                double global_xMin = std::numeric_limits<int>::max();
                double global_yMin = std::numeric_limits<int>::max();
                double global_xMax = -std::numeric_limits<int>::max();
                double global_yMax = -std::numeric_limits<int>::max();
                // spawn all available threads (processors)
                #pragma omp parallel firstprivate(line, objectCount) reduction(min:global_xMin) reduction(min:global_yMin) reduction(max:global_xMax)  reduction(max:global_yMax)
                {
                    size_t recID;
                    double x,y;
                    DB_STATUS local_ret = DBERR_OK;
                    int tid = omp_get_thread_num();
                    // calculate which lines this thread will handle
                    size_t linesPerThread = (objectCount / MAX_THREADS);
                    size_t fromLine = 1 + (tid * linesPerThread);          // first line is object count
                    size_t toLine = 1 + ((tid + 1) * linesPerThread);    // exclusive
                    if (tid == MAX_THREADS - 1) {
                        toLine = objectCount+1;
                    }
                    // open file
                    std::ifstream fin(dataset->metadata.path);
                    if (!fin.is_open()) {
                        logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
                        #pragma omp cancel parallel
                        ret = DBERR_MISSING_FILE;
                    }
                    // jump to start line
                    for (size_t i=0; i<fromLine; i++) {
                        fin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
                    }
                    // loop
                    size_t currentLine = fromLine;
                    std::string token;
                    while (true) {
                        // next object
                        std::getline(fin, line);                
                        std::stringstream ss(line);
                        // recID
                        std::getline(ss, token, ',');
                        // recID = (size_t) std::stoull(token);
                        recID = currentLine;
                        // Read the coords x,y
                        while (std::getline(ss, token, ',')) {
                            std::stringstream coordStream(token);
                            // Split the x and y values
                            std::getline(coordStream, token, ' ');
                            x = std::stof(token);
                            std::getline(coordStream, token, ' ');
                            y = std::stof(token);
                            // bounds
                            global_xMin = std::min(global_xMin, x);
                            global_yMin = std::min(global_yMin, y);
                            global_xMax = std::max(global_xMax, x);
                            global_yMax = std::max(global_yMax, y);
                        }
                        currentLine += 1;
                        if (currentLine >= toLine) {
                            // the last line for this thread has been read
                            break;
                        }
                    }
                    // close file
                    fin.close();
                }
                if (ret != DBERR_OK) {
                    return ret;
                }
                // set extent
                dataset->metadata.dataspaceMetadata.set(global_xMin, global_yMin, global_xMax, global_yMax);
                return ret;
            }
        }   // csv

        namespace wkt
        {
            static DB_STATUS calculateDatasetMetadata(Dataset* dataset) {
                DB_STATUS ret = DBERR_OK;
                // open file
                std::ifstream fin(dataset->metadata.path);
                if (!fin.is_open()) {
                    logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
                    return DBERR_MISSING_FILE;
                }
                std::string line;
                // count total objects (lines)
                size_t totalObjects = 0;
                ret = storage::reader::getDatasetLineCount(dataset, totalObjects);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // dataset global bounds
                double global_xMin = std::numeric_limits<int>::max();
                double global_yMin = std::numeric_limits<int>::max();
                double global_xMax = -std::numeric_limits<int>::max();
                double global_yMax = -std::numeric_limits<int>::max();
                // spawn all available threads (processors)
                #pragma omp parallel firstprivate(line, totalObjects) reduction(min:global_xMin) reduction(min:global_yMin) reduction(max:global_xMax)  reduction(max:global_yMax)
                {
                    DB_STATUS local_ret = DBERR_OK;
                    int tid = omp_get_thread_num();
                    // calculate which lines this thread will handle
                    size_t linesPerThread = (totalObjects / MAX_THREADS);
                    size_t fromLine = 1 + (tid * linesPerThread);          // first line is object count
                    size_t toLine = 1 + ((tid + 1) * linesPerThread);    // exclusive
                    if (tid == MAX_THREADS - 1) {
                        toLine = totalObjects+1;
                    }
                    // open file
                    std::ifstream fin(dataset->metadata.path);
                    if (!fin.is_open()) {
                        logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
                        #pragma omp cancel parallel
                        ret = DBERR_MISSING_FILE;
                    }
                    // jump to start line
                    for (size_t i=0; i<fromLine; i++) {
                        fin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
                    }
                    
                    // create empty object based on data type
                    Shape object;
                    local_ret = shape_factory::createEmpty(dataset->metadata.dataType, object);
                    if (local_ret != DBERR_OK) {
                        // error creating shape
                        #pragma omp cancel parallel
                        ret = local_ret;
                    } else {
                        // loop
                        size_t currentLine = fromLine;
                        std::string token;
                        while (true) {
                            object.reset();
                            // next object
                            std::getline(fin, line);   
                            // parse line to get only the first column (wkt geometry)
                            std::stringstream ss(line);
                            std::getline(ss, token, '\t');
                            // set object from the WKT
                            local_ret = object.setFromWKT(token);
                            if (local_ret == DBERR_INVALID_GEOMETRY) {
                                // this line is not the appropriate geometry type, so just ignore
                            } else if (local_ret != DBERR_OK) {
                                // some other error occured, interrupt
                                #pragma omp cancel parallel
                                ret = local_ret;
                            } else {
                                // set the MBR
                                object.setMBR();
                                // get global bounds
                                global_xMin = std::min(global_xMin, object.mbr.pMin.x);
                                global_yMin = std::min(global_yMin, object.mbr.pMin.y);
                                global_xMax = std::max(global_xMax, object.mbr.pMax.x);
                                global_yMax = std::max(global_yMax, object.mbr.pMax.y);
                            }
                            // next line
                            currentLine += 1;
                            if (currentLine >= toLine) {
                                // the last line for this thread has been read
                                break;
                            }
                        }
                    }
                    // close file
                    fin.close();
                }
                if (ret != DBERR_OK) {
                    return ret;
                }
                // set total objects (valid + invalid)
                dataset->totalObjects = totalObjects;
                // set extent
                dataset->metadata.dataspaceMetadata.set(global_xMin, global_yMin, global_xMax, global_yMax);
                return ret;
            } 
        } // wkt 

        DB_STATUS getDatasetLineCount(Dataset* dataset, size_t &totalLines) {
            int fd = open(dataset->metadata.path.c_str(), O_RDONLY);
            if (fd == -1) {
                logger::log_error(DBERR_MISSING_FILE, "Could not open dataset file at", dataset->metadata.path);
                return DBERR_MISSING_FILE;
            }

            struct stat fileStat;
            if (fstat(fd, &fileStat) == -1) {
                close(fd);
                throw std::runtime_error("Could not get file stats");
                logger::log_error(DBERR_OPERATION_FAILED, "Could not get file stats through fstat syscall.");
                return DBERR_OPERATION_FAILED;
            }

            size_t fileSize = fileStat.st_size;
            if (fileSize == 0) {
                close(fd);
                logger::log_warning("Dataset file is empty. Path:", dataset->metadata.path);
                return DBERR_OK;
            }

            char* fileData = static_cast<char*>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));
            if (fileData == MAP_FAILED) {
                close(fd);
                logger::log_error(DBERR_OPERATION_FAILED, "MMAP failed for file of dataset with index", dataset->metadata.internalID);
                return DBERR_OPERATION_FAILED;
            }

            size_t lineCount = 0;
            for (size_t i = 0; i < fileSize; ++i) {
                if (fileData[i] == '\n') {
                    lineCount++;
                }
            }

            munmap(fileData, fileSize);
            close(fd);
            totalLines = lineCount;
            return DBERR_OK;
        }

        DB_STATUS calculateDatasetMetadata(Dataset* dataset) {
            DB_STATUS ret = DBERR_OK;
            switch (dataset->metadata.fileType) {
                // perform the partitioning
                case hec::FT_CSV:
                    // csv dataset
                    ret = csv::calculateDatasetMetadata(dataset);
                    if (ret != DBERR_OK) {
                        logger::log_error(DBERR_OPERATION_FAILED, "Calculating metadata failed for dataset", dataset->metadata.internalID);
                        return ret;
                    }
                    break;
                case hec::FT_WKT:
                    // wkt dataset
                    // logger::log_task("Calculating dataset metadata for dataset", dataset->metadata.datasetName);
                    ret = wkt::calculateDatasetMetadata(dataset);
                    if (ret != DBERR_OK) {
                        logger::log_error(DBERR_OPERATION_FAILED, "Calculating metadata failed for dataset", dataset->metadata.internalID);
                        return ret;
                    }
                    // logger::log_task("Done!");
                    break;
                default:
                    logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported data file type:", dataset->metadata.fileType);
                    break;
            }
            return ret;
        }

        namespace partitionFile
        {
            DB_STATUS loadDatasetMetadata(FILE* pFile, Dataset *dataset) {
                DB_STATUS ret = DBERR_OK;
                double xMin, yMin, xMax, yMax;
                int length;
                // object count
                size_t elementsRead = fread(&dataset->totalObjects, sizeof(size_t), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the object count");
                    return DBERR_DISK_READ_FAILED;
                }
                // read data type
                elementsRead = fread(&dataset->metadata.dataType, sizeof(DataType), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the dataset's datatype");
                    return DBERR_DISK_READ_FAILED;
                }
                // dataspace MBR
                elementsRead = 0;
                elementsRead += fread(&xMin, sizeof(double), 1, pFile);
                elementsRead += fread(&yMin, sizeof(double), 1, pFile);
                elementsRead += fread(&xMax, sizeof(double), 1, pFile);
                elementsRead += fread(&yMax, sizeof(double), 1, pFile);
                if (elementsRead != 4) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the dataset's dataspace MBR");
                    return DBERR_DISK_READ_FAILED;
                }
                dataset->metadata.dataspaceMetadata.set(xMin, yMin, xMax, yMax);
                return ret;
            }

            DB_STATUS loadNextObjectComplete(FILE* pFile, Shape &object) {
                // read recID and partition count
                int partitionCount;
                size_t elementsRead = fread(&object.recID, sizeof(size_t), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the recID");
                    return DBERR_DISK_READ_FAILED;
                }
                elementsRead = fread(&partitionCount, sizeof(int), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the  partitionCount");
                    return DBERR_DISK_READ_FAILED;
                }
                // read partitions data
                std::vector<int> partitionVector;
                partitionVector.resize(partitionCount * 2);
                elementsRead = fread(partitionVector.data(), sizeof(int), partitionCount * 2, pFile);
                if (elementsRead != partitionCount * 2) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the partition IDs");
                    return DBERR_DISK_READ_FAILED;
                }
                // set the partitions in the object
                object.setPartitions(partitionVector, partitionCount);

                // read vertex count
                int vertexCount;
                elementsRead = fread(&vertexCount, sizeof(int), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the vertex count");
                    return DBERR_DISK_READ_FAILED;
                }
                // read the points
                std::vector<double> coords(vertexCount*2);
                elementsRead = fread(coords.data(), sizeof(double), vertexCount*2, pFile);
                if (elementsRead != vertexCount*2) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the vertices");
                    return DBERR_DISK_READ_FAILED;
                }
                // reset mbr
                object.resetMBR();
                for (auto it = coords.begin(); it != coords.end(); it += 2) {
                    // mbr
                    object.mbr.pMin.x = std::min(object.mbr.pMin.x, *it);
                    object.mbr.pMin.y = std::min(object.mbr.pMin.y, *(it+1));
                    object.mbr.pMax.x = std::max(object.mbr.pMax.x, *it);
                    object.mbr.pMax.y = std::max(object.mbr.pMax.y, *(it+1));
                    // vector data
                    object.addPoint(*it, *(it+1));
                }
                // correct
                object.correctGeometry();

                // printf("Object %ld has %d partitions:\n ", object.recID, object.getPartitionCount());
                // std::vector<int>* partitionRef = object.getPartitionsRef();
                // for (int i=0; i<object.getPartitionCount(); i++) {
                //     printf("(%d,%s),", object.getPartitionID(i), mapping::twoLayerClassIntToStr(object.getPartitionClass(i)).c_str());
                // }
                // printf("\n");
                
                return DBERR_OK;
            }


            static DB_STATUS loadPolygonDatasetContents(FILE *pFile, Dataset *dataset) {
                DB_STATUS ret = DBERR_OK;
                // read data (vector and create MBRs as well)
                for (size_t i=0; i<dataset->totalObjects; i++) {
                    // read next object
                    Shape object = shape_factory::createEmptyPolygonShape();
                    ret = loadNextObjectComplete(pFile, object);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to read MBR for object number", i, "of", dataset->totalObjects);
                        return ret;
                    }
                    // store in dataset
                    ret = dataset->addObject(object);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Error when adding object with id", object.recID, "in dataset", dataset->metadata.internalID);
                        return ret;
                    }
                }

                return ret;
            }
            
            static DB_STATUS loadLinestringDatasetContents(FILE *pFile, Dataset *dataset) {
                DB_STATUS ret = DBERR_OK;
                // read data (vector and create MBRs as well)
                for (size_t i=0; i<dataset->totalObjects; i++) {
                    // read next object
                    Shape object = shape_factory::createEmptyLineStringShape();
                    ret = loadNextObjectComplete(pFile, object);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to read MBR for object number", i, "of", dataset->totalObjects);
                        return ret;
                    }
                    // store in dataset
                    ret = dataset->addObject(object);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Error when adding object with id", object.recID, "in dataset", dataset->metadata.internalID);
                        return ret;
                    }
                }
                return ret;
            }

            static DB_STATUS loadPointDatasetContents(FILE *pFile, Dataset* dataset) {
                DB_STATUS ret = DBERR_OK;
                // read data (vector and create MBRs as well)
                for (size_t i=0; i<dataset->totalObjects; i++) {
                    // read next object
                    Shape object = shape_factory::createEmptyPointShape();
                    ret = loadNextObjectComplete(pFile, object);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to read MBR for object number", i, "of", dataset->totalObjects);
                        return ret;
                    }
                    // store in dataset
                    ret = dataset->addObject(object);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Error when adding object with id", object.recID, "in dataset", dataset->metadata.internalID);
                        return ret;
                    }
                }
                return ret;
            }

            static DB_STATUS loadRectangleDatasetContents(FILE *pFile, Dataset* dataset) {
                DB_STATUS ret = DBERR_OK;
                // read data (vector and create MBRs as well)
                for (size_t i=0; i<dataset->totalObjects; i++) {
                    // read next object
                    Shape object = shape_factory::createEmptyRectangleShape();
                    ret = loadNextObjectComplete(pFile, object);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to read MBR for object number", i, "of", dataset->totalObjects);
                        return ret;
                    }
                    // store in dataset
                    ret = dataset->addObject(object);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Error when adding object with id", object.recID, "in dataset", dataset->metadata.internalID);
                        return ret;
                    }
                }
                return ret;
            }

            DB_STATUS loadDatasetComplete(Dataset *dataset) {
                DB_STATUS ret = DBERR_OK;
                int length = 0;
                // open partition file
                FILE* pFile = fopen(dataset->metadata.path.c_str(), "rb");
                if (pFile == NULL) {
                    logger::log_error(DBERR_MISSING_FILE, "Could not open partitioned dataset file from path:", dataset->metadata.path);
                    return DBERR_MISSING_FILE;
                }
                // dataset metadata
                ret = loadDatasetMetadata(pFile, dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to load dataset metadata");
                    goto CLOSE_AND_EXIT;
                }
                // dataset contents based on type
                switch (dataset->metadata.dataType) {
                    case DT_POLYGON:
                        ret = loadPolygonDatasetContents(pFile, dataset);
                        break;
                    case DT_LINESTRING:
                        ret = loadLinestringDatasetContents(pFile, dataset);
                        break;
                    case DT_POINT:
                        ret = loadPointDatasetContents(pFile, dataset);
                        break;
                    case DT_RECTANGLE:
                        ret = loadRectangleDatasetContents(pFile, dataset);
                        break;
                    default:
                        logger::log_error(DBERR_INVALID_DATATYPE, "Invalid datatype on load dataset");
                        ret = DBERR_INVALID_DATATYPE;
                        goto CLOSE_AND_EXIT; 
                }
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to load dataset contents");
                    goto CLOSE_AND_EXIT;
                }
                // sort two layer
                dataset->twoLayerIndex.sortPartitionsOnY();
                // logger::log_success("Loaded", dataset.totalObjects, "objects");
CLOSE_AND_EXIT:
                fclose(pFile);
                return ret;
            }
        }


    }
}
