#include "storage/read.h"

namespace storage
{   
    namespace reader
    {

        namespace partitionFile
        {
            DB_STATUS loadDatasetInfo(FILE* pFile, Dataset *dataset) {
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
                elementsRead = fread(&dataset->dataType, sizeof(DataTypeE), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the dataset's datatype");
                    return DBERR_DISK_READ_FAILED;
                }
                // read nickname length + string
                elementsRead = 0;
                elementsRead += fread(&length, sizeof(int), 1, pFile);
                elementsRead += fread(dataset->nickname.data(), length * sizeof(char), length, pFile);
                if (elementsRead != length + 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the dataset's nickname");
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
                dataset->dataspaceInfo.set(xMin, yMin, xMax, yMax);
                // logger::log_success("loaded dataset info:", dataset.totalObjects, dataset.dataType, dataset.nickname, dataset.dataspaceInfo.xMinGlobal, dataset.dataspaceInfo.yMinGlobal, dataset.dataspaceInfo.xMaxGlobal, dataset.dataspaceInfo.yMaxGlobal);
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
                object.mbr.pMin.x = std::numeric_limits<int>::max();
                object.mbr.pMin.y = std::numeric_limits<int>::max();
                object.mbr.pMax.x = -std::numeric_limits<int>::max();
                object.mbr.pMax.y = -std::numeric_limits<int>::max();
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
                        logger::log_error(ret, "Error when adding object with id", object.recID, "in dataset", dataset->nickname);
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
                        logger::log_error(ret, "Error when adding object with id", object.recID, "in dataset", dataset->nickname);
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
                        logger::log_error(ret, "Error when adding object with id", object.recID, "in dataset", dataset->nickname);
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
                        logger::log_error(ret, "Error when adding object with id", object.recID, "in dataset", dataset->nickname);
                        return ret;
                    }
                }
                return ret;
            }

            DB_STATUS loadDatasetComplete(Dataset *dataset) {
                DB_STATUS ret = DBERR_OK;
                int length = 0;
                // open partition file
                FILE* pFile = fopen(dataset->path.c_str(), "rb");
                if (pFile == NULL) {
                    logger::log_error(DBERR_MISSING_FILE, "Could not open partitioned dataset file from path:", dataset->path);
                    return DBERR_MISSING_FILE;
                }
                // dataset info
                ret = loadDatasetInfo(pFile, dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to load dataset info");
                    goto CLOSE_AND_EXIT;
                }
                // dataset contents based on type
                switch (dataset->dataType) {
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
