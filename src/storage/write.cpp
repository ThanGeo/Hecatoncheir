#include "storage/write.h"

namespace storage
{
    namespace writer
    {
        namespace partitionFile
        {
            DB_STATUS appendDatasetInfoToPartitionFile(FILE* outFile, Dataset* dataset) {
                // datatype
                fwrite(&dataset->dataType, sizeof(DataTypeE), 1, outFile);
                // nikcname length + string
                int length = dataset->nickname.length();
                fwrite(&length, sizeof(int), 1, outFile);
                fwrite(dataset->nickname.data(), length * sizeof(char), length, outFile);
                // dataspace MBR
                fwrite(&dataset->dataspaceInfo.xMinGlobal, sizeof(double), 1, outFile);
                fwrite(&dataset->dataspaceInfo.yMinGlobal, sizeof(double), 1, outFile);
                fwrite(&dataset->dataspaceInfo.xMaxGlobal, sizeof(double), 1, outFile);
                fwrite(&dataset->dataspaceInfo.yMaxGlobal, sizeof(double), 1, outFile);
                // logger::log_success("Wrote:", dataset->dataspaceInfo.xMinGlobal, dataset->dataspaceInfo.yMinGlobal, dataset->dataspaceInfo.xMaxGlobal, dataset->dataspaceInfo.yMaxGlobal);
                return DBERR_OK;
            }

            DB_STATUS appendBatchToPartitionFile(FILE* outFile, Batch* batch, Dataset* dataset) {
                size_t elementsWritten;
                // store each geometry
                for(auto &it: batch->objects) {
                    elementsWritten = fwrite(&it.recID, sizeof(size_t), 1, outFile);
                    if (elementsWritten != 1) {
                        logger::log_error(DBERR_DISK_WRITE_FAILED, "Wrote", elementsWritten, "instead of 1 element for recID.");
                        return DBERR_DISK_WRITE_FAILED;
                    }
                    int partitionCount = it.getPartitionCount();
                    elementsWritten = fwrite(&partitionCount, sizeof(int), 1, outFile);
                    if (elementsWritten != 1) {
                        logger::log_error(DBERR_DISK_WRITE_FAILED, "Wrote", elementsWritten, "instead of 1 element for partitionCount.");
                        return DBERR_DISK_WRITE_FAILED;
                    }
                    elementsWritten = fwrite(it.getPartitionsRef()->data(), sizeof(int), partitionCount * 2, outFile);
                    if (elementsWritten != partitionCount * 2) {
                        logger::log_error(DBERR_DISK_WRITE_FAILED, "Wrote", elementsWritten, "instead of", partitionCount * 2, " elements for partitions.");
                        return DBERR_DISK_WRITE_FAILED;
                    }
                    int vertexCount = it.getVertexCount();
                    elementsWritten = fwrite(&vertexCount, sizeof(int), 1, outFile);
                    if (elementsWritten != 1) {
                        logger::log_error(DBERR_DISK_WRITE_FAILED, "Wrote", elementsWritten, "instead of", 1, " element for vertex count.");
                        return DBERR_DISK_WRITE_FAILED;
                    }

                    const std::vector<bg_point_xy>* bgPointsRef = it.getReferenceToPoints();
                    std::vector<double> pointList(vertexCount * 2);
                    for (int i=0; i<vertexCount; i++) {
                        pointList[2*i] = bgPointsRef->at(i).x();
                        pointList[2*i + 1] = bgPointsRef->at(i).y();
                    }
                    elementsWritten = fwrite(pointList.data(), sizeof(double), vertexCount * 2, outFile);
                    if (elementsWritten != vertexCount * 2) {
                        logger::log_error(DBERR_DISK_WRITE_FAILED, "Wrote", elementsWritten, "instead of", vertexCount * 2, " elements for points.");
                        return DBERR_DISK_WRITE_FAILED;
                    }
                }
                // append batch count to dataset count
                dataset->totalObjects += batch->objectCount;

                return DBERR_OK;
            }

        }
        
        DB_STATUS updateObjectCountInFile(FILE* outFile, size_t objectCount) {
            // go to the begining place in the file
            fseek(outFile, 0, SEEK_SET);
            size_t elementsWritten = fwrite(&objectCount, sizeof(size_t), 1, outFile);
            if (elementsWritten != 1) {
                logger::log_error(DBERR_DISK_WRITE_FAILED, "Failed while updating object count in partition file");
                return DBERR_DISK_WRITE_FAILED;
            }
            fflush(outFile);
            return DBERR_OK;
        }
    }
}