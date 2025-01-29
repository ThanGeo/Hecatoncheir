#include "storage/write.h"

namespace storage
{
    namespace writer
    {
        namespace partitionFile
        {
            DB_STATUS appendDatasetMetadataToPartitionFile(FILE* outFile, Dataset* dataset) {
                // replace with "size_t result = fwrite(&metadata, sizeof(DataspaceMetadata), 1, file);"
                
                // datatype
                fwrite(&dataset->metadata.dataType, sizeof(DataType), 1, outFile);
                // dataspace MBR
                fwrite(&dataset->metadata.dataspaceMetadata.xMinGlobal, sizeof(double), 1, outFile);
                fwrite(&dataset->metadata.dataspaceMetadata.yMinGlobal, sizeof(double), 1, outFile);
                fwrite(&dataset->metadata.dataspaceMetadata.xMaxGlobal, sizeof(double), 1, outFile);
                fwrite(&dataset->metadata.dataspaceMetadata.yMaxGlobal, sizeof(double), 1, outFile);
                // logger::log_success("Wrote:", dataset->dataspaceMetadata.xMinGlobal, dataset->dataspaceMetadata.yMinGlobal, dataset->dataspaceMetadata.xMaxGlobal, dataset->dataspaceMetadata.yMaxGlobal);
                return DBERR_OK;
            }

            DB_STATUS appendBatchToPartitionFile(FILE* outFile, Batch* batch, Dataset* dataset) {
                DB_STATUS ret = DBERR_OK;
                size_t elementsWritten;
                // store each geometry
                for(auto &it: batch->objects) {
                    elementsWritten = fwrite(&it.recID, sizeof(size_t), 1, outFile);
                    if (elementsWritten != 1) {
                        logger::log_error(DBERR_DISK_WRITE_FAILED, "Wrote", elementsWritten, "instead of 1 element for recID.");
                        return DBERR_DISK_WRITE_FAILED;
                    }
                    int vertexCount = it.getVertexCount();
                    elementsWritten = fwrite(&vertexCount, sizeof(int), 1, outFile);
                    if (elementsWritten != 1) {
                        logger::log_error(DBERR_DISK_WRITE_FAILED, "Wrote", elementsWritten, "instead of", 1, " element for vertex count.");
                        return DBERR_DISK_WRITE_FAILED;
                    }

                    // const std::vector<bg_point_xy>* bgPointsRef = it.getReferenceToPoints();
                    // std::vector<double> pointList(vertexCount * 2);
                    // for (int i=0; i<vertexCount; i++) {
                    //     pointList[2*i] = bgPointsRef->at(i).x();
                    //     pointList[2*i + 1] = bgPointsRef->at(i).y();
                    // }
                    // elementsWritten = fwrite(pointList.data(), sizeof(double), vertexCount * 2, outFile);
                    // if (elementsWritten != vertexCount * 2) {
                    //     logger::log_error(DBERR_DISK_WRITE_FAILED, "Wrote", elementsWritten, "instead of", vertexCount * 2, " elements for points.");
                    //     return DBERR_DISK_WRITE_FAILED;
                    // }

                    double* bufferPtr;
                    int bufferElementCount;
                    ret = it.serializeCoordinates(&bufferPtr, bufferElementCount);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Serializing coordinates failed.");
                        return ret;
                    }
                    elementsWritten = fwrite(bufferPtr, sizeof(double), bufferElementCount, outFile);
                    if (elementsWritten != bufferElementCount) {
                        logger::log_error(DBERR_DISK_WRITE_FAILED, "Wrote", elementsWritten, "instead of", bufferElementCount, " elements for points.");
                        return DBERR_DISK_WRITE_FAILED;
                    }
                    
                }
                // append batch count to dataset count
                dataset->totalObjects += batch->objectCount;

                return ret;
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