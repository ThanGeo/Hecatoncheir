#include "storage/write.h"

namespace storage
{
    namespace writer
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

        DB_STATUS appendBatchToPartitionFile(FILE* outFile, GeometryBatchT* batch, Dataset* dataset) {
            // store each geometry
            for(auto &it: batch->geometries) {
                fwrite(&it.recID, sizeof(int), 1, outFile);
                fwrite(&it.partitionCount, sizeof(int), 1, outFile);
                fwrite(it.partitions.data(), sizeof(int), it.partitionCount * 2, outFile);
                fwrite(&it.vertexCount, sizeof(int), 1, outFile);
                fwrite(it.coords.data(), sizeof(double), it.coords.size(), outFile);
            }

            // append batch count to dataset count
            dataset->totalObjects += batch->objectCount;

            return DBERR_OK;
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