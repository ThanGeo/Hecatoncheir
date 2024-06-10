#include "storage/write.h"

namespace storage
{
    namespace writer
    {
        DB_STATUS appendBatchToDatasetPartitionFile(FILE* outFile, GeometryBatchT* batch, spatial_lib::DatasetT* dataset) {
            // store each geometry
            for(auto &it: batch->geometries) {
                fwrite(&it.recID, sizeof(int), 1, outFile);
                fwrite(&it.partitionCount, sizeof(int), 1, outFile);
                fwrite(it.partitionIDs.data(), sizeof(int), it.partitionCount, outFile);
                fwrite(&it.vertexCount, sizeof(int), 1, outFile);
                fwrite(it.coords.data(), sizeof(double), it.coords.size(), outFile);
            }

            // append batch count to dataset count
            dataset->totalObjects += batch->objectCount;

            return DBERR_OK;
        }

        DB_STATUS updateObjectCountInPartitionFile(FILE* outFile, int objectCount) {
            // keep current position
            long currentPos = ftell(outFile);
            // go to begining of file
            fseek(outFile, 0, SEEK_SET);
            fwrite(&objectCount, sizeof(objectCount), 1, outFile);
            // return to the original position
            fseek(outFile, currentPos, SEEK_SET);
            return DBERR_OK;
        }
    }
}