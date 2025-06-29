#include "UniformGrid/filter.h"

namespace uniform_grid
{
    namespace knn_filter
    {        
        DB_STATUS evaluate(hec::KNNQuery *knnQuery, std::unique_ptr<hec::QResultBase>& queryResult) {
            Shape qPoint;
            Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(knnQuery->getDatasetID());
            // create query shape
            DB_STATUS ret = shape_factory::createEmpty(DT_POINT, qPoint);
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_INVALID_GEOMETRY, "Couldn't create shape object from knn query.");
                return DBERR_INVALID_GEOMETRY;
            }
            ret = qPoint.setFromWKT(knnQuery->getWKT());
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_INVALID_GEOMETRY, "Couldn't set knn query shape from query WKT. WKT:", knnQuery->getWKT());
                return DBERR_INVALID_GEOMETRY;
            }
    
            std::vector<PartitionBase*>* partitions = dataset->index->getPartitions();
            for (auto &partition : *partitions) {
                if (partition == nullptr) {
                    continue;
                }
                /** CHECK PARTITION OPTIMIZATION */
                int iFine = partition->partitionID % g_config.partitioningMethod->getGlobalPPD(); 
                int jFine = partition->partitionID / g_config.partitioningMethod->getGlobalPPD(); 
                double xFineStart = g_config.datasetOptions.dataspaceMetadata.xMinGlobal + iFine * g_config.partitioningMethod->getPartPartionExtentX();
                double yFineStart = g_config.datasetOptions.dataspaceMetadata.yMinGlobal + jFine * g_config.partitioningMethod->getPartPartionExtentY();
                double xFineEnd = xFineStart + g_config.partitioningMethod->getDistPartionExtentX();
                double yFineEnd = yFineStart + g_config.partitioningMethod->getDistPartionExtentY();                
                // calculate query point distance to fine partition
                double distanceToPartition = qPoint.distanceToPartition(xFineStart, yFineStart, xFineEnd, yFineEnd);
                // if the distance to partition is larger than the current max in heap, skip this partition entirely
                if (!queryResult->checkDistance(distanceToPartition)) {
                    continue;
                }
    
                // get partition contents
                std::vector<Shape *>* contents = partition->getContents();
                if (contents == nullptr) {
                    continue;
                }
                // loop contents
                for (auto &obj: *contents) {
                    double distance = obj->distance(qPoint);
                    // add result (the heap handles insertions automatically)
                    queryResult->addResult(obj->recID, distance);
                }
            }
    
            return ret;
        }
    }
}
