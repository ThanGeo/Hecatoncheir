#ifndef TWO_LAYER_CONTROLLER_H
#define TWO_LAYER_CONTROLLER_H

#include "def.h"
#include "relation.h"
#include "partitioning.h"
#include "filter.h"
#include "SpatialLib.h"

namespace two_layer
{   

    /**
     * Sets the (xMin,yMin),(xMax,yMax) values based on the datasets R,S min/max
     * giving the total boundaries that enclose both datasets
    */
    void getDatasetGlobalBounds(double &xMin, double &yMin, double &xMax, double &yMax);

    /**
     * load datasets R and S. Must be binary files (no csv support yet). 
     */
    void loadDatasets(std::string &Rpath, std::string &Spath);

    /**
     * @brief adds a single MBR to the input dataset(relation)
     * 
     * @param left  if true add object to R (left relation), else add to S 
     * @param recID 
     * @param xMin 
     * @param yMin 
     * @param xMax 
     * @param yMax 
     */
    void addObjectToDataset(bool left, uint recID, double xMin, double yMin, double xMax, double yMax);

    /**
     * requires datasets to be loaded to work. Calculates global bounds based on the dataset bounds.
    */
    void initTwoLayer(uint partitionsPerDimension, spatial_lib::MBRFilterTypeE mbrFilterType, spatial_lib::QueryTypeE queryType);

    /**
     * requires datasets to be loaded to work. Sets hardcoded global bounds.
    */
    void initTwoLayer(uint partitionsPerDimension, double xMin, double yMin, double xMax, double yMax);


    /**
     * Sets the intermediate filter type to forward each pair to.
     * the filter type is stored in the global g_iFilterType. 
     */
    void setNextStage(spatial_lib::IntermediateFilterTypeE iFilterType);

    /**
     * evaluate the two layer MBR filter on the two loaded datasets R and S. Returns the result count.
     * both loadDatasets() and initTwolayer(...) must have been called first successfully.
     */
    unsigned long long evaluateTwoLayer();

}


#endif