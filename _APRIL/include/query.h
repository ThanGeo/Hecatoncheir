#ifndef APRIL_CONTROLLER_H
#define APRIL_CONTROLLER_H

#include "SpatialLib.h"
#include "Rasterizer.h"
#include <fstream>

#include "utils.h"
#include "join.h"
#include "topology.h"

namespace APRIL
{
    extern spatial_lib::QueryT* g_query;

    /**
     * Sets up the APRIL intermediate filter.
     * TODO: if R and S have different april config, i might need to make a new method
    */
    void setupAPRILIntermediateFilter(spatial_lib::QueryT *query);
    
    namespace optimized
    {
        /**
         * @brief Entrypoint function for the find relation intermediate filter
         * 
         * @param idR 
         * @param idS 
         * @param relationCase 
         */
        void IntermediateFilterEntrypoint(uint idR, uint idS, int relationCase);
    }

    namespace standard
    {
        /**
         * @brief Standard APRIL intermediate filter that detects disjoint and intersection pairs,
         * but used for find relation queries. Non-disjoint pairs are refined using DE-9IM
         * 
         * 
         * @param idR 
         * @param idS 
         */
        void IntermediateFilterEntrypoint(uint idR, uint idS);
    }

}

#endif