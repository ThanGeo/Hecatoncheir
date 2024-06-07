#include "intervalization.h"


namespace rasterizerlib{
    static int checkNeighborsInInterval(uint32_t &current_id, uint32_t &x, uint32_t &y, polygon2d &polygon, 
                                std::vector<uint32_t> &fullIntervals, std::vector<uint32_t> &partialCells, 
                                uint32_t cellsPerDim){

        if(fullIntervals.size() == 0){
            return UNCERTAIN;
        }
        uint32_t d;
        //4 neighbors
        for(int i=0; i<x_offset.size(); i++){		
            //if neighbor is out of bounds, ignore
            if(x+x_offset.at(i) < polygon.rasterData.minCellX || x+x_offset.at(i) > polygon.rasterData.maxCellX || y+y_offset.at(i) < polygon.rasterData.minCellY || y+y_offset.at(i) > polygon.rasterData.maxCellY){
                continue;
            }

            //get hilbert cell id
            d = xy2d(cellsPerDim, x+x_offset.at(i), y+y_offset.at(i));
            //if it has higher hilbert order, ignore
            if(d >= current_id){
                continue;
            }
            // if its a partial cell, ignore
            if(binarySearchInVector(partialCells, d)){
                //partial
                continue;
            }
            //check if it is full
            if(binarySearchInIntervalVector(fullIntervals, d)){
                //full
                return FULL;
            }else{
                //else its empty
                return EMPTY;
            }
        }
        return UNCERTAIN;
    }

    static spatial_lib::AprilDataT computeAllAndFullIntervals(polygon2d &polygon, uint32_t cellsPerDim, std::vector<uint32_t> &partialCells){
        int res;
        bool pip_res;
        uint32_t x,y, current_id;
        std::vector<uint32_t> fullIntervals;
        std::vector<uint32_t> allIntervals;
        clock_t timer;

        // remember to delete after use
        spatial_lib::AprilDataT aprilData;

        //set first partial cell
        auto current_partial_cell = partialCells.begin();
        uint32_t allStart = *current_partial_cell;
        //set first interval start and starting uncertain cell (the next cell after the first partial interval)
        while(*current_partial_cell == *(current_partial_cell+1) - 1){
            current_partial_cell++;
        }
        current_id = *(current_partial_cell) + 1;
        d2xy(cellsPerDim, current_id, x, y);
        current_partial_cell++;

        while(current_partial_cell < partialCells.end()){		
            
            //check neighboring cells
            res = checkNeighborsInInterval(current_id, x, y, polygon, fullIntervals, partialCells, cellsPerDim);

            if(res == FULL){
                //no need for pip test, it is full
                //this full interval ends at the next partial cell
                fullIntervals.emplace_back(current_id);
                fullIntervals.emplace_back(*current_partial_cell);
            }else if(res == EMPTY){
                //current cell is empty
                //save this all interval
                allIntervals.emplace_back(allStart);
                allIntervals.emplace_back(current_id);	
                //keep next interval's start
                allStart = *current_partial_cell;		
            }else{
                //uncertain, must perform PiP test
                bg_point_xy p(x, y);
                pip_res = boost::geometry::within(p, polygon.bgPolygon);
                if(pip_res){
                    //current cell is full
                    //this full interval ends at the next partial cell
                    fullIntervals.emplace_back(current_id);
                    fullIntervals.emplace_back(*current_partial_cell);
                }else{
                    //current cell is empty
                    //save this all interval
                    allIntervals.emplace_back(allStart);
                    allIntervals.emplace_back(current_id);	
                    //keep next interval's start
                    allStart = *current_partial_cell;
                }
            }

            //get next partial and uncertain
            while(*current_partial_cell == *(current_partial_cell+1) - 1){
                current_partial_cell++;
            }
            current_id = *(current_partial_cell) + 1;
            d2xy(cellsPerDim, current_id, x, y);
            current_partial_cell++;
        }

        // save last interval
        allIntervals.emplace_back(allStart);
        allIntervals.emplace_back(*(current_partial_cell-1) + 1);

        // store into the object
        aprilData.intervalsALL = allIntervals;
        aprilData.numIntervalsALL = allIntervals.size() / 2;
        aprilData.intervalsFULL = fullIntervals;
        aprilData.numIntervalsFULL = fullIntervals.size() / 2;
        return aprilData;
    }

    static spatial_lib::AprilDataT intervalize(polygon2d &polygon, uint32_t cellsPerDim){

        uint32_t x,y;
        clock_t timer;

        //first of all map the polygon's coordinates to this section's hilbert space
        mapPolygonToHilbert(polygon, cellsPerDim);

        // compute partial cells
        uint32_t **M = calculatePartialAndUncertain(polygon, cellsPerDim);
        std::vector<uint32_t> partialCells;
        partialCells = getPartialCellsFromMatrix(polygon, M);
        //sort the cells by cell uint32_t 
        sort(partialCells.begin(), partialCells.end());
        // delete the matrix memory, not needed anymore
        for(size_t i = 0; i < polygon.rasterData.bufferWidth; i++){
            delete M[i];
        }
        delete M;

        //compute all/full intervals
        return computeAllAndFullIntervals(polygon, cellsPerDim, partialCells);
    }

    spatial_lib::AprilDataT intervalizationBegin(polygon2d &polygon) {
        // safety checks
        if (g_config.celEnumType != CE_HILBERT) {
            log_err("can't intervalize on non-hilbert grids");
        }
        
        // proceed to intervalization
        return intervalize(polygon, g_config.cellsPerDim);
    }

    /**
     * @brief For boost geometry polygon
     * 
     */
    spatial_lib::AprilDataT intervalizationBGPolygon(spatial_lib::bg_polygon &bg_polygon) {
        uint32_t x,y;
        clock_t timer;

        // printConfig();

        // set local object
        polygon2d polygon;
        polygon.bgPolygon = bg_polygon;
        polygon.mbr.minPoint.x = std::numeric_limits<int>::max();
		polygon.mbr.minPoint.y = std::numeric_limits<int>::max();
		polygon.mbr.maxPoint.x = -std::numeric_limits<int>::max();
		polygon.mbr.maxPoint.y = -std::numeric_limits<int>::max();

        // printf("rasterizer: before map\n");
        // for (auto &it : bg_polygon.outer()) {
        //     printf("(%f,%f),", it.x(), it.y());
        // }
        // printf("\n");

        //first of all map the polygon's coordinates to this section's hilbert space
        mapBoostPolygonToHilbert(polygon, g_config.cellsPerDim);
        
        // printf("rasterizer: after map\n");
        // for (auto &it : polygon.bgPolygon.outer()) {
        //     printf("(%f,%f),", it.x(), it.y());
        // }
        // printf("\n");


        // printf("rasterizer: mapped\n");

        // compute partial cells
        uint32_t **M = calculatePartialAndUncertainBGPolygon(polygon, g_config.cellsPerDim);
        // printf("rasterizer: calculated partial and uncertain\n");
        std::vector<uint32_t> partialCells;
        partialCells = getPartialCellsFromMatrix(polygon, M);
        // printf("rasterizer: got partial cells\n");
        //sort the cells by cell uint32_t 
        sort(partialCells.begin(), partialCells.end());
        // delete the matrix memory, not needed anymore
        for(size_t i = 0; i < polygon.rasterData.bufferWidth; i++){
            delete M[i];
        }
        delete M;

        // printf("rasterizer: calculated partial\n");

        //compute all/full intervals
        return computeAllAndFullIntervals(polygon, g_config.cellsPerDim, partialCells);
    }
}