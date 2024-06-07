#include "rasterization.h"

#include <iostream>

namespace rasterizerlib
{

    static inline bool checkY(double &y1, double &OGy1, double &OGy2, uint32_t &startCellY, uint32_t &endCellY){
        if(OGy1 < OGy2){
            return (startCellY <= y1 && y1 <= endCellY + 1);
        }else{
            return (endCellY <= y1 && y1 <= startCellY + 1);
        }
    }

    uint32_t** calculatePartialAndUncertainBGPolygon(polygon2d &polygon, uint32_t &cellsPerDim){
        //create empty matrix
        uint32_t **M = new uint32_t*[polygon.rasterData.bufferWidth]();
        for(uint32_t i=0; i<polygon.rasterData.bufferWidth; i++){
            M[i] = new uint32_t[polygon.rasterData.bufferHeight]();		
            for(uint32_t j=0; j<polygon.rasterData.bufferHeight; j++){
                M[i][j] = UNCERTAIN;
            }
        }

        // local scope variables
        uint32_t startCellX, startCellY, endCellX, endCellY;;
        uint32_t currentCellX, currentCellY;
        double x1, y1, x2, y2;
        double ogy1, ogy2;
        int stepX, stepY;
        uint32_t verticalStartY, verticalEndY, horizontalY;
        double tMaxX, tMaxY;
        double tDeltaX, tDeltaY;
        double edgeLength;
        std::deque<bg_point_xy> output;
        double error_margin = 0.00001;

        //loop points
        for (auto it = polygon.bgPolygon.outer().begin(); it != polygon.bgPolygon.outer().end()-1; it++) {
            //set points x1 has the lowest x
            if(it->x() < (it+1)->x()){
                x1 = it->x();
                y1 = it->y();
                x2 = (it+1)->x();
                y2 = (it+1)->y();
            }else{
                x2 = it->x();
                y2 = it->y();
                x1 = (it+1)->x();
                y1 = (it+1)->y();
            }

            //keep original values of y1,y2
            ogy1 = y1;
            ogy2 = y2;	

            //set endpoint hilbert cells
            startCellX = (int)x1;
            startCellY = (int)y1;
            endCellX = (int)x2;
            endCellY = (int)y2;

            if(startCellX == endCellX && startCellY == endCellY){
                //label the cell in the partially covered matrix
                M[startCellX-polygon.rasterData.minCellX][startCellY-polygon.rasterData.minCellY] = PARTIAL;
            }else{
                //set step (based on direction)
                stepX = x2 > x1 ? 1 : -1;
                stepY = y2 > y1 ? 1 : -1;

                //define the polygon edge
                bg_linestring ls{{x1, y1},{x2, y2}};
                edgeLength = boost::geometry::length(ls);

                //define NEAREST VERTICAL grid line
                bg_linestring vertical{{startCellX+1, 0},{startCellX+1, cellsPerDim}};
                
                //define NEAREST HORIZONTAL grid line
                y1 < y2 ? horizontalY = int(y1) + 1 : horizontalY = int(y1);
                bg_linestring horizontal{{0, horizontalY},{cellsPerDim, horizontalY}};

                //get intersection points with the vertical and nearest lines
                boost::geometry::intersection(ls, vertical, output);
                bg_point_xy intersectionPointVertical = output[0];
                output.clear();
                boost::geometry::intersection(ls, horizontal, output);
                bg_point_xy intersectionPointHorizontal = output[0];
                output.clear();

                // //keep in mind: the line segment may not intersect a vertical or horizontal line!!!!!!!!
                if(boost::geometry::distance(intersectionPointVertical, ls) <= error_margin && boost::geometry::distance(intersectionPointVertical, vertical) <= error_margin){
                    bg_linestring tXMaxline{{x1,y1},intersectionPointVertical};
                    tMaxX = boost::geometry::length(tXMaxline);
                }else{
                    tMaxX = edgeLength;
                }
                if(boost::geometry::distance(intersectionPointHorizontal, ls) <= error_margin && boost::geometry::distance(intersectionPointHorizontal, horizontal) <= error_margin){
                    bg_linestring tYMaxline{{x1,y1},intersectionPointHorizontal};
                    tMaxY = boost::geometry::length(tYMaxline);
                }else{
                    tMaxY = edgeLength;
                }

                //deltas
                tDeltaX = edgeLength / (x2 - x1);
                tDeltaY = edgeLength / abs(y2 - y1);
                
                //loop (traverse ray)
                while(startCellX <= x1 && x1 < endCellX+1 && checkY(y1, ogy1, ogy2, startCellY, endCellY)){
                    M[(int)x1-polygon.rasterData.minCellX][(int)y1-polygon.rasterData.minCellY] = PARTIAL;
                    if(tMaxX < tMaxY){
                        x1 = x1 + stepX;
                        tMaxX = tMaxX + tDeltaX;
                    }else{
                        y1 = y1 + stepY;
                        tMaxY = tMaxY + tDeltaY;
                    }
                }
            }
        }
        return M;
    }

    uint32_t** calculatePartialAndUncertain(polygon2d &polygon, uint32_t &cellsPerDim){
        //create empty matrix
        uint32_t **M = new uint32_t*[polygon.rasterData.bufferWidth]();
        for(uint32_t i=0; i<polygon.rasterData.bufferWidth; i++){
            M[i] = new uint32_t[polygon.rasterData.bufferHeight]();		
            for(uint32_t j=0; j<polygon.rasterData.bufferHeight; j++){
                M[i][j] = UNCERTAIN;
            }
        }

        // local scope variables
        uint32_t startCellX, startCellY, endCellX, endCellY;;
        uint32_t currentCellX, currentCellY;
        double x1, y1, x2, y2;
        double ogy1, ogy2;
        int stepX, stepY;
        uint32_t verticalStartY, verticalEndY, horizontalY;
        double tMaxX, tMaxY;
        double tDeltaX, tDeltaY;
        double edgeLength;
        std::deque<bg_point_xy> output;
        double error_margin = 0.00001;

        //loop points
        for (auto it = polygon.vertices.begin(); it != polygon.vertices.end()-1; it++) {
            //set points x1 has the lowest x
            if(it->x < (it+1)->x){
                x1 = it->x;
                y1 = it->y;
                x2 = (it+1)->x;
                y2 = (it+1)->y;
            }else{
                x2 = it->x;
                y2 = it->y;
                x1 = (it+1)->x;
                y1 = (it+1)->y;
            }

            //keep original values of y1,y2
            ogy1 = y1;
            ogy2 = y2;	

            //set endpoint hilbert cells
            startCellX = (int)x1;
            startCellY = (int)y1;
            endCellX = (int)x2;
            endCellY = (int)y2;

            if(startCellX == endCellX && startCellY == endCellY){
                //label the cell in the partially covered matrix
                M[startCellX-polygon.rasterData.minCellX][startCellY-polygon.rasterData.minCellY] = PARTIAL;
            }else{
                //set step (based on direction)
                stepX = x2 > x1 ? 1 : -1;
                stepY = y2 > y1 ? 1 : -1;

                //define the polygon edge
                bg_linestring ls{{x1, y1},{x2, y2}};
                edgeLength = boost::geometry::length(ls);

                //define NEAREST VERTICAL grid line
                bg_linestring vertical{{startCellX+1, 0},{startCellX+1, cellsPerDim}};
                
                //define NEAREST HORIZONTAL grid line
                y1 < y2 ? horizontalY = int(y1) + 1 : horizontalY = int(y1);
                bg_linestring horizontal{{0, horizontalY},{cellsPerDim, horizontalY}};

                //get intersection points with the vertical and nearest lines
                boost::geometry::intersection(ls, vertical, output);
                bg_point_xy intersectionPointVertical = output[0];
                output.clear();
                boost::geometry::intersection(ls, horizontal, output);
                bg_point_xy intersectionPointHorizontal = output[0];
                output.clear();

                // //keep in mind: the line segment may not intersect a vertical or horizontal line!!!!!!!!
                if(boost::geometry::distance(intersectionPointVertical, ls) <= error_margin && boost::geometry::distance(intersectionPointVertical, vertical) <= error_margin){
                    bg_linestring tXMaxline{{x1,y1},intersectionPointVertical};
                    tMaxX = boost::geometry::length(tXMaxline);
                }else{
                    tMaxX = edgeLength;
                }
                if(boost::geometry::distance(intersectionPointHorizontal, ls) <= error_margin && boost::geometry::distance(intersectionPointHorizontal, horizontal) <= error_margin){
                    bg_linestring tYMaxline{{x1,y1},intersectionPointHorizontal};
                    tMaxY = boost::geometry::length(tYMaxline);
                }else{
                    tMaxY = edgeLength;
                }

                //deltas
                tDeltaX = edgeLength / (x2 - x1);
                tDeltaY = edgeLength / abs(y2 - y1);
                
                //loop (traverse ray)
                while(startCellX <= x1 && x1 < endCellX+1 && checkY(y1, ogy1, ogy2, startCellY, endCellY)){
                    M[(int)x1-polygon.rasterData.minCellX][(int)y1-polygon.rasterData.minCellY] = PARTIAL;
                    if(tMaxX < tMaxY){
                        x1 = x1 + stepX;
                        tMaxX = tMaxX + tDeltaX;
                    }else{
                        y1 = y1 + stepY;
                        tMaxY = tMaxY + tDeltaY;
                    }
                }
            }
        }
        return M;
    }

    static int checkNeighborsInCellList(uint32_t &current_id, uint32_t &x, uint32_t &y, polygon2d &polygon, 
                                std::vector<uint32_t> &fullCells, std::vector<uint32_t> &partialCells, 
                                uint32_t cellsPerDim){

        if(fullCells.size() == 0){
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
            if(binarySearchInVector(fullCells, d)){
                //full
                return FULL;
            }else{
                //else its empty
                return EMPTY;
            }
        }
        return UNCERTAIN;
    }

    static spatial_lib::AprilDataT* computeFullCells(polygon2d &polygon, uint32_t cellsPerDim, std::vector<uint32_t> &partialCells){
        int res;
        bool pip_res;
        uint32_t x,y, current_id;
        std::vector<uint32_t> fullCells;
        clock_t timer;

        spatial_lib::AprilDataT* aprilData = new spatial_lib::AprilDataT;

        //set first partial cell
        auto current_partial_cell = partialCells.begin();
        //set first interval start and starting uncertain cell (the next cell after the first partial interval)
        while(*current_partial_cell == *(current_partial_cell+1) - 1){
            current_partial_cell++;
        }
        current_id = *(current_partial_cell) + 1;
        d2xy(cellsPerDim, current_id, x, y);
        current_partial_cell++;

        while(current_partial_cell < partialCells.end()){		
            
            //check neighboring cells
            res = checkNeighborsInCellList(current_id, x, y, polygon, fullCells, partialCells, cellsPerDim);

            if(res == FULL){
                //no need for pip test, it is full
                //this full interval ends at the next partial cell
                // save all full cells from current_id up to current_partial_cell exclusive
                for (uint32_t i = current_id; i < *current_partial_cell; i++) {
                    fullCells.emplace_back(i);
                }
            }else if(res == EMPTY){
                //current cell is empty	
            }else{
                //uncertain, must perform PiP test
                bg_point_xy p(x, y);
                pip_res = boost::geometry::within(p, polygon.bgPolygon);
                if(pip_res){
                    // current cell is full
                    // this full interval ends at the next partial cell
                    // save all full cells from current_id up to current_partial_cell exclusive
                    for (uint32_t i = current_id; i < *current_partial_cell; i++) {
                        fullCells.emplace_back(i);
                    }
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

        // store into the object
        aprilData->intervalsALL = partialCells;
        aprilData->numIntervalsALL = partialCells.size();
        aprilData->intervalsFULL = fullCells;
        aprilData->numIntervalsFULL = fullCells.size();
        return aprilData;
    }

    static void rasterizeEfficiently(polygon2d &polygon, uint32_t cellsPerDim){

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

        //compute full cells
        computeFullCells(polygon, cellsPerDim, partialCells);
    }

    void rasterizationBegin(polygon2d &polygon) {
        // safety checks
        if (!g_config.lib_init) {
            log_err("lib not initialized");
        }
        if (g_config.celEnumType != CE_HILBERT) {
            log_err("no support for rasterization on non-hilbert grids");
        }

        // proceed to rasterization
        rasterizeEfficiently(polygon, g_config.cellsPerDim);

    }

    static void rasterizeEfficientlyPartialOnly(polygon2d &polygon, uint32_t cellsPerDim){

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

        // store to object
        // polygon.rasterData.data.listA = partialCells;
        printf("Not implemented yet.");
        exit(-1);
    }

    void rasterizationPartialOnlyBegin(polygon2d &polygon) {
        // safety checks
        if (g_config.celEnumType != CE_HILBERT) {
            log_err("can't intervalize on non-hilbert grids");
        }
        // proceed to rasterization
        rasterizeEfficientlyPartialOnly(polygon, g_config.cellsPerDim);

    }
    
    





    

} 
