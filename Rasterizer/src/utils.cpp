#include "utils.h"

namespace rasterizerlib
{
    std::vector<int> x_offset = { 1,1,1, 0,0,-1,-1,-1};
    std::vector<int> y_offset = {-1,0,1,-1,1,-1, 0, 1};

    // rotate/flip a quadrant appropriately
    inline void rot(uint32_t n, uint32_t &x, uint32_t &y, uint32_t rx, uint32_t ry) {
        if (ry == 0) {
            if (rx == 1) {
                x = n-1 - x;
                y = n-1 - y;
            }

            //Swap x and y
            uint32_t t  = x;
            x = y;
            y = t;
        }
    }

    // convert (x,y) to d
    uint32_t xy2d (uint32_t n, uint32_t x, uint32_t y) {
        uint32_t rx, ry, s;
        uint32_t d=0;
        for (s=n/2; s>0; s/=2) {
            rx = (x & s) > 0;
            ry = (y & s) > 0;
            d += s * s * ((3 * rx) ^ ry);
            rot(s, x, y, rx, ry);
        }
        return d;
    }

    // convert d to (x,y)
    void d2xy(uint32_t n, uint32_t d, uint32_t &x, uint32_t &y) {
        uint32_t rx, ry, s, t=d;
        x = y = 0;
        for (s=1; s<n; s*=2) {
            rx = 1 & (t/2);
            ry = 1 & (t ^ rx);
            rot(s, x, y, rx, ry);
            x += s * rx;
            y += s * ry;
            t /= 4;
        }
    }

    bool binarySearchInIntervalVector(std::vector<uint32_t> &vec, uint32_t x){
        int low = 0;
        int high = vec.size()-1;
        int mid;
        while(low <= high){
            mid = (low+high) / 2;
            if(vec.at(mid) > x){
                if(mid-1 > 0){
                    if(vec.at(mid-1) <= x){
                        return !((mid-1)%2);
                    }
                }
                high = mid-1;
            }else{
                if(mid+1 > vec.size()-1 || vec.at(mid+1) > x){
                    return !(mid%2);
                }
                low = mid+1;
            }
        }
        return false;
    }


    bool binarySearchInVector(std::vector<uint32_t> &vec, uint32_t &x){
        int low = 0;
        int high = vec.size()-1;
        int mid;
        while(low <= high){
            mid = (low+high) / 2;
            if(vec.at(mid) == x){
                return true;
            }
            if(vec.at(mid) < x){
                low = mid+1;
            }else{
                high = mid-1;
            }
        }
        return false;
    }

    static double mapSingleValueToHilbert(double val, double minval, double maxval, uint32_t &cellsPerDim){
        double newval =  ((double) (cellsPerDim-1) / (maxval - minval)) * (val - minval);
        if(newval < 0){
            newval = 0;
        }
        if(newval >= cellsPerDim){
            newval = cellsPerDim-1;
        }
        return newval;
    }

    static void mapXYToHilbert(double &x, double &y, uint32_t &cellsPerDim){
        x = ((double) (cellsPerDim-1) / (g_config.xMax - g_config.xMin)) * (x - g_config.xMin);
        y = ((double) (cellsPerDim-1) / (g_config.yMax - g_config.yMin)) * (y - g_config.yMin);

        if(x < 0){
            x = 0;
        }
        if(y < 0){
            y = 0;
        }

        if(x >= cellsPerDim){
            x = cellsPerDim-1;
        }
        if(y >= cellsPerDim){
            y = cellsPerDim-1;
        }
    }

    void mapPolygonToHilbert(polygon2d &polygon, uint32_t cellsPerDim){
        //first, map the polygon's coordinates to this section's hilbert space
        for(auto &p: polygon.vertices){
            mapXYToHilbert(p.x, p.y, cellsPerDim);
            polygon.bgPolygon.outer().emplace_back(p.x, p.y);
        }
        boost::geometry::correct(polygon.bgPolygon);	
        //map the polygon's mbr
        polygon.rasterData.minCellX = mapSingleValueToHilbert(polygon.mbr.minPoint.x, g_config.xMin, g_config.xMax, cellsPerDim);
        polygon.rasterData.minCellX > 0 ? polygon.rasterData.minCellX -= 1 : polygon.rasterData.minCellX = 0;
        polygon.rasterData.minCellY = mapSingleValueToHilbert(polygon.mbr.minPoint.y, g_config.yMin, g_config.yMax, cellsPerDim);
        polygon.rasterData.minCellY > 0 ? polygon.rasterData.minCellY -= 1 : polygon.rasterData.minCellY = 0;
        polygon.rasterData.maxCellX = mapSingleValueToHilbert(polygon.mbr.maxPoint.x, g_config.xMin, g_config.xMax, cellsPerDim);
        polygon.rasterData.maxCellX < cellsPerDim - 1 ? polygon.rasterData.maxCellX += 1 : polygon.rasterData.maxCellX = cellsPerDim - 1;
        polygon.rasterData.maxCellY = mapSingleValueToHilbert(polygon.mbr.maxPoint.y, g_config.yMin, g_config.yMax, cellsPerDim);
        polygon.rasterData.maxCellY < cellsPerDim - 1 ? polygon.rasterData.maxCellY += 1 : polygon.rasterData.maxCellY = cellsPerDim - 1;

        //set dimensions for buffers 
        polygon.rasterData.bufferWidth = polygon.rasterData.maxCellX - polygon.rasterData.minCellX + 1;
        polygon.rasterData.bufferHeight = polygon.rasterData.maxCellY - polygon.rasterData.minCellY + 1;
    }

    void mapBoostPolygonToHilbert(polygon2d &polygon, uint32_t cellsPerDim){
        double x,y;
        //first, map the polygon's coordinates to this section's hilbert space
        spatial_lib::bg_polygon bg_polygon;
        for(auto &p: polygon.bgPolygon.outer()){
            // get
            x = p.x();
            y = p.y();
            // set MBR
            polygon.mbr.minPoint.x = std::min(polygon.mbr.minPoint.x, x);
            polygon.mbr.minPoint.y = std::min(polygon.mbr.minPoint.y, y);
            polygon.mbr.maxPoint.x = std::max(polygon.mbr.maxPoint.x, x);
            polygon.mbr.maxPoint.y = std::max(polygon.mbr.maxPoint.y, y);
            // map
            mapXYToHilbert(x, y, cellsPerDim);
            // set
            bg_polygon.outer().emplace_back(x,y);
        }
        boost::geometry::correct(polygon.bgPolygon);	
        // replace in polygon
        polygon.bgPolygon = bg_polygon;

        //map the polygon's mbr
        polygon.rasterData.minCellX = mapSingleValueToHilbert(polygon.mbr.minPoint.x, g_config.xMin, g_config.xMax, cellsPerDim);
        polygon.rasterData.minCellX > 0 ? polygon.rasterData.minCellX -= 1 : polygon.rasterData.minCellX = 0;
        polygon.rasterData.minCellY = mapSingleValueToHilbert(polygon.mbr.minPoint.y, g_config.yMin, g_config.yMax, cellsPerDim);
        polygon.rasterData.minCellY > 0 ? polygon.rasterData.minCellY -= 1 : polygon.rasterData.minCellY = 0;
        polygon.rasterData.maxCellX = mapSingleValueToHilbert(polygon.mbr.maxPoint.x, g_config.xMin, g_config.xMax, cellsPerDim);
        polygon.rasterData.maxCellX < cellsPerDim - 1 ? polygon.rasterData.maxCellX += 1 : polygon.rasterData.maxCellX = cellsPerDim - 1;
        polygon.rasterData.maxCellY = mapSingleValueToHilbert(polygon.mbr.maxPoint.y, g_config.yMin, g_config.yMax, cellsPerDim);
        polygon.rasterData.maxCellY < cellsPerDim - 1 ? polygon.rasterData.maxCellY += 1 : polygon.rasterData.maxCellY = cellsPerDim - 1;

        //set dimensions for buffers 
        polygon.rasterData.bufferWidth = polygon.rasterData.maxCellX - polygon.rasterData.minCellX + 1;
        polygon.rasterData.bufferHeight = polygon.rasterData.maxCellY - polygon.rasterData.minCellY + 1;


    }

    std::vector<uint32_t> getPartialCellsFromMatrix(polygon2d &polygon, uint32_t **M){
        std::vector<uint32_t> partialCells;
        for(int i=0; i<polygon.rasterData.bufferWidth; i++){
            for(int j=0; j<polygon.rasterData.bufferHeight; j++){
                if(M[i][j] == PARTIAL){			
                    //store into preallocated array
                    partialCells.emplace_back(xy2d(g_config.cellsPerDim, i + polygon.rasterData.minCellX, j + polygon.rasterData.minCellY));
                }
            }
        }
        return partialCells;
    }

    bool checkIfPolygonIsInsideDataspace(polygon2d &polygon) {
        return !(polygon.mbr.minPoint.x < g_config.xMin || polygon.mbr.minPoint.y < g_config.yMin || polygon.mbr.maxPoint.x > g_config.xMax || polygon.mbr.maxPoint.y > g_config.yMax);
    }
}