#include "def.h"


namespace rasterizerlib
{
    configT g_config;
    
    int setDataspace(double xMin, double yMin, double xMax, double yMax){
        if (xMin >= xMax) {
            log_err("xMax can't be smaller than xMmin");
            return 0;
        }
        if (yMin >= yMax) {
            log_err("yMax can't be smaller than yMmin");
            return 0;
        }
        
        g_config.xMin = xMin;
        g_config.yMin = yMin;
        g_config.xMax = xMax;
        g_config.yMax = yMax;

        if (xMax - yMin != yMax - yMin) {
            g_config.areCellsSquare = false;
        } else {
            g_config.areCellsSquare = true;
        }

        return 1;
    }

    void printConfig() {
        printf("Dataspace MBR: (%f,%f) to (%f,%f)\n", g_config.xMin, g_config.yMin, g_config.xMax, g_config.yMax);
        printf("N: %u, cells per dim: %u\n", g_config.orderN, g_config.cellsPerDim);
        switch (g_config.celEnumType) {
            case CE_HILBERT:
                printf("CELL ENUM TYPE: HILBERT \n");
                break;
        }
    }

    int init(double xMin, double yMin, double xMax, double yMax) {

        // set data space bounds
        int ret = setDataspace(xMin, yMin, xMax, yMax);

        // set init flag to true
        g_config.lib_init = true;
        return ret;
    }

    void log_err(std::string errorText) {
        std::string msg = "*** rasterizerlib fail: " + errorText + "\n";
        fprintf(stderr, "%s\n", msg.c_str());
    }

    polygon2d createPolygon(std::vector<spatial_lib::PointT> &vertices) {
        polygon2d polygon;
        if (vertices.size() < 3) {
            log_err("Polygon needs at least 3 points");
            return polygon;
        }
        
        polygon.vertices = vertices;
       
        polygon.mbr.minPoint.x = std::numeric_limits<int>::max();
		polygon.mbr.minPoint.y = std::numeric_limits<int>::max();
		polygon.mbr.maxPoint.x = -std::numeric_limits<int>::max();
		polygon.mbr.maxPoint.y = -std::numeric_limits<int>::max();

        for (auto &p : vertices) {
            polygon.bgPolygon.outer().push_back(bg_point_xy(p.x, p.y));

            polygon.mbr.minPoint.x = std::min(polygon.mbr.minPoint.x, p.x);
            polygon.mbr.minPoint.y = std::min(polygon.mbr.minPoint.y, p.y);
            polygon.mbr.maxPoint.x = std::max(polygon.mbr.maxPoint.x, p.x);
            polygon.mbr.maxPoint.y = std::max(polygon.mbr.maxPoint.y, p.y);
        }
        boost::geometry::correct(polygon.bgPolygon);

        return polygon;
    }

    polygon2d createPolygon(double* coords, int vertexCount) {
        polygon2d polygon;
        if (vertexCount < 3) {
            log_err("Polygon needs at least 3 points");
            return polygon;
        }
        
        polygon.vertices.reserve(vertexCount);
       
        polygon.mbr.minPoint.x = std::numeric_limits<int>::max();
		polygon.mbr.minPoint.y = std::numeric_limits<int>::max();
		polygon.mbr.maxPoint.x = -std::numeric_limits<int>::max();
		polygon.mbr.maxPoint.y = -std::numeric_limits<int>::max();

        for (int i=0; i<vertexCount; i+=2) {
            polygon.vertices.emplace_back(coords[i], coords[i+1]);
            polygon.bgPolygon.outer().push_back(bg_point_xy(coords[i], coords[i+1]));
            polygon.mbr.minPoint.x = std::min(polygon.mbr.minPoint.x, coords[i]);
            polygon.mbr.minPoint.y = std::min(polygon.mbr.minPoint.y, coords[i+1]);
            polygon.mbr.maxPoint.x = std::max(polygon.mbr.maxPoint.x, coords[i]);
            polygon.mbr.maxPoint.y = std::max(polygon.mbr.maxPoint.y, coords[i+1]);
        }

        boost::geometry::correct(polygon.bgPolygon);

        return polygon;
    }
}

