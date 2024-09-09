/**
 * @file containers.h
@brief Contains the struct definitions.
 * 
 * The struct member functions are defined in 'containers.cpp'.
 */
#ifndef D_CONTAINERS_H
#define D_CONTAINERS_H

#include <unistd.h>
#include <vector>
#include <stdio.h>
#include <mpi.h>
#include <math.h>
#include <any>
#include <cstring>
#include <variant>

#include "def.h"
#include "utils.h"
#include "env/comm_def.h"

/**
 * @brief Holds all the APRIL related info for a single object
 * 
 * This struct contains two lists and their sizes, for APRIL's A-list and F-list.
 * The intervals' start,end are stored as consecutive numbers. 
 * Starting from the first position, every two consecutive numbers represent an interval's [start,end).
 * 
 */
struct AprilData {
    /** @brief A-list size */
    uint numIntervalsALL = 0;
    /** 
    @brief A-list contents
     * 
     * All of the object's intervals regardless of cell type (Partial and Full)
     */
    std::vector<uint32_t> intervalsALL;
    /** @brief F-list size */
    uint numIntervalsFULL = 0;
    /** 
    @brief F-list contents
     * 
     * Intervals comprised solely of Full cells i.e. cells covered completely by the original geometry.
     */
    std::vector<uint32_t> intervalsFULL;

    /**
    @brief Prints the interval A-list.
     */
    void printALLintervals();
    /**
    @brief Prints the interval A-list as cells instead of intervals.
     */
    void printALLcells(uint32_t n);
    /**
    @brief Prints the interval F-list.
     */
    void printFULLintervals();
};

/**
 * @brief Contains all relevant APRIL configuration parameters.
 * 
 * The objects parameters are set by the configuration file.
 */
struct AprilConfig {
    private:
        /**
        @brief The Hilbert curve order.
         * @warning Always in range [10,16].
         */
        int N = 16;
        /**
        @brief How many cells are in the Hilbert grid based on the Hilbert curve order.
         * Always automatically set when defining the order N.
         */
        int cellsPerDim = pow(2,16);
    public:
        /** @brief Whether compressed intervals are enabled (1) or disabled (0) 
         * @warning Only uncompressed version is supported currently.
        */
        bool compression = 0;
        /** @brief Number of partitions per dimension in the Hilbert space
         * @warning Only 1 partition is supported currently.
         */
        int partitions = 1;
        /** @brief The A-list intervals filepath on disk. */
        std::string ALL_intervals_path;
        /** @brief The F-list intervals filepath on disk. */
        std::string FULL_intervals_path;

    /** @brief Sets the Hilbert curve order N. */
    void setN(int N);
    /** @brief Returns the Hilbert curve order N. */
    int getN();
    /** @brief Returns the number of cells per dimension in the Hilbert grid. */
    int getCellsPerDim();
};

/**
 * @brief Struct for 2-dimension points with double coordinates x and y (lon, lat).
 */
struct Point {
    double x, y;
    Point(double xVal, double yVal) : x(xVal), y(yVal) {}
};

/**
 * @brief Struct for Minimum Bounding Rectangles (MBR).
 * 
 * It holds the MBR's bottom-left (pMin) and top-right (pMax) points.
 */
struct MBR {
    Point pMin;
    Point pMax;

    MBR() : pMin(Point(std::numeric_limits<int>::max(), std::numeric_limits<int>::max())), pMax(Point(-std::numeric_limits<int>::max(), -std::numeric_limits<int>::max())) {}
};

/** @brief Simple Geometry object used only in the data partitioning/broadcasting.
 * @todo Redundant. Maybe use the Shape struct.
 */
struct Geometry {
    size_t recID;
    int partitionCount;
    std::vector<int> partitions;    // tuples of <partition ID, twolayer class>
    int vertexCount;
    std::vector<double> coords;

    Geometry(size_t recID, int vertexCount, std::vector<double> &coords);
    Geometry(size_t recID, std::vector<int> &partitions, int vertexCount, std::vector<double> &coords);
    void setPartitions(std::vector<int> &ids);
    void setPartitions(std::vector<int> &ids, std::vector<TwoLayerClass> &classes);
};

/**
 * @brief A batch containing multiple objects for the batch partitioning/broadcasting.
 */
struct GeometryBatch {
    // serializable
    size_t objectCount = 0;
    std::vector<Geometry> geometries;
    // unserializable/unclearable (todo: make const?)
    int destRank = -1;   // destination node rank
    size_t maxObjectCount; 
    MPI_Comm* comm; // communicator that the batch will be send through
    int tag = -1;        // MPI tag = indicates spatial data type

    bool isValid();
    void addGeometryToBatch(Geometry &geometry);
    void setDestNodeRank(int destRank);

    // calculate the size needed for the serialization buffer
    int calculateBufferSize();

    void clear();

    /**
    @brief serializes the geometry batch into the buffer. This method also allocates the buffer's memory.
     * Caller is responsible to free.
     */
    int serialize(char **buffer);

    /**
    @brief fills the struct with data from the input serialized buffer
     * The caller must free the buffer memory
     */
    void deserialize(const char *buffer, int bufferSize);

};

/**
 * @brief Wrapper class for the Geometry objects.
 * 
 * Contains only the definitions of the functions so that the derived geometry classes can inherit and overload them.
 * @warning Never should anyone define an object of this type and try to utilize it (undefined behaviour)
 */
template<typename GeometryType>
struct GeometryWrapper {
public:
    /**
    @brief Template field geometry will be set by the derived classes to the appropriate Boost Geometry type 
     * that represents the the derived class.
     */
    GeometryType geometry;

    GeometryWrapper() {}
    explicit GeometryWrapper(const GeometryType& geom) : geometry(geom) {}

    /** @brief Sets the boost geometry object. */
    void setGeometry(const GeometryType& geom) {
        geometry = geom;
    }

    void reset() {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: reset");
    }

    void addPoint(const double x, const double y) {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: addPoint");
    }

    bool pipTest(const bg_point_xy &point) const {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: pipTest");
        return false;
    }

    void printGeometry() {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: printGeometry");
    }

    void correctGeometry() {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: correctGeometry");
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: modifyBoostPointByIndex");
    }

    const std::vector<bg_point_xy>* getReferenceToPoints() const {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: getReferenceToPoints.");
        return nullptr;
    }

    int getVertexCount() const {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: getVertexCount.");
        return 0;
    }

    template<typename OtherGeometryType>
    std::string createMaskCode(const GeometryWrapper<OtherGeometryType> &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "createMaskCode unsupported for the invoked shapes.");
        return "";
    }

    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return boost::geometry::intersects(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return boost::geometry::disjoint(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool inside(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "intersects predicate unsupported for the invoked shapes.");
        return false;
    }

    template<typename OtherBoostGeometryObj>
    bool coveredBy(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "covered bt predicate unsupported for the invoked shapes.");
        return false;
    }

    template<typename OtherBoostGeometryObj>
    bool contains(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "contains predicate unsupported for the invoked shapes.");
        return false;
    }

    template<typename OtherBoostGeometryObj>
    bool covers(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "covers predicate unsupported for the invoked shapes.");
        return false;
    }

    template<typename OtherBoostGeometryObj>
    bool meets(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "meets predicate unsupported for the invoked shapes.");
        return false;
    }

    template<typename OtherBoostGeometryObj>
    bool equals(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "meets predicate unsupported for the invoked shapes.");
        return false;
    }
};

/**
@brief Point geometry derived struct. 
 */
template<>
struct GeometryWrapper<bg_point_xy> {
public:
    bg_point_xy geometry;
    GeometryWrapper(){}
    GeometryWrapper(const bg_point_xy &geom) : geometry(geom) {}

    /** @brief Replaces the geometry (point) with a new one. */
    void addPoint(const double x, const double y) {
        // For points, simply replace the existing point
        geometry = bg_point_xy(x, y);
    }

    void correctGeometry() {
        boost::geometry::correct(geometry);
    }

    void reset() {
        boost::geometry::clear(geometry);
    }

    bool pipTest(const bg_point_xy &point) const {
        return false;
    }

    void printGeometry() {
        printf("(%f,%f)\n", geometry.x(), geometry.y());
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        if (index != 0) {
            logger::log_error(DBERR_INVALID_OPERATION, "Ignoring non-zero index for point shape, modifying the point anyway.");
        }
        geometry = bg_point_xy(x, y);
    }


    const std::vector<bg_point_xy>* getReferenceToPoints() const {
        logger::log_error(DBERR_INVALID_OPERATION, "Can't return reference to points on Point shape.");
        return nullptr;
    }

    int getVertexCount() const {
        return 1;
    }

    /**
     * queries
     */

    std::string createMaskCode(const GeometryWrapper<bg_polygon>& other) const;
    std::string createMaskCode(const GeometryWrapper<bg_point_xy>& other) const {return "";}
    std::string createMaskCode(const GeometryWrapper<bg_linestring>& other) const {return "";}
    std::string createMaskCode(const GeometryWrapper<bg_rectangle>& other) const {return "";}

    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return boost::geometry::intersects(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return boost::geometry::disjoint(geometry, other.geometry);
    }

    bool inside(const GeometryWrapper<bg_polygon>& other) const;
    bool inside(const GeometryWrapper<bg_linestring>& other) const;
    bool inside(const GeometryWrapper<bg_rectangle>& other) const;
    bool inside(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::within(geometry, other.geometry);
    }

    bool contains(const GeometryWrapper<bg_point_xy>& other) const {return false;}
    bool contains(const GeometryWrapper<bg_polygon>& other) const {return false;}
    bool contains(const GeometryWrapper<bg_linestring>& other) const {return false;}
    bool contains(const GeometryWrapper<bg_rectangle>& other) const {return false;}

    bool meets(const GeometryWrapper<bg_polygon>& other) const;
    bool meets(const GeometryWrapper<bg_linestring>& other) const;
    bool meets(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool meets(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }

    bool equals(const GeometryWrapper<bg_linestring>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_polygon>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::equals(geometry, other.geometry);
    }
};


// rectangle
template<>
struct GeometryWrapper<bg_rectangle> {
public:
    bg_rectangle geometry;
    std::vector<bg_point_xy> vertices;
    GeometryWrapper(){}
    GeometryWrapper(const bg_rectangle &geom) : geometry(geom) {}

    void addPoint(const double x, const double y) {
        bg_point_xy point(x, y);
        if (geometry.min_corner().x() == 0 && geometry.min_corner().y() == 0) {
            // no points yet, add min point
            geometry.min_corner() = point;
        } else if (geometry.max_corner().x() == 0 && geometry.max_corner().y() == 0) {
            // one point exists, add max point
            geometry.max_corner() = point;
        } else {
            // both points already exist, error
            logger::log_error(DBERR_INVALID_OPERATION, "Cannot add more than two points to a rectangle");
        }
        vertices.emplace_back(point);
    }

    void correctGeometry() {
        boost::geometry::correct(geometry);
    }

    bool pipTest(const bg_point_xy &point) const {
        return boost::geometry::within(point, geometry);
    }

    void reset() {
        boost::geometry::clear(geometry);
        vertices.clear();
    }

    void printGeometry() {
        printf("(%f,%f),(%f,%f),(%f,%f),(%f,%f)\n", geometry.min_corner().x(), geometry.min_corner().y(),
        geometry.max_corner().x(), geometry.min_corner().y(),
        geometry.max_corner().x(), geometry.max_corner().y(),
        geometry.min_corner().x(), geometry.max_corner().y());
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        if (index == 0) {
            geometry.min_corner() = bg_point_xy(x, y);
        } else if (index == 1) {
            geometry.max_corner() = bg_point_xy(x, y);
        } else {
            logger::log_error(DBERR_OUT_OF_BOUNDS, "Rectangle point index out of bounds for modifyBoostPointByIndex:", index);
        }
        vertices[index] = bg_point_xy(x, y);
    }

    const std::vector<bg_point_xy>* getReferenceToPoints() const {
        return &vertices;
    }

    int getVertexCount() const {
        return 2;
    }

    /**
     * queries
     */
    
    template<typename OtherGeometryType>
    std::string createMaskCode(const GeometryWrapper<OtherGeometryType> &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "createMaskCode unsupported for the invoked shapes.");
        return "";
    }

    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return boost::geometry::intersects(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return boost::geometry::disjoint(geometry, other.geometry);
    }

    bool inside(const GeometryWrapper<bg_point_xy> &other) const {return false;}
    bool inside(const GeometryWrapper<bg_linestring> &other) const {return false;}
    bool inside(const GeometryWrapper<bg_polygon> &other) const {return false;}
    bool inside(const GeometryWrapper<bg_rectangle> &other) const {
        return boost::geometry::within(geometry, other.geometry);
    }

    bool contains(const GeometryWrapper<bg_linestring> &other) const {return false;}
    bool contains(const GeometryWrapper<bg_polygon> &other) const {return false;}
    bool contains(const GeometryWrapper<bg_rectangle> &other) const {
        return boost::geometry::within(other.geometry, geometry);
    }
    bool contains(const GeometryWrapper<bg_point_xy> &other) const {
        return boost::geometry::within(other.geometry, geometry);
    }

    bool meets(const GeometryWrapper<bg_point_xy> &other) const {return false;}
    bool meets(const GeometryWrapper<bg_linestring> &other) const {return false;}
    bool meets(const GeometryWrapper<bg_polygon> &other) const {return false;}
    bool meets(const GeometryWrapper<bg_rectangle> &other) const {return false;}

    bool equals(const GeometryWrapper<bg_point_xy>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_linestring>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_polygon>& other) const;
    bool equals(const GeometryWrapper<bg_rectangle>& other) const {
        return boost::geometry::equals(geometry, other.geometry);
    }
};

// linestring
template<>
struct GeometryWrapper<bg_linestring> {
public:
    bg_linestring geometry;
    GeometryWrapper(){}
    GeometryWrapper(const bg_linestring &geom) : geometry(geom) {}

    void addPoint(const double x, const double y) {
        bg_point_xy point(x, y);
        geometry.push_back(point);
    }

    void correctGeometry() {
        boost::geometry::correct(geometry);
    }

    bool pipTest(const bg_point_xy &point) const {
        return false;
    }

    void printGeometry() {
        for(auto &it: geometry) {
            printf("(%f,%f),", it.x(), it.y());
        }
        printf("\n");
    }

    void reset() {
        boost::geometry::clear(geometry);
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        if (index < geometry.size()) {
            geometry[index] = bg_point_xy(x, y);
        } else {
            logger::log_error(DBERR_OUT_OF_BOUNDS, "Linestring point index out of bounds for modifyBoostPointByIndex:", index);
        }
    }

    const std::vector<bg_point_xy>* getReferenceToPoints() const {
        return &geometry;
    }

    int getVertexCount() const {
        return geometry.size();
    }

    // topology
    // declaration
    std::string createMaskCode(const GeometryWrapper<bg_polygon>& other) const;
    std::string createMaskCode(const GeometryWrapper<bg_point_xy>& other) const {return "";}
    std::string createMaskCode(const GeometryWrapper<bg_rectangle>& other) const {return "";}
    // definitions
    std::string createMaskCode(const GeometryWrapper<bg_linestring>& other) const {
        boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
        return matrix.str();
    }
    
    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return boost::geometry::intersects(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return boost::geometry::disjoint(geometry, other.geometry);
    }

    bool inside(const GeometryWrapper<bg_point_xy> &other) const {return false;}
    bool inside(const GeometryWrapper<bg_polygon> &other) const;
    bool inside(const GeometryWrapper<bg_rectangle> &other) const {return false;}
    bool inside(const GeometryWrapper<bg_linestring> &other) const {
        return boost::geometry::within(geometry, other.geometry);
    }

    bool contains(const GeometryWrapper<bg_point_xy> &other) const {return false;}
    bool contains(const GeometryWrapper<bg_polygon> &other) const {return false;}
    bool contains(const GeometryWrapper<bg_rectangle> &other) const {return false;}
    bool contains(const GeometryWrapper<bg_linestring> &other) const {return false;}

    bool meets(const GeometryWrapper<bg_polygon>& other) const;
    bool meets(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool meets(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }
    bool meets(const GeometryWrapper<bg_linestring>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }

    bool equals(const GeometryWrapper<bg_point_xy>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_polygon>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_linestring>& other) const {
        return boost::geometry::equals(geometry, other.geometry);
    }
};

// polygon
template<>
struct GeometryWrapper<bg_polygon> {
public:
    bg_polygon geometry;
    GeometryWrapper(){}
    GeometryWrapper(const bg_polygon &geom) : geometry(geom) {}

    void addPoint(const double x, const double y) {
        bg_point_xy point(x, y);
        boost::geometry::append(geometry.outer(), point);
    }

    void correctGeometry() {
        boost::geometry::correct(geometry);
    }

    void printGeometry() {
        for(auto &it: geometry.outer()) {
            printf("(%f,%f),", it.x(), it.y());
        }
        printf("\n");
    }

    void reset() {
        boost::geometry::clear(geometry);
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        if (index < geometry.outer().size()) {
            geometry.outer()[index] = bg_point_xy(x, y);
        } else {
            logger::log_error(DBERR_OUT_OF_BOUNDS, "Polygon point index out of bounds for modifyBoostPointByIndex:", index);
        }
    }

    const std::vector<bg_point_xy>* getReferenceToPoints() const {
        return &geometry.outer();
    }

    int getVertexCount() const {
        return geometry.outer().size();
    }

    // APRIL
    bool pipTest(const bg_point_xy &point) const {
        return boost::geometry::within(point, geometry);
    }

    // topology definitions
    std::string createMaskCode(const GeometryWrapper<bg_rectangle>& other) const {return "";};
    std::string createMaskCode(const GeometryWrapper<bg_polygon>& other) const {
        boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
        return matrix.str();
    }
    std::string createMaskCode(const GeometryWrapper<bg_linestring>& other) const {
        boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
        return matrix.str();
    }
    std::string createMaskCode(const GeometryWrapper<bg_point_xy>& other) const {
        boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
        return matrix.str();
    }

    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return boost::geometry::intersects(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return boost::geometry::disjoint(geometry, other.geometry);
    }

    bool inside(const GeometryWrapper<bg_point_xy>& other) const {return false;}
    bool inside(const GeometryWrapper<bg_linestring>& other) const {return false;}
    bool inside(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool inside(const GeometryWrapper<bg_polygon>& other) const {
        return boost::geometry::within(geometry, other.geometry);
    }

    bool contains(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool contains(const GeometryWrapper<bg_polygon>& other) const {
        return boost::geometry::within(other.geometry, geometry);
    }
    bool contains(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::within(other.geometry, geometry);
    }
    bool contains(const GeometryWrapper<bg_linestring>& other) const {
        return boost::geometry::within(other.geometry, geometry);
    }

    bool meets(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool meets(const GeometryWrapper<bg_polygon>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }
    bool meets(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }
    bool meets(const GeometryWrapper<bg_linestring>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }

    bool equals(const GeometryWrapper<bg_point_xy>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_linestring>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_polygon>& other) const {
        return boost::geometry::equals(geometry, other.geometry);
    }
    bool equals(const GeometryWrapper<bg_rectangle>& other) const {
        return boost::geometry::equals(geometry, other.geometry);
    }
};

/** Define all forward declared member functions for the wrappers. 
 * They had to be forward-declared and later defined because of dependencies.
 */

/** @brief Overloaded method for creating the DE-9IM mask code for Linestring-Polygon cases.*/
inline std::string GeometryWrapper<bg_linestring>::createMaskCode(const GeometryWrapper<bg_polygon>& other) const {
    boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
    return matrix.str();
}
/** @brief Overloaded method for the 'inside' relate predicate query for Linestring-Polygon cases.*/
inline bool GeometryWrapper<bg_linestring>::inside(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
/** @brief Overloaded method for the 'meets' relate predicate query for Linestring-Polygon cases.*/
inline bool GeometryWrapper<bg_linestring>::meets(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::touches(geometry, other.geometry);
}

/** @brief Overloaded method for creating the DE-9IM mask code for Point-Polygon cases.*/
inline std::string GeometryWrapper<bg_point_xy>::createMaskCode(const GeometryWrapper<bg_polygon>& other) const  {
    boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
    return matrix.str();
}
/** @brief Overloaded method for the 'inside' relate predicate query for Point-Linestring cases.*/
inline bool GeometryWrapper<bg_point_xy>::inside(const GeometryWrapper<bg_linestring> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
/** @brief Overloaded method for the 'inside' relate predicate query for Point-Rectangle cases.*/
inline bool GeometryWrapper<bg_point_xy>::inside(const GeometryWrapper<bg_rectangle> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
/** @brief Overloaded method for the 'inside' relate predicate query for Point-Polygon cases.*/
inline bool GeometryWrapper<bg_point_xy>::inside(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
/** @brief Overloaded method for the 'meets' relate predicate query for Point-Linestring cases.*/
inline bool GeometryWrapper<bg_point_xy>::meets(const GeometryWrapper<bg_linestring> &other) const {
    return boost::geometry::touches(geometry, other.geometry);
}
/** @brief Overloaded method for the 'meets' relate predicate query for Point-Polygon cases.*/
inline bool GeometryWrapper<bg_point_xy>::meets(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::touches(geometry, other.geometry);
}

/** @brief Overloaded method for the 'equals' relate predicate query for Rectangle-Polygon cases.*/
inline bool GeometryWrapper<bg_rectangle>::equals(const GeometryWrapper<bg_polygon>& other) const {
    return boost::geometry::equals(geometry, other.geometry);
}

/** @typedef PointWrapper @brief type definition for the point wrapper*/
using PointWrapper = GeometryWrapper<bg_point_xy>;
/** @typedef PolygonWrapper @brief type definition for the polygon wrapper*/
using PolygonWrapper = GeometryWrapper<bg_polygon>;
/** @typedef LineStringWrapper @brief type definition for the linestring wrapper*/
using LineStringWrapper = GeometryWrapper<bg_linestring>;
/** @typedef RectangleWrapper @brief type definition for the rectangle wrapper*/
using RectangleWrapper = GeometryWrapper<bg_rectangle>;

/** @typedef ShapeVariant @brief All the allowed Shape variants (geometry wrappers). */
using ShapeVariant = std::variant<PointWrapper, PolygonWrapper, LineStringWrapper, RectangleWrapper>;

/**
 * @brief A spatial object. Could be point, linestring, rectangle or polygon, as specified by its 'dataType' field.
 * 
 * @details All geometry wrappers are not meant to be visible to the user. Always use this struct for loading, querying or otherwise handling data.
 * Extensions to the struct's methods require explicit definitions for the geometry types in the derived geometry structs.
 */
struct Shape {
private:
    /**
    @brief The geometry variant of the Shape object. Access to the object's boost geometry parent field is done through variant
     * method definitions that utilize this field.
     * @warning Direct access is not encouraged. See the member method definitions for more.
     */
    ShapeVariant shape;
public:
    /** @brief the object's ID, as read by the data file. */
    size_t recID;
    /** @brief the object's geometry type. */
    enum DataType dataType;
    /** @brief the object's MBR. */
    MBR mbr;
    /** @brief The object's partition index, containing info about the partitions that this object intersects with
     * and its two-layer index class in each of them.
     * @param key Partition ID.
     * @param value The two-layer index class of the object in that partition.
     */
    std::unordered_map<int, int> partitions;

    /** @brief Default empty Shape constructor. */
    Shape() {}
    

    /** @brief Default empty expicit type Shape constructor. */
    template<typename T>
    explicit Shape(T geom) : shape(geom) {}

    /** @brief Adds a point to the boost geometry (see derived method definitions). */
    void addPoint(const double x, const double y) {
        // then in boost object
        std::visit([&x, &y](auto&& arg) {
            arg.addPoint(x, y);
        }, shape);
    }

    /** @brief Corrects the geometry object based on the boost geometry standards. */
    void correctGeometry() {
        std::visit([](auto&& arg) {
            arg.correctGeometry();
        }, shape);
    }

    void setFromGeometry(Geometry &geometry, DataType dataType) {
        this->recID = geometry.recID;
        this->dataType = dataType;
        this->mbr.pMin.x = std::numeric_limits<int>::max();
        this->mbr.pMin.y = std::numeric_limits<int>::max();
        this->mbr.pMax.x = -std::numeric_limits<int>::max();
        this->mbr.pMax.y = -std::numeric_limits<int>::max();
        for (int i=0; i<geometry.coords.size(); i+=2) {
            this->mbr.pMin.x = std::min(this->mbr.pMin.x, geometry.coords[i]);
            this->mbr.pMin.y = std::min(this->mbr.pMin.y, geometry.coords[i+1]);
            this->mbr.pMax.x = std::max(this->mbr.pMax.x, geometry.coords[i]);
            this->mbr.pMax.y = std::max(this->mbr.pMax.y, geometry.coords[i+1]);
            this->addPoint(geometry.coords[i], geometry.coords[i+1]);
        }
        this->correctGeometry();
        for (int i=0; i<geometry.partitions.size(); i+=2) {
            this->partitions.insert(std::make_pair(geometry.partitions[i], geometry.partitions[i+1]));
        }
    }

    /** @brief Modifies the point specified by 'index' with the new values x,y. (see derived method definitions) 
     * @param index The position of the point to modify in the geometry object.
     * @param x The new x value.
     * @param y The new y value.
    */
    void modifyBoostPointByIndex(int index, double x, double y) {
        std::visit([index, &x, &y](auto&& arg) {
            arg.modifyBoostPointByIndex(index, x, y);
        }, shape);
    }

    /** @brief Prints the geometry. */
    void printGeometry() {
        printf("id: %zu\n", recID);
        std::visit([](auto&& arg) {
            arg.printGeometry();
        }, shape);
    }

    /** @brief Resets the boost geometry object. */
    void reset() {
        recID = 0;
        dataType = DT_INVALID;
        mbr = MBR();
        partitions.clear();
        std::visit([](auto&& arg) {
            arg.reset();
        }, shape);
    }

    /** @brief Returns a reference to the point list of the geometry. */
    const std::vector<bg_point_xy>* getReferenceToPoints() {
        return std::visit([](auto&& arg) -> const std::vector<bg_point_xy>* {
            return arg.getReferenceToPoints();
        }, shape);
    }

    /** @brief Returns the point count of the geometry. */
    int getVertexCount() {
        return std::visit([](auto&& arg) -> int {
            return arg.getVertexCount();
        }, shape);
    }

    /** @brief Performs a point-in-polygon test with the given point (see derived method definitions). */
    bool pipTest(const bg_point_xy& point) const {
        return std::visit([&point](auto&& arg) -> bool {
            return arg.pipTest(point);
        }, shape);
    }

    /** @brief Generates and returns the DE-9IM mask of this geometry (as R) with the input geometry (as S) 
     * @warning Not all geometry type combinations are supported (see data type support).
    */
    std::string createMaskCode(const Shape &other) const {
        return std::visit([&other](auto&& arg) -> std::string {
            return std::visit([&arg](auto&& otherArg) -> std::string {
                return arg.createMaskCode(otherArg);
            }, other.shape);
        }, shape);
    }

    /** @brief Returns true whether the input geometry intersects (border or area) with this geometry. False otherwise. 
     * @warning Not all geometry type combinations are supported (see data type support).
    */
    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.intersects(otherArg);
            }, other.shape);
        }, shape);
    }

    /** @brief Returns true whether the input geometry is disjoint (no common points) with this geometry. False otherwise. 
     * @warning Not all geometry type combinations are supported (see data type support).
    */
    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.disjoint(otherArg);
            }, other.shape);
        }, shape);
    }

    /** @brief Returns true whether the geometry is completely inside (no inside-border common points) the input geometry. False otherwise. 
     * @warning Not all geometry type combinations are supported (see data type support).
    */
    template<typename OtherBoostGeometryObj>
    bool inside(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.inside(otherArg);
            }, other.shape);
        }, shape);
    }

    /** @brief Returns true whether the geometry is covered by (inside-border common points are allowed) the input geometry. False otherwise. 
     * @warning Not all geometry type combinations are supported (see data type support).
    */
    template<typename OtherBoostGeometryObj>
    bool coveredBy(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.inside(otherArg);
            }, other.shape);
        }, shape);
    }

    /** @brief Returns true whether the geometry completely contains (reverse of inside) the input geometry. False otherwise. 
     * @warning Not all geometry type combinations are supported (see data type support).
    */
    template<typename OtherBoostGeometryObj>
    bool contains(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.contains(otherArg);
            }, other.shape);
        }, shape);
    }

    /** @brief Returns true whether the geometry covers (reverse of covered by) the input geometry. False otherwise. 
     * @warning Not all geometry type combinations are supported (see data type support).
    */
    template<typename OtherBoostGeometryObj>
    bool covers(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.contains(otherArg);
            }, other.shape);
        }, shape);
    }

    /** @brief Returns true whether the geometry meets (touches) the input geometry (their insides do not have common points, but their borders do). 
     * False otherwise. 
     * @warning Not all geometry type combinations are supported (see data type support).
    */
    template<typename OtherBoostGeometryObj>
    bool meets(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.meets(otherArg);
            }, other.shape);
        }, shape);
    }

    /** @brief Returns true whether the geometry is spatially equal the input geometry. False otherwise. 
     * @warning Not all geometry type combinations are supported (see data type support).
    */
    template<typename OtherBoostGeometryObj>
    bool equals(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.equals(otherArg);
            }, other.shape);
        }, shape);
    }

};

/** @namespace shape_factory
@brief contains the factory methods for generating geometry wrapper derived objects.
 */
namespace shape_factory
{
    // Create empty shapes
    Shape createEmptyPointShape();
    Shape createEmptyPolygonShape();
    Shape createEmptyLineStringShape();
    Shape createEmptyRectangleShape();
}

/**
 * @brief Holds all the query result related information.
 */
struct QueryOutput {
    // for regular query rsesults
    int queryResults;
    // for topology relations results
    std::unordered_map<int,uint> topologyRelationsResultMap;
    // statistics
    int postMBRFilterCandidates;
    int refinementCandidates;
    int trueHits;
    int trueNegatives;
    // times
    double totalTime;
    double mbrFilterTime;
    double iFilterTime;
    double refinementTime;

    QueryOutput();

    void reset();
    void countAPRILresult(int result);
    void countResult();
    void countMBRresult();
    void countRefinementCandidate();
    void countTopologyRelationResult(int result);
    int getResultForTopologyRelation(TopologyRelation relation);
    void setTopologyRelationResult(int relation, int result);
    /**
    @brief copies the contents of the 'other' query output object into this struct
     */
    void shallowCopy(QueryOutput &other);
};
/** @brief The main query output global variable used by the host controller to store the results. */
extern QueryOutput g_queryOutput;

/** @brief Holds information about sections, i.e. APRIL partitions.
 */
struct Section {
    uint sectionID;
    // section x,y position indices
    uint i,j;
    //objects that intersect this MBR will be assigned to this area
    double interestxMin, interestyMin, interestxMax, interestyMax;
    //this MBR defines the rasterization area (widened interest area to include intersecting polygons completely)
    double rasterxMin, rasteryMin, rasterxMax, rasteryMax;
    // APRIL data
    size_t objectCount = 0;
    std::unordered_map<size_t, AprilData> aprilData;
};

/** @brief All dataspace related info, filled in after loading the dataset(s).
 * @param xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal The dataspace's borders (MBR).
 * @param xExtent, yExtent, maxExtent The dataspace's extent.
 */
struct DataspaceInfo {
    double xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal;  // global bounds based on dataset bounds
    double xExtent, yExtent, maxExtent;
    bool boundsSet = false;

    DataspaceInfo();
    void set(double xMinGlobal, double yMinGlobal, double xMaxGlobal, double yMaxGlobal);
    void set(DataspaceInfo &other);
    void clear();
};

/** @brief Holds all necessary partition information. 
 * @param partitionID The partition's ID in the grid.
 * @param classIndex Fixed 4 position vector, one for each two-layer index class.
 */
struct Partition {
    size_t partitionID;
    /** @brief Contains the list of objects (Shape) of each class for this partition */
    std::vector<std::vector<Shape*>> classIndex;
    /**
    @brief Default constructor that defines the 4-position vector. Two-layer index classes: A, B, C, D
     */
    Partition(size_t id) : partitionID(id), classIndex(4) {}
    /** @brief Returns a reference to the partition's contents for the given class type. @param classType Either A, B, C or D. */
    std::vector<Shape*>* getContainerClassContents(TwoLayerClass classType);
};

/** @brief Holds all two-layer related index information.
 * @param partitions A vector containing each individual non-empty partition.
 * @param partitionMap A map that holds the positions of each partition (by ID) in the 'partitions' vector.
 */
struct TwoLayerIndex {
    std::vector<Partition> partitions;
    std::unordered_map<int, size_t> partitionMap;
private:
    /** @brief Compares to Shapes by MBR bottom-left point y. */
    static bool compareByY(const Shape* a, const Shape* b) {
        return a->mbr.pMin.y < b->mbr.pMin.y;
    }
public:
    /** @brief Returns or creates a new partition with the given ID. */
    Partition* getOrCreatePartition(int partitionID);
    /** 
    @brief Adds an object to a partition with partitionID, with the specified classType 
     * and returns a pointer to it.
     * @param[in] partitionID The partition's ID to add the object to.
     * @param[in] classType The class of the object in that partition.
     * @param[out] objectRef The returned object reference.
     */
    void addObject(int partitionID, TwoLayerClass classType, Shape* objectRef);
    /**
    @brief Returns the partition reference to this partition ID
     */
    Partition* getPartition(int partitionID);
    /**
    @brief Sorts all objects in all partitions on the Y axis
     */
    void sortPartitionsOnY();
};

/**
 * @brief All dataset related information.
 */
struct Dataset{
    DataType dataType;
    FileType fileType;
    std::string path;
    // derived from the path
    std::string datasetName;
    // as given by arguments and specified by datasets.ini config file
    std::string nickname;
    // holds the dataset's dataspace info (MBR, extent)
    DataspaceInfo dataspaceInfo;
    // unique object count
    size_t totalObjects = 0;
    std::vector<size_t> objectIDs;
    std::unordered_map<size_t, Shape> objects;
    TwoLayerIndex twoLayerIndex;
    ApproximationType approxType;
    AprilConfig aprilConfig;
    /** Section mapping. key,value = sectionID,(unordered map of key,value = recID,aprilData) @warning only sectionID = 0 is supported currently.*/
    std::unordered_map<uint, Section> sectionMap;
    /** map: key,value = recID,vector<sectionID>: maps recs to sections */
    std::unordered_map<size_t,std::vector<uint>> recToSectionIdMap;        

    /** @brief Adds a Shape object into the two layer index and the reference map. 
     * @note Calculates the partitions and the object's classes in them. */
    void addObject(Shape &object);

    /** @brief Returns a reference to the object with the given ID. */
    Shape* getObject(size_t recID);

    /** @brief Calculate the size needed for the dataset serialization. */
    int calculateBufferSize();
    /** @brief Serializes the dataset object (only the important stuff). */
    int serialize(char **buffer);
    /** @brief Deserializes the given serialized buffer of the specified size into this Dataset object. @warning Caller is responsible for the validity of the input buffer. */
    void deserialize(const char *buffer, int bufferSize);
    /** @warning UNSUPPORTED */
    void addAprilDataToApproximationDataMap(const uint sectionID, const size_t recID, const AprilData &aprilData);
    /** @brief Adds the given object ID to the specified section ID. @warning UNSUPPORTED for sections IDs different than 0 (1 partition) */
    void addObjectToSectionMap(const uint sectionID, const size_t recID);
    /** @brief Adds the given intervals into the given object with recID in the given section with section ID.
     * @param ALL Specfies whether the input intervals represent the A-list or not (F-list otherwise).
     */
    void addIntervalsToAprilData(const uint sectionID, const size_t recID, const int numIntervals, const std::vector<uint32_t> &intervals, const bool ALL);
    /** @brief Returns a reference to the April Data of the given object's ID in section ID */
    AprilData* getAprilDataBySectionAndObjectID(uint sectionID, size_t recID);
    /** @brief Sets the april data in the dataset for the given object ID and section ID */
    DB_STATUS setAprilDataForSectionAndObjectID(uint sectionID, size_t recID, AprilData &aprilData);

    DB_STATUS addDataFromGeometryBatch(GeometryBatch &batch);

    void printTwoLayerInfo();
    void printInfo();
};

/** @brief Holds all query-related information.
 */
struct Query{
    QueryType type;
    int numberOfDatasets;
    Dataset R;         // R: left dataset
    Dataset S;         // S: right dataset
    bool boundsSet = false;
    DataspaceInfo dataspaceInfo;
};

/** @brief Holds all system-related directory paths.
 * @note They refer to node-local paths.
 * @warning Hardcoded, tread carefully when altering them.
 */
struct DirectoryPaths {
    std::string configFilePath = "../config.ini";
    const std::string datasetsConfigPath = "../datasets.ini";
    std::string dataPath = "../data/";
    std::string partitionsPath = "../data/partitions/";
    std::string approximationPath = "../data/approximations/";

    void setNodeDataDirectories(std::string &dataPath) {
        this->dataPath = dataPath;
        this->partitionsPath = dataPath + "partitions/";
        this->approximationPath = dataPath + "approximations/";
    }
};

/** @brief Base class for the partitioning methods (abstract class). */
struct PartitioningMethod {
    /** @brief The partitioning technique */
    PartitioningType type;
    /** @brief The number of partitions per dimension */
    int distPartitionsPerDim;
    /** @brief The batch size for the data distribution, in number of objects. */             
    int batchSize;
    /** @brief The distribution (coarse) grid's dataspace info. */
    DataspaceInfo distGridDataspaceInfo;
    // The X,Y extents of a single partition in the distribution grid
    double distPartitionExtentX;
    double distPartitionExtentY;

    /** @brief Base constructor. */
    PartitioningMethod(PartitioningType pType, int pBatchSize, int partitionsPerDimensionNum) : type(pType), batchSize(pBatchSize), distPartitionsPerDim(partitionsPerDimensionNum) {}

    /** @brief Returns the partitioning method's type. */
    PartitioningType getType() {
        return type;
    }

    /** @brief Returns the batch size. */
    int getBatchSize() {
        return batchSize;
    }

    /** @brief Abstract method. Gets the partition's indices for the given partition ID , as defined by the derived partitioning method. */
    void getDistributionGridPartitionIndices(int partitionID, int &i, int &j) {
        j = partitionID / distPartitionsPerDim;
        i = partitionID % distPartitionsPerDim;
    }

    /** @brief Abstract method. Gets the partition's indices for the given partition ID , as defined by the derived partitioning method. */
    virtual void getPartitioningGridPartitionIndices(int partitionID, int &i, int &j) = 0;

    /** @brief Abstract method. Returns the partition ID in the distribution grid for the given indices, as defined by the derived partitioning method. */
    virtual int getDistributionGridPartitionID(int i, int j) = 0;

    /** @brief Abstract method. Returns the partition ID in the partitioning grid for the given indices, as defined by the derived partitioning method. */
    virtual int getPartitioningGridPartitionID(int distI, int distJ, int partI, int partJ) = 0;

    /** @brief Returns the node's rank that's responsible for this partition. 
     * @warning Should only be called by the host controller and the input partitionID should be of the outermost (distribution) grid. */
    int getNodeRankForPartitionID(int partitionID) {
        return (partitionID % g_world_size);
    }

    /** @brief Abstract method. Returns the number of partitions per dimension in the data distribution grid. */
    virtual int getDistributionPPD() = 0;

    /** @brief Abstract method. Returns the number of partitions per dimension in the partitioning grid. */
    virtual int getPartitioningPPD() = 0;

    /** @brief Abstract method. Returns the number of partitions per dimension in the partitioning grid. */
    virtual int getGlobalPPD() = 0;

    /** @brief Sets the distribution grid's dataspace info. (bounds and extent) */
    void setDistGridDataspace(double xMin, double yMin, double xMax, double yMax) {
        distGridDataspaceInfo.set(xMin, yMin, xMax, yMax);
        distPartitionExtentX = distGridDataspaceInfo.xExtent / distPartitionsPerDim;
        distPartitionExtentY = distGridDataspaceInfo.yExtent / distPartitionsPerDim;
    }

     /** @brief Sets the distribution grid's dataspace info. (bounds and extent) */
    void setDistGridDataspace(DataspaceInfo &otherDataspaceInfo) {
        distGridDataspaceInfo = otherDataspaceInfo;
        distPartitionExtentX = distGridDataspaceInfo.xExtent / distPartitionsPerDim;
        distPartitionExtentY = distGridDataspaceInfo.yExtent / distPartitionsPerDim;
    }

    /** @brief Abstract method. Sets the partitioning grid's dataspace info. */
    virtual void setPartGridDataspace(double xMin, double yMin, double xMax, double yMax) = 0;

    /** @brief Set the fine grid's dataspace info. */
    virtual void setPartGridDataspace(DataspaceInfo &otherDataspaceInfo) = 0;

    double getDistPartionExtentX() {
        return distPartitionExtentX;
    }

    double getDistPartionExtentY() {
        return distPartitionExtentY;
    }

    virtual double getPartPartionExtentX() = 0;
    virtual double getPartPartionExtentY() = 0;

    virtual void printInfo() = 0;
};

/** @brief Two Grid partitioning method. Uses a distribution grid for data distribution and a partitioning grid for the actual partitioning. */
struct TwoGridPartitioning : public PartitioningMethod {
private:
    /** @brief The partitioning (fine) grid's number of partitions per dimension */
    int partPartitionsPerDim;
    /** @brief The global grid's number of partitions per dimension. = dPPD * pPPD */
    int globalPartitionsPerDim;
public:
    /** @brief The fine grid's dataspace info. */
    DataspaceInfo partGridDataspaceInfo;
    // The X,Y extents of a single partition in the partitioning grid 
    double partPartitionExtentX;
    double partPartitionExtentY;

    /** @brief Round robing partitioning constructor. */
    TwoGridPartitioning(PartitioningType type, int batchSize, int distPartitionsPerDim, int partPartitionsPerDim) : PartitioningMethod(type, batchSize, distPartitionsPerDim) {
        this->partPartitionsPerDim = partPartitionsPerDim;
        this->distPartitionsPerDim = distPartitionsPerDim;
        globalPartitionsPerDim = partPartitionsPerDim * distPartitionsPerDim;
        // logger::log_success("Set TWOGRID PPD. DistPPD =", this->distPartitionsPerDim, "PartPPD =", this->partPartitionsPerDim, "GlobalPPD =", this->globalPartitionsPerDim);
    }

    /** @brief Get the distribution grid's partition ID (from parent) for the given indices. */
    int getDistributionGridPartitionID(int i, int j) override;

    /** @brief Get the partitioning grid's partition ID (from parent) for the given distribution grid AND partitioning grid indices. */
    int getPartitioningGridPartitionID(int distI, int distJ, int partI, int partJ) override;

    void getPartitioningGridPartitionIndices(int partitionID, int &i, int &j) override;

    /** @brief Returns the distribution (coarse) grid's partitions per dimension number. */
    int getDistributionPPD() override;

    /** @brief Returns the partitioning (fine) grid's partitions per dimension number. */
    int getPartitioningPPD() override;

    /** @brief Returns the partitioning (fine) grid's partitions per dimension number. */
    int getGlobalPPD() override;

    /** @brief Set the fine grid's dataspace info. */
    void setPartGridDataspace(double xMin, double yMin, double xMax, double yMax) override;

    /** @brief Set the fine grid's dataspace info. */
    void setPartGridDataspace(DataspaceInfo &otherDataspaceInfo) override;

    double getPartPartionExtentX() override;
    double getPartPartionExtentY() override;

    void printInfo() override;
};

/** @brief Simple round-robin partitioning method. Uses a single grid for the distribution and the partitioning. 
 * @note It doesn't matter which method will be used to get a partition's ID, since there is a single grid.  */
struct RoundRobinPartitioning : public PartitioningMethod {
public:
    /** @brief Round robing partitioning constructor. */
    RoundRobinPartitioning(PartitioningType type, int batchSize, int partitionsPerDim) : PartitioningMethod(type, batchSize, partitionsPerDim) {}

    /** @brief Get the grid's partition ID (from parent). */
    int getDistributionGridPartitionID(int i, int j) override;

    /** @brief Get the grid's partition ID (from parent). */
    int getPartitioningGridPartitionID(int distI, int distJ, int partI, int partJ) override;

    void getPartitioningGridPartitionIndices(int partitionID, int &i, int &j) override;

    /** @brief Returns the distribution (coarse) grid's partitions per dimension number. */
    int getDistributionPPD() override;

    /** @brief Returns the partitioning (fine) grid's partitions per dimension number. */
    int getPartitioningPPD() override;

    /** @brief Returns the partitioning (fine) grid's partitions per dimension number. */
    int getGlobalPPD() override;

    /** @brief Dist grid = part grid. */
    void setPartGridDataspace(double xMin, double yMin, double xMax, double yMax) override;

    /** @brief Dist grid = part grid. */
    void setPartGridDataspace(DataspaceInfo &otherDataspaceInfo) override;

    double getPartPartionExtentX() override;
    double getPartPartionExtentY() override;

    void printInfo() override;
};

/** @brief Holds all partitioning related information.
 */
// struct PartitioningInfo {
//     /** @brief The partitioning technique */
//     PartitioningType type;                 
//     /** @brief The number of partitions per dimension */
//     int ppdNum;                
//     /** @brief The batch size for the data distribution, in number of objects. */             
//     int batchSize;                         
    
//     // cell enumeration function
//     int getCellID(int i, int j) {
//         return (i + (j * ppdNum));
//     }
//     // node assignment function
//     int getNodeRankForPartitionID(int partitionID) {
//         return (partitionID % g_world_size);
//     }
// };

/** @brief Defines a system task/job that the host controller is responsible for performing/broadcasting.
 */
struct Action {
    ActionType type;
    Action(){
        this->type = ACTION_NONE;
    }
    Action(ActionType type) {
        this->type = type;
    }
};

/** @brief Holds the dataset(s) related info in the configuration.
 */
struct DatasetInfo {
private:
    Dataset* R = nullptr;
    Dataset* S = nullptr;
    Dataset Q;
    int numberOfDatasets;

public:
    std::unordered_map<std::string,Dataset> datasets;
    DataspaceInfo dataspaceInfo;
    
    Dataset* getDatasetByNickname(std::string &nickname);

    int getNumberOfDatasets();

    void clear();

    Dataset* getDatasetR();
    void setDatasetRptr(Dataset *other);

    Dataset* getDatasetS();
    void setDatasetSptr(Dataset *other);

    Dataset* getDatasetQ();
    void setDatasetRptrToQuery();
    /**
    @brief adds a Dataset to the configuration's dataset info
     * @warning it has to be an empty dataset BUT its nickname needs to be set
     */
    void addDataset(Dataset &dataset);

    /**
    @brief adds a query Dataset to the configuration's dataset info
     * @warning it has to be an empty dataset BUT its nickname needs to be set
     */
    void addQueryDataset(Dataset &queryDataset);

    void updateDataspace();
};

/** @brief Holds all approximation related info.
 */
struct ApproximationInfo {
    ApproximationType type;   // sets which of the following fields will be used
    AprilConfig aprilConfig;  
    ApproximationInfo() {
        this->type = AT_NONE;
    }
    ApproximationInfo(ApproximationType type) {
        this->type = type;
    }
};

/** @brief Holds all the query related info in the configuration.
 */
struct QueryInfo {
    QueryType type = Q_NONE;
    int MBRFilter = 1;
    int IntermediateFilter = 1;
    int Refinement = 1;
};

/** @brief Holds all the system related info in the configuration.
 */
struct SystemOptions {
    SystemSetupType setupType;
    std::string nodefilePath;
    uint nodeCount;
};

/** @brief The main configuration struct. Holds all necessary configuration options.
 */
struct Config {
    DirectoryPaths dirPaths;
    SystemOptions options;
    std::vector<Action> actions;
    // PartitioningInfo partitioningInfo;
    PartitioningMethod *partitioningMethod;
    DatasetInfo datasetInfo;
    ApproximationInfo approximationInfo;
    QueryInfo queryInfo;
};



/**
@brief The main global configuration variable.
*/
extern Config g_config;

/**
@brief Based on query type in config, it thread-safely appends new results to the query output in parallel.
 * Used only in parallel sections for OpenMP.
 */
void queryResultReductionFunc(QueryOutput &in, QueryOutput &out);

// Declare the parallel reduction function
#pragma omp declare reduction (query_output_reduction: QueryOutput: queryResultReductionFunc(omp_in, omp_out)) initializer(omp_priv = QueryOutput())


#endif