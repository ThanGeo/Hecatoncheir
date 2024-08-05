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

/*************************************
 * 
 * 
 * 
 * 
 *              APRIL
 * 
 * 
 * 
 * 
 *********************************** */

// APRIL data
struct AprilData {
    // APRIL data
    uint numIntervalsALL = 0;
    std::vector<uint32_t> intervalsALL;
    uint numIntervalsFULL = 0;
    std::vector<uint32_t> intervalsFULL;

    void printALLintervals();
    void printALLcells(uint32_t n);
    void printFULLintervals();
};

// APRIL configuration
struct AprilConfig {
    private:
        // Hilbert curve order
        int N = 16;
        // cells per dimension in Hilbert grid: 2^N
        int cellsPerDim = pow(2,16);
    public:
        // compression enabled, disabled
        bool compression = 0;
        // how many partitions (sections) in the data space
        int partitions = 1;
        // APRIL data file paths
        std::string ALL_intervals_path;
        std::string FULL_intervals_path;

    void setN(int N);
    int getN();
    int getCellsPerDim();
};


/*************************************
 * 
 * 
 * 
 * 
 *          DATA STRUCTS
 * 
 * 
 * 
 * 
 *********************************** */

struct Point {
    double x, y;
    Point(double xVal, double yVal) : x(xVal), y(yVal) {}
};

struct MBR {
    Point pMin;
    Point pMax;

    MBR() : pMin(Point(std::numeric_limits<int>::max(), std::numeric_limits<int>::max())), pMax(Point(-std::numeric_limits<int>::max(), -std::numeric_limits<int>::max())) {}
};

// wrapper struct 
template<typename GeometryType>
struct GeometryWrapper {
public:
    GeometryType geometry;

    GeometryWrapper() {}
    explicit GeometryWrapper(const GeometryType& geom) : geometry(geom) {}

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
        // Default behavior: Do nothing (for types that don't support modifying points)
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

// point
template<>
struct GeometryWrapper<bg_point_xy> {
public:
    bg_point_xy geometry;
    GeometryWrapper(){}
    GeometryWrapper(const bg_point_xy &geom) : geometry(geom) {}

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

/* define forward declared member functions for the wrappers */
// linestring
inline std::string GeometryWrapper<bg_linestring>::createMaskCode(const GeometryWrapper<bg_polygon>& other) const {
    boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
    return matrix.str();
}
inline bool GeometryWrapper<bg_linestring>::inside(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
inline bool GeometryWrapper<bg_linestring>::meets(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::touches(geometry, other.geometry);
}

// point
inline std::string GeometryWrapper<bg_point_xy>::createMaskCode(const GeometryWrapper<bg_polygon>& other) const  {
    boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
    return matrix.str();
}
inline bool GeometryWrapper<bg_point_xy>::inside(const GeometryWrapper<bg_linestring> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
inline bool GeometryWrapper<bg_point_xy>::inside(const GeometryWrapper<bg_rectangle> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
inline bool GeometryWrapper<bg_point_xy>::inside(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
inline bool GeometryWrapper<bg_point_xy>::meets(const GeometryWrapper<bg_linestring> &other) const {
    return boost::geometry::touches(geometry, other.geometry);
}
inline bool GeometryWrapper<bg_point_xy>::meets(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::touches(geometry, other.geometry);
}

//rectangle
inline bool GeometryWrapper<bg_rectangle>::equals(const GeometryWrapper<bg_polygon>& other) const {
    return boost::geometry::equals(geometry, other.geometry);
}

// define shape wrappers and shape struct
using PointWrapper = GeometryWrapper<bg_point_xy>;
using PolygonWrapper = GeometryWrapper<bg_polygon>;
using LineStringWrapper = GeometryWrapper<bg_linestring>;
using RectangleWrapper = GeometryWrapper<bg_rectangle>;

using ShapeVariant = std::variant<PointWrapper, PolygonWrapper, LineStringWrapper, RectangleWrapper>;

struct Shape {
    size_t recID;
    DataTypeE dataType;
    MBR mbr;
    // partition ID -> two layer class of this object in that partition
    std::unordered_map<int, int> partitions;
    // shape variant
    ShapeVariant shape;

    Shape() {}

    template<typename T>
    explicit Shape(T geom) : shape(geom) {}

    /**
     * utils
     */

    // adds a point to the shape
    void addPoint(const double x, const double y) {
        // then in boost object
        std::visit([&x, &y](auto&& arg) {
            arg.addPoint(x, y);
        }, shape);
    }

    void correctGeometry() {
        std::visit([](auto&& arg) {
            arg.correctGeometry();
        }, shape);
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        std::visit([index, &x, &y](auto&& arg) {
            arg.modifyBoostPointByIndex(index, x, y);
        }, shape);
    }

    void printGeometry() {
        printf("id: %zu\n", recID);
        std::visit([](auto&& arg) {
            arg.printGeometry();
        }, shape);
    }

    void reset() {
        recID = 0;
        dataType = DT_INVALID;
        mbr = MBR();
        partitions.clear();
        std::visit([](auto&& arg) {
            arg.reset();
        }, shape);
    }

    const std::vector<bg_point_xy>* getReferenceToPoints() {
        return std::visit([](auto&& arg) -> const std::vector<bg_point_xy>* {
            return arg.getReferenceToPoints();
        }, shape);
    }

    int getVertexCount() {
        return std::visit([](auto&& arg) -> int {
            return arg.getVertexCount();
        }, shape);
    }

    /**
     * APRIL
     */

    bool pipTest(const bg_point_xy& point) const {
        return std::visit([&point](auto&& arg) -> bool {
            return arg.pipTest(point);
        }, shape);
    }

    /**
     * queries/spatial operations
     */
    std::string createMaskCode(const Shape &other) const {
        return std::visit([&other](auto&& arg) -> std::string {
            return std::visit([&arg](auto&& otherArg) -> std::string {
                return arg.createMaskCode(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.intersects(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.disjoint(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool inside(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.inside(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool coveredBy(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.inside(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool contains(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.contains(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool covers(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.contains(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool meets(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.meets(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool equals(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.equals(otherArg);
            }, other.shape);
        }, shape);
    }

};

namespace shape_factory
{
    // Create empty shapes
    Shape createEmptyPointShape();
    Shape createEmptyPolygonShape();
    Shape createEmptyLineStringShape();
    Shape createEmptyRectangleShape();
}

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
    int getResultForTopologyRelation(TopologyRelationE relation);
    void setTopologyRelationResult(int relation, int result);
    /**
     * @brief copies the contents of the 'other' object into this struct
     */
    void shallowCopy(QueryOutput &other);
};
// global query output variable
extern QueryOutput g_queryOutput;

struct Section {
    uint sectionID;
    // axis position indexes
    uint i,j;
    //objects that intersect this MBR will be assigned to this area
    double interestxMin, interestyMin, interestxMax, interestyMax;
    // double normInterestxMin, normInterestyMin, normInterestxMax, normInterestyMax;
    //this MBR defines the rasterization area (widened interest area to include intersecting polygons completely)
    double rasterxMin, rasteryMin, rasterxMax, rasteryMax;
    // double normRasterxMin, normRasteryMin, normRasterxMax, normRasteryMax;
    // APRIL data
    size_t objectCount = 0;
    std::unordered_map<size_t, AprilData> aprilData;
};

struct DataspaceInfo {
    double xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal;  // global bounds based on dataset bounds
    double xExtent, yExtent, maxExtent;
    bool boundsSet = false;

    DataspaceInfo();
    void set(double xMinGlobal, double yMinGlobal, double xMaxGlobal, double yMaxGlobal);
    void clear();
};


struct Partition {
    size_t partitionID;
    /* 4 positions, one for each class type. Contains the objects of that class for this partition */
    std::vector<std::vector<Shape*>> classIndex;
    /**
     * @brief Constructor: there are only 4 classes (A, B, C, D)
     */
    Partition(size_t id) : partitionID(id), classIndex(4) {}
    std::vector<Shape*>* getContainerClassContents(TwoLayerClassE classType);
};

struct TwoLayerIndex {
    std::vector<Partition> partitions;
    std::unordered_map<int, size_t> partitionMap;
    // methods
private:
    static bool compareByY(const Shape* a, const Shape* b) {
        return a->mbr.pMin.y < b->mbr.pMin.y;
    }
public:
    Partition* getOrCreatePartition(int partitionID);
    /** 
     * @brief adds an object to the partition with partitionID, with the specified classType 
     * @note creates a copy of the object, and returns a pointer to the copied address
     * @warning input Shape &object has virtually no lifetime
     */
    void addObject(int partitionID, TwoLayerClassE classType, Shape* objectRef);
    /**
     * returns the partition reference to this partition ID
     */
    Partition* getPartition(int partitionID);
    /**
     * sorts all objects in all partitions on the Y axis
     */
    void sortPartitionsOnY();
};

struct Dataset{
    DataTypeE dataType;
    FileTypeE fileType;
    std::string path;
    // derived from the path
    std::string datasetName;
    // as given by arguments and specified by datasets.ini config file
    std::string nickname;
    // holds the dataset's dataspace info (MBR, extent)
    DataspaceInfo dataspaceInfo;
    // unique object count
    size_t totalObjects = 0;
    std::unordered_map<size_t, Shape> objects;
    // two layer
    TwoLayerIndex twoLayerIndex;
    /* approximations */ 
    ApproximationTypeE approxType;
    // APRIL
    AprilConfig aprilConfig;
    std::unordered_map<uint, Section> sectionMap;                        // map: k,v = sectionID,(unordered map of k,v = recID,aprilData)
    std::unordered_map<size_t,std::vector<uint>> recToSectionIdMap;         // map: k,v = recID,vector<sectionID>: maps recs to sections 

    /**
     * Methods
    */
    /**
     * @brief adds a Shape object into two layer index and the reference map
     * 
     */
    void addObject(Shape &object);

    Shape* getObject(size_t recID);

    // calculate the size needed for the serialization buffer
    int calculateBufferSize();
    /**
     * serialize dataset info (only specific stuff)
     */
    int serialize(char **buffer);
    /**
     * deserializes a serialized buffer that contains the dataset info, 
     * into this Dataset object
     */
    void deserialize(const char *buffer, int bufferSize);
    // APRIL
    void addAprilDataToApproximationDataMap(const uint sectionID, const size_t recID, const AprilData &aprilData);
    void addObjectToSectionMap(const uint sectionID, const size_t recID);
    void addIntervalsToAprilData(const uint sectionID, const size_t recID, const int numIntervals, const std::vector<uint32_t> &intervals, const bool ALL);
    AprilData* getAprilDataBySectionAndObjectID(uint sectionID, size_t recID);
};

struct Query{
    QueryTypeE type;
    int numberOfDatasets;
    Dataset R;         // R: left dataset
    Dataset S;         // S: right dataset
    bool boundsSet = false;
    // double xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal;  // global bounds based on dataset bounds
    DataspaceInfo dataspaceInfo;
};

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

struct PartitioningInfo {
    PartitioningTypeE type;                 // type of the partitioning technique
    int partitionsPerDimension;             // # of partitions per dimension
    int batchSize;                          // size of each batche, in number of objects
    
    // cell enumeration function
    int getCellID(int i, int j) {
        return (i + (j * partitionsPerDimension));
    }
    // node assignment function
    int getNodeRankForPartitionID(int partitionID) {
        return (partitionID % g_world_size);
    }
};

struct Action {
    ActionTypeE type;

    Action(){
        this->type = ACTION_NONE;
    }

    Action(ActionTypeE type) {
        this->type = type;
    }
    
};

struct DatasetInfo {
    private:
    Dataset* R;
    Dataset* S;
    int numberOfDatasets;

    public:
    std::unordered_map<std::string,Dataset> datasets;
    DataspaceInfo dataspaceInfo;
    
    Dataset* getDatasetByNickname(std::string &nickname);

    int getNumberOfDatasets();

    void clear();

    Dataset* getDatasetR();

    Dataset* getDatasetS();
    /**
     * @brief adds a Dataset to the configuration's dataset info
     * @warning it has to be an empty dataset BUT its nickname needs to be set
     */
    void addDataset(Dataset &dataset);

    void updateDataspace();
};

struct ApproximationInfo {
    ApproximationTypeE type;   // sets which of the following fields will be used
    AprilConfig aprilConfig;  
    
    ApproximationInfo() {
        this->type = AT_NONE;
    }

    ApproximationInfo(ApproximationTypeE type) {
        this->type = type;
    }
};

struct QueryInfo {
    QueryTypeE type = Q_NONE;
    int MBRFilter = 1;
    int IntermediateFilter = 1;
    int Refinement = 1;
};

struct SystemOptions {
    SystemSetupTypeE setupType;
    std::string nodefilePath;
    uint nodeCount;
};

struct Config {
    DirectoryPaths dirPaths;
    SystemOptions options;
    std::vector<Action> actions;
    PartitioningInfo partitioningInfo;
    DatasetInfo datasetInfo;
    ApproximationInfo approximationInfo;
    QueryInfo queryInfo;
};

struct Geometry {
    size_t recID;
    int partitionCount;
    std::vector<int> partitions;    // tuples of <partition ID, twolayer class>
    int vertexCount;
    std::vector<double> coords;

    Geometry(size_t recID, int vertexCount, std::vector<double> &coords);
    Geometry(size_t recID, std::vector<int> &partitions, int vertexCount, std::vector<double> &coords);
    void setPartitions(std::vector<int> &ids, std::vector<TwoLayerClassE> &classes);
};

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
     * @brief serializes the geometry batch into the buffer. This method also allocates the buffer's memory.
     * Caller is responsible to free.
     */
    int serialize(char **buffer);

    /**
     * @brief fills the struct with data from the input serialized buffer
     * The caller must free the buffer memory
     */
    void deserialize(const char *buffer, int bufferSize);

};

/**
 * @brief global configuration variable 
*/
extern Config g_config;

/**
 * @brief based on query type in config, it appropriately adds the query output in parallel
 * Used only in parallel sections for OpenMP
 */
void queryResultReductionFunc(QueryOutput &in, QueryOutput &out);

// Declare the parallel reduction function
#pragma omp declare reduction (query_output_reduction: QueryOutput: queryResultReductionFunc(omp_in, omp_out)) initializer(omp_priv = QueryOutput())


#endif