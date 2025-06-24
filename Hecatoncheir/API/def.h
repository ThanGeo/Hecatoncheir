#ifndef HEC_API_DEF_H
#define HEC_API_DEF_H

namespace hec
{
    /** @brief Input file types supported by Hecatoncheir.
     * @details 
     * - CSV assumes there is no header line in the beginning of the file.
     * - WKT assumes that the file is indeed a CSV, but the first column is a WKT with the shape (disregards the rest)
     */
    enum FileType {
        FT_CSV,
        FT_WKT,
    };

    /** @enum QueryType @brief Query types. */
    enum QueryType{
        Q_RANGE,
        Q_INTERSECTION_JOIN,
        Q_INSIDE_JOIN,
        Q_DISJOINT_JOIN,
        Q_EQUAL_JOIN,
        Q_MEET_JOIN,
        Q_CONTAINS_JOIN,
        Q_COVERS_JOIN,
        Q_COVERED_BY_JOIN,
        Q_FIND_RELATION_JOIN,    // find what type of topological relation is there
        Q_DISTANCE_JOIN,
        Q_KNN,
        Q_NONE = 777, // no query
    };

    /** @brief Topological Relations. */
    enum TopologyRelation {
        TR_DISJOINT,
        TR_INTERSECT,
        TR_INSIDE,
        TR_CONTAINS,
        TR_COVERED_BY,
        TR_COVERS,
        TR_EQUAL,
        TR_MEET,
    };
}

#endif