#ifndef APRIL_TOPOLOGY_H
#define APRIL_TOPOLOGY_H

#include "SpatialLib.h"
#include "join.h"

namespace APRIL
{
    int findRelationAPRILUncompressed(uint idR, uint idS, spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);

    /**
     * joins two APRIL approximations to detect true hits, true negatives and inconclusive cases for INTERSECTION.
    */
    int intersectionAPRILUncompressed(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);

    /**
     * joins two APRIL approximations to detect true hits, true negatives and inconclusive cases for DISJOINT.
    */
    int disjointAPRILUncompressed(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);

    /**
     * joins two APRIL approximations to detect true hits, true negatives and inconclusive cases for INSIDE (R inside S).
    */
    int insideAPRILUncompressed(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);

    /**
     * joins two APRIL approximations to detect true negatives and inconclusive cases for EQUAL
     */
    int equalAPRILUncompressed(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);

    /**
     * joins two APRIL approximations to detect true negatives and inconclusive cases for MEET
     */
    int meetAPRILUncompressed(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);

    /**
     * joins two APRIL approximations to detect true negatives and inconclusive cases for CONTAINS
     */
    int containsAPRILUncompressed(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);

    /**
     * joins two APRIL approximations to detect true negatives and inconclusive cases for COVERS
     */
    int coversAPRILUncompressed(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);

    /**
     * joins two APRIL approximations to detect true negatives and inconclusive cases for COVERED BY
     */
    int coveredByAPRILUncompressed(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);



    /**
     * @brief if MBR(r) in MBR(s), then use this method
     * 
     * @param aprilR 
     * @param aprilS 
     * @return int 
     */
    int RinSContainmentAPRILUncompressed(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);



    /**
     * @brief if MBR(s) in MBR(r), then use this method
     * 
     * @param aprilR 
     * @param aprilS 
     * @return int 
     */
    int SinRContainmentAPRILUncompressed(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);


    /**
     * @brief if MBRs intersect generally, then use this method
     * 
     * @param aprilR 
     * @param aprilS 
     * @return int 
     */
    int MBRIntersectionAPRILUncompressed(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);


    /**
     * @brief if MBRs are equal, then use this method
     * 
     * @param aprilR 
     * @param aprilS 
     * @return int 
     */
    int equalMBRsAPRILUncompressed(uint idR, uint idS, spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS);
}


#endif