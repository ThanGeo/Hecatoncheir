#include "storage.h"

namespace APRIL
{

    uint loadSpace32[10000000];

    void saveAPRILonDisk(std::ofstream &foutALL, std::ofstream &foutFULL, uint recID, uint sectionID, spatial_lib::AprilDataT* aprilData) {
        // std::cout << "Polygon " << recID << " ALL intervals: " << polygon.rasterData.data.listA.size() << std::endl; 
        //write ID
        foutALL.write((char*)(&recID), sizeof(uint));
        //write section
        foutALL.write((char*)(&sectionID), sizeof(uint));
        //write number of intervals
        foutALL.write((char*)(&aprilData->numIntervalsALL), sizeof(uint));
        //write the interval data (numintervals * 2 uint values)
        foutALL.write((char*)(&aprilData->intervalsALL.data()[0]), aprilData->numIntervalsALL * 2 * sizeof(uint));

        // same for FULL (if any)
        if(aprilData->numIntervalsFULL > 0){
            foutFULL.write((char*)(&recID), sizeof(uint));
            foutFULL.write((char*)(&sectionID), sizeof(uint));
            foutFULL.write((char*)(&aprilData->numIntervalsFULL), sizeof(uint));
            foutFULL.write((char*)(&aprilData->intervalsFULL.data()[0]), aprilData->numIntervalsFULL * 2 * sizeof(uint));	
        }
    }

    void loadAPRILfromDisk(spatial_lib::DatasetT &dataset) {
        size_t fileSize;
        uint8_t* buffer;
        size_t result;
        int sectionID;
        uint totalPolygons;
        uint recID;
        /* ---------- ALL ---------- */
        FILE* pFile = fopen(dataset.aprilConfig.ALL_intervals_path.c_str(), "rb");
        // obtain file size:
        fseek (pFile , 0 , SEEK_END);
        fileSize = ftell (pFile);
        rewind (pFile);
        // allocate memory to contain the whole file:
        buffer = (uint8_t*) malloc (sizeof(uint8_t)*fileSize);
        if (buffer == NULL) {fputs ("Memory error",stderr); exit (2);}
        // copy the file into the buffer:
        result = fread (buffer, 1, fileSize, pFile);
        if (result != fileSize) {fputs ("Reading error",stderr); exit (3);}
        //init read indices
        unsigned long bufferIndex = 0;
        unsigned long loadIndex = 0;

        //read total polygons
        memcpy(&totalPolygons, &buffer[bufferIndex], sizeof(uint));
        bufferIndex += sizeof(uint);

        //read loop
        uint polCounter = 0;
        while(bufferIndex < fileSize){
            loadIndex = 0;
            //id
            memcpy(&recID, &buffer[bufferIndex], sizeof(uint));
            bufferIndex += sizeof(uint);

            // make approximation object
            spatial_lib::AprilDataT aprilData = spatial_lib::createEmptyAprilDataObject();
            
            //section
            memcpy(&sectionID, &buffer[bufferIndex], sizeof(uint));
            bufferIndex += sizeof(uint);
            
            //total intervals in polygon
            memcpy(&aprilData.numIntervalsALL, &buffer[bufferIndex], sizeof(uint));
            bufferIndex += sizeof(uint);

            //copy uncompressed data
            uint totalValues = aprilData.numIntervalsALL * 2;
            memcpy(&loadSpace32[loadIndex], &buffer[bufferIndex], totalValues * sizeof(uint));
            loadIndex += totalValues;
            bufferIndex += totalValues * sizeof(uint);

            //add intervals to april data
            aprilData.intervalsALL.insert(aprilData.intervalsALL.begin(), &loadSpace32[0], loadSpace32 + loadIndex);

            // add to dataset map
            spatial_lib::addAprilDataToApproximationDataMap(dataset, sectionID, recID, &aprilData);
            
            polCounter++;
        }
        // terminate this file
        fclose (pFile);
        
        //free memory
        free (buffer);

        /* ---------- FULL ---------- */
        // obtain file size:
        pFile = fopen(dataset.aprilConfig.FULL_intervals_path.c_str(), "rb");
        fseek (pFile , 0 , SEEK_END);
        fileSize = ftell (pFile);
        rewind (pFile);
        // allocate memory to contain the whole file:
        buffer = (uint8_t*) malloc (sizeof(uint8_t)*fileSize);
        if (buffer == NULL) {fputs ("Memory error",stderr); exit (2);}
        // copy the file into the buffer:
        result = fread (buffer, 1, fileSize, pFile);
        if (result != fileSize) {fputs ("Reading error",stderr); exit (3);}
        //init read indices
        bufferIndex = 0;
        loadIndex = 0;

        //read total polygons
        memcpy(&totalPolygons, &buffer[bufferIndex], sizeof(uint));
        bufferIndex += sizeof(uint);

        //read loop
        polCounter = 0;
        while(bufferIndex < fileSize){
            loadIndex = 0;
            //id
            memcpy(&recID, &buffer[bufferIndex], sizeof(uint));
            bufferIndex += sizeof(uint);

            //section
            memcpy(&sectionID, &buffer[bufferIndex], sizeof(uint));
            bufferIndex += sizeof(uint);
            
            // fetch approximation object
            spatial_lib::AprilDataT* aprilData = spatial_lib::getAprilDataBySectionAndObjectIDs(dataset, sectionID, recID);
            
            //total intervals in polygon
            memcpy(&aprilData->numIntervalsFULL, &buffer[bufferIndex], sizeof(uint));
            bufferIndex += sizeof(uint);

            //copy uncompressed data
            uint totalValues = aprilData->numIntervalsFULL * 2;
            memcpy(&loadSpace32[loadIndex], &buffer[bufferIndex], totalValues * sizeof(uint));
            loadIndex += totalValues;
            bufferIndex += totalValues * sizeof(uint);		

            //add intervals to the april data reference
            aprilData->intervalsFULL.insert(aprilData->intervalsFULL.begin(), &loadSpace32[0], loadSpace32 + loadIndex);


            // if (recID == 206434) {
            //     printAPRIL(*aprilData);
            //     printf("^Dataset: %s polygon %u\n", dataset.nickname.c_str(), recID);
            // }

            polCounter++;
        }
        // terminate this file
        fclose (pFile);
        
        //free memory
        free (buffer);
    }
}