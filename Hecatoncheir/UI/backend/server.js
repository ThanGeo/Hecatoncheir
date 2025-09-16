const express = require('express');
const multer = require('multer');
const cors = require('cors');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const app = express();
const port = 5000;

app.use(cors());
app.use(express.json());

const UPLOADS_DIR = path.join(__dirname, 'uploads');
const TEMP_FILES_DIR = path.join(__dirname, 'temp_files');

if (!fs.existsSync(UPLOADS_DIR)) fs.mkdirSync(UPLOADS_DIR);
if (!fs.existsSync(TEMP_FILES_DIR)) fs.mkdirSync(TEMP_FILES_DIR);

const upload = multer({ dest: UPLOADS_DIR });

// Start C++ server as persistent process
const cppServerPath = "/home/command/Desktop/Hecatoncheir/build/Hecatoncheir/UI/hec_server"; 

const mpiCmd = "mpirun.mpich";
const mpiArgs = ["-np", "1", cppServerPath];
const cppServer = spawn(mpiCmd, mpiArgs);




cppServer.stdout.on('data', (data) => {
    console.log(`C++: ${data.toString()}`);
});
cppServer.stderr.on('data', (data) => {
    console.error(`C++ ERR: ${data.toString()}`);
});
cppServer.on('close', (code) => {
    console.log(`C++ server exited with code ${code}`);
});

// Send JSON command to C++ server
function sendCommandToCpp(cmd) {
    return new Promise((resolve, reject) => {
        const listener = (data) => {
            try {
                const response = JSON.parse(data.toString());
                resolve(response);
            } catch (err) {
                reject(err);
            }
        };
        cppServer.stdout.once('data', listener);
        cppServer.stdin.write(JSON.stringify(cmd) + '\n');
    });
}

// Save uploaded file temporarily
function saveUploadedFile(file, tempFiles) {
    if (!file) return null;
    const tempName = `${Date.now()}-${file.originalname}`;
    const tempPath = path.join(TEMP_FILES_DIR, tempName);
    fs.renameSync(file.path, tempPath);
    tempFiles.push(tempPath);
    return tempPath;
}

// Cleanup temporary files
function cleanupTempFiles(tempFiles, multerFiles) {
    tempFiles.forEach(f => fs.existsSync(f) && fs.unlinkSync(f));
    if (multerFiles) {
        multerFiles.forEach(f => f && fs.existsSync(f.path) && fs.unlinkSync(f.path));
    }
}

// Endpoints
app.post('/init-hec', async (req, res) => {
    try {
        const hosts = req.body.pcs.map(pc => pc.nameOrIp + ":1");
        const response = await sendCommandToCpp({ action: "init", hosts });
        res.json(response);
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.post('/prepare-hec', upload.fields([
    { name: 'datasetFile', maxCount: 1 },
    { name: 'queryDatasetFile', maxCount: 1 },
    { name: 'leftDatasetFile', maxCount: 1 },
    { name: 'rightDatasetFile', maxCount: 1 }
]), async (req, res) => {
    const { queryType, kValue } = req.body;
    const files = req.files;
    const tempFiles = [];
    const allMulterFiles = [files.datasetFile, files.queryDatasetFile, files.leftDatasetFile, files.rightDatasetFile].flat().filter(Boolean);

    try {
        const requestBody = { action: "prepare", queryType };

        if (queryType === 'knnQuery' || queryType === 'rangeQuery') {
            const dataset = saveUploadedFile(files.datasetFile?.[0], tempFiles);
            const queryDataset = saveUploadedFile(files.queryDatasetFile?.[0], tempFiles);
            if (!dataset || !queryDataset) throw new Error("Missing dataset files");

            requestBody.dataset = dataset;
            requestBody.queryDataset = queryDataset;
            if (kValue) requestBody.kValue = parseInt(kValue);

        } else if (queryType === 'spatialJoins') {
            const leftDataset = saveUploadedFile(files.leftDatasetFile?.[0], tempFiles);
            const rightDataset = saveUploadedFile(files.rightDatasetFile?.[0], tempFiles);
            if (!leftDataset || !rightDataset) throw new Error("Missing left/right dataset files");

            requestBody.leftDataset = leftDataset;
            requestBody.rightDataset = rightDataset;
        }

        const response = await sendCommandToCpp(requestBody);
        cleanupTempFiles(tempFiles, allMulterFiles);
        res.json(response);

    } catch (err) {
        cleanupTempFiles(tempFiles, allMulterFiles);
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.post('/execute-hec', async (req, res) => {
    try {
        const response = await sendCommandToCpp({ action: "execute" });
        res.json(response);
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.post('/terminate-hec', async (req, res) => {
    try {
        const response = await sendCommandToCpp({ action: "terminate" });
        res.json(response);
        cppServer.kill();
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.listen(port, () => {
    console.log(`Node.js server listening at http://localhost:${port}`);
});
