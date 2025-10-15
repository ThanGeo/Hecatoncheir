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

const projectRoot = path.resolve(__dirname, '..', '..', '..');
const cppServerPath = path.join(projectRoot, 'build', 'Hecatoncheir', 'UI', 'hec_server');

const mpiCmd = "mpirun.mpich";
const mpiArgs = ["-np", "1", cppServerPath];

let activeTempFiles = [];
let cppServer = null;

function getFileFormat(filename) {
    const ext = path.extname(filename).toLowerCase();
    if (ext === '.csv') return 'CSV';
    if (ext === '.tsv') return 'TSV';
    if (ext === '.wkt') return 'WKT';
    return 'WKT'; // default
}

function mapGeometryType(frontendType) {
    const mapping = {
        'points': 'POINT',
        'point': 'POINT',
        'line': 'LINE',
        'polygon': 'POLYGON',
        'box': 'BOX' 
    };
    return mapping[frontendType] || 'POINT';
}

function startCppServer() {
    cppServer = spawn(mpiCmd, mpiArgs);

    cppServer.stdout.on('data', (data) => {
        console.log(`C++: ${data.toString()}`);
    });

    cppServer.stderr.on('data', (data) => {
        console.error(`C++ ERR: ${data.toString()}`);
    });

    cppServer.on('close', (code) => {
        console.log(`C++ server exited with code ${code}`);
        cppServer = null;
    });
}

let cppPending = false;

function sendCommandToCpp(cmd, { timeoutMs = 5000 } = {}) {
  return new Promise((resolve, reject) => {
    if (cppPending) {
      return reject(new Error('Another command is already pending. Use a queue or include request IDs in the protocol.'));
    }
    cppPending = true;

    let buffer = '';          
    let prefixPending = '';   
    let timeout = null;

    const cleanup = () => {
      cppServer.stdout.off('data', onData);
      if (timeout) clearTimeout(timeout);
      cppPending = false;
    };

    timeout = setTimeout(() => {
      cleanup();
      reject(new Error('Timeout waiting for response from C++'));
    }, timeoutMs);

    const tryExtractJSON = () => {

      let s = buffer;

      while (true) {
        const firstBrace = s.indexOf('{');
        const lastBrace = s.lastIndexOf('}');

        if (firstBrace === -1 || lastBrace === -1 || lastBrace <= firstBrace) {
          break;
        }

        const candidate = s.slice(firstBrace, lastBrace + 1).trim();

        try {
          const parsed = JSON.parse(candidate);
          buffer = s.slice(lastBrace + 1);
          return parsed;
        } catch (err) {

          const prevLast = s.lastIndexOf('}', lastBrace - 1);
          if (prevLast === -1 || prevLast <= firstBrace) {
            break;
          } else {
            s = s.slice(0, prevLast + 1);
            continue;
          }
        }
      }

      return null;
    };

    const onData = (data) => {
      buffer += data.toString();


      buffer = buffer.replace(/\r/g, ''); 
      let parsed;
      try {
        parsed = tryExtractJSON();
      } catch (err) {
        cleanup();
        return reject(err);
      }

      if (parsed !== null) {
        cleanup();
        return resolve(parsed);
      }

    };

    cppServer.stdout.on('data', onData);

    try {
      cppServer.stdin.write(JSON.stringify(cmd) + '\n');
    } catch (err) {
      cleanup();
      return reject(err);
    }
  });
}

function saveUploadedFile(file, tempFiles) {
    if (!file) return null;
    const tempName = `${Date.now()}-${file.originalname}`;
    const tempPath = path.join(TEMP_FILES_DIR, tempName);
    fs.renameSync(file.path, tempPath);
    tempFiles.push(tempPath);
    return tempPath;
}

function cleanupTempFiles(tempFiles, multerFiles) {
    tempFiles.forEach(f => fs.existsSync(f) && fs.unlinkSync(f));
    if (multerFiles) {
        multerFiles.forEach(f => f && fs.existsSync(f.path) && fs.unlinkSync(f.path));
    }
}

app.post('/init-hec', async (req, res) => {
    try {
        if (!cppServer) {
            console.log("Spawning fresh C++ process...");
            startCppServer();
        }
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
    const { queryType, kValue, predicate, spatialDataType, querySetType } = req.body;
    const files = req.files;
    const tempFiles = [];
    const allMulterFiles = [files.datasetFile, files.queryDatasetFile, files.leftDatasetFile, files.rightDatasetFile].flat().filter(Boolean);

    try {
        const requestBody = { action: "prepare", queryType };

        if (queryType === 'knnQuery' || queryType === 'rangeQuery') {
            const datasetFile = files.datasetFile?.[0];
            const queryDatasetFile = files.queryDatasetFile?.[0];
            
            const dataset = saveUploadedFile(datasetFile, tempFiles);
            const queryDataset = saveUploadedFile(queryDatasetFile, tempFiles);
            
            if (!dataset || !queryDataset) throw new Error("Missing dataset files");

            requestBody.dataset = dataset;
            requestBody.queryDataset = queryDataset;
            
            // Extract file formats
            requestBody.fileFormat = getFileFormat(datasetFile.originalname);
            requestBody.queryFileFormat = getFileFormat(queryDatasetFile.originalname);
            
            // Map geometry types
            requestBody.geometryType = mapGeometryType(spatialDataType);
            requestBody.queryGeometryType = mapGeometryType(querySetType);
            
            if (kValue) requestBody.kValue = parseInt(kValue);

        } else if (queryType === 'spatialJoins') {
            const leftDatasetFile = files.leftDatasetFile?.[0];
            const rightDatasetFile = files.rightDatasetFile?.[0];
            
            const leftDataset = saveUploadedFile(leftDatasetFile, tempFiles);
            const rightDataset = saveUploadedFile(rightDatasetFile, tempFiles);
            
            if (!leftDataset || !rightDataset) throw new Error("Missing left/right dataset files");

            requestBody.leftDataset = leftDataset;
            requestBody.rightDataset = rightDataset;
            
            // Extract file formats
            requestBody.leftFileFormat = getFileFormat(leftDatasetFile.originalname);
            requestBody.rightFileFormat = getFileFormat(rightDatasetFile.originalname);
            
            // Map geometry types
            requestBody.leftGeometryType = mapGeometryType(spatialDataType);
            requestBody.rightGeometryType = mapGeometryType(querySetType);
            
            if (predicate) {
                requestBody.predicate = predicate;
            }
        }

        console.log("Sending to C++:", requestBody);
        const response = await sendCommandToCpp(requestBody);
        activeTempFiles.push(...tempFiles);
        res.json(response);

    } catch (err) {
        cleanupTempFiles(tempFiles, allMulterFiles);
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.post('/execute-hec', async (req, res) => {
    try {
        const response = await sendCommandToCpp({ action: "execute" });
        cleanupTempFiles(activeTempFiles);
        activeTempFiles = [];

        res.json(response);
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.post('/clear', async (req, res) => {
    try {
        const response = await sendCommandToCpp({ action: "clear" });
        cleanupTempFiles(activeTempFiles);
        activeTempFiles = [];
        res.json(response);
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.get('/download-results/:filename', (req, res) => {
    const filename = req.params.filename;
    const filepath = path.join(projectRoot, 'build', 'Hecatoncheir', 'UI', filename);
    
    if (fs.existsSync(filepath)) {
        res.download(filepath, filename, (err) => {
            if (err) {
                console.error('Download error:', err);
            }
            fs.unlinkSync(filepath);
        });
    } else {
        res.status(404).json({ error: 'File not found' });
    }
});

app.post('/terminate-hec', async (req, res) => {
    try {
        const response = await sendCommandToCpp({ action: "terminate" });
        cleanupTempFiles(activeTempFiles);
        activeTempFiles = [];
        res.json(response);
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.listen(port, () => {
    console.log(`Node.js server listening at http://localhost:${port}`);
});