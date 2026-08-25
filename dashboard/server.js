const express = require('express');
const net = require('net');
const path = require('path');
const fs = require('fs');
const yaml = require('js-yaml');
const { exec, spawn } = require('child_process');

const app = express();
const port = 5991;
const SOCKET_PATH = '/tmp/gateway_admin.sock';

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

function queryGateway(command) {
    return new Promise((resolve, reject) => {
        const client = net.createConnection(SOCKET_PATH, () => {
            client.write(command);
        });

        let data = '';
        client.on('data', (chunk) => {
            data += chunk.toString();
        });

        client.on('end', () => {
            try {
                const json = JSON.parse(data);
                resolve(json);
            } catch (err) {
                console.error("Failed to parse gateway response:", data);
                resolve({ error: "parse_error", raw: data });
            }
        });

        client.on('error', (err) => {
            reject(err);
        });
    });
}

app.get('/api/metrics', async (req, res) => {
    try {
        const data = await queryGateway('GET_METRICS');
        res.json(data);
    } catch (err) {
        res.status(500).json({ error: 'Gateway unavailable' });
    }
});

app.get('/api/topology', async (req, res) => {
    try {
        const data = await queryGateway('GET_TOPOLOGY');
        res.json(data);
    } catch (err) {
        res.status(500).json({ error: 'Gateway unavailable' });
    }
});

app.get('/api/events', async (req, res) => {
    try {
        const data = await queryGateway('GET_EVENTS');
        res.json(data);
    } catch (err) {
        res.status(500).json({ error: 'Gateway unavailable' });
    }
});

app.post('/api/service', (req, res) => {
    try {
        const configPath = path.join(__dirname, '../config/gateway.yaml');
        const fileContents = fs.readFileSync(configPath, 'utf8');
        const doc = yaml.load(fileContents);
        
        if (!doc.gateway) doc.gateway = {};
        if (!doc.gateway.services) doc.gateway.services = [];
        
        doc.gateway.services.push(req.body);
        
        fs.writeFileSync(configPath, yaml.dump(doc));
        
        exec('pkill -HUP gateway', (error, stdout, stderr) => {
            if (error) {
                console.error(`exec error: ${error}`);
            }
            res.json({ success: true });
        });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: 'Failed to update config' });
    }
});

app.post('/api/backend/start', (req, res) => {
    const port = req.body.port;
    if (!port) return res.status(400).json({ error: 'Port required' });
    
    const backendScript = path.join(__dirname, '../tests/echo_backend.py');
    const child = spawn('python3', [backendScript, port.toString()], {
        detached: true,
        stdio: 'ignore'
    });
    child.unref();
    
    res.json({ success: true });
});

app.post('/api/backend/stop', (req, res) => {
    const port = req.body.port;
    if (!port) return res.status(400).json({ error: 'Port required' });
    
    exec(`pkill -f "echo_backend.py ${port}"`, (error, stdout, stderr) => {
        if (error) {
            console.error(`exec error: ${error}`);
        }
        res.json({ success: true });
    });
});

app.listen(port, () => {
    console.log(`Gateway Dashboard running at http://localhost:${port}`);
});
