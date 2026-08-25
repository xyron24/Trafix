const API_BASE = '/api';

// Format bytes
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

// Format timestamp
function formatTime(ms) {
    const d = new Date(ms);
    return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second:'2-digit' }) + '.' + d.getMilliseconds().toString().padStart(3, '0');
}

// Update Global Metrics
async function fetchMetrics() {
    try {
        const res = await fetch(`${API_BASE}/metrics`);
        if (!res.ok) return;
        const data = await res.json();
        
        document.getElementById('global-total-conns').textContent = data.total_connections;
        document.getElementById('global-active-conns').textContent = data.active_connections;
        document.getElementById('global-bytes-c2b').textContent = formatBytes(data.bytes_c2b);
        document.getElementById('global-bytes-b2c').textContent = formatBytes(data.bytes_b2c);
    } catch (e) {
        console.error("Failed to fetch metrics", e);
    }
}

// Update Topology
async function fetchTopology() {
    try {
        const res = await fetch(`${API_BASE}/topology`);
        if (!res.ok) return;
        const data = await res.json();
        
        const grid = document.getElementById('services-grid');
        grid.innerHTML = ''; // clear for simple redraw
        
        const srvTpl = document.getElementById('service-template');
        const bkdTpl = document.getElementById('backend-template');
        
        if (data.services) {
            data.services.forEach(srv => {
                const srvNode = srvTpl.content.cloneNode(true);
                srvNode.querySelector('.service-name').textContent = srv.name;
                srvNode.querySelector('.service-port').textContent = ':' + srv.listen_port;
                
                const backendsList = srvNode.querySelector('.backends-list');
                srv.backends.forEach(bkd => {
                    const bkdNode = bkdTpl.content.cloneNode(true);
                    const bkdEl = bkdNode.querySelector('.backend-item');
                    
                    bkdEl.classList.add(bkd.is_healthy ? 'healthy' : 'down');
                    bkdNode.querySelector('.backend-host').textContent = `${bkd.host}:${bkd.port}`;
                    bkdNode.querySelector('.backend-conns').textContent = bkd.active_connections;
                    
                    const actionBtn = bkdNode.querySelector('.backend-action-btn');
                    if (bkd.is_healthy) {
                        actionBtn.textContent = '⏹';
                        actionBtn.title = 'Stop Backend';
                        actionBtn.classList.add('stop-btn');
                        actionBtn.onclick = () => stopBackend(bkd.port);
                    } else {
                        actionBtn.textContent = '▶';
                        actionBtn.title = 'Start Backend';
                        actionBtn.classList.add('play-btn');
                        actionBtn.onclick = () => startBackend(bkd.port);
                    }
                    
                    backendsList.appendChild(bkdNode);
                });
                
                grid.appendChild(srvNode);
            });
        }
    } catch (e) {
        console.error("Failed to fetch topology", e);
    }
}

// Update Events Feed
let lastEventTimestamp = 0;
async function fetchEvents() {
    try {
        const res = await fetch(`${API_BASE}/events`);
        if (!res.ok) return;
        const events = await res.json();
        
        const terminal = document.getElementById('terminal-feed');
        let added = false;
        
        events.forEach(ev => {
            // Very simple deduplication based on exact timestamp+type matching
            // In a real app we'd want unique event IDs
            if (ev.timestamp > lastEventTimestamp) {
                lastEventTimestamp = ev.timestamp;
                
                const div = document.createElement('div');
                div.className = `log-entry ${ev.type.toLowerCase()}`;
                
                const timeSpan = document.createElement('span');
                timeSpan.className = 'log-time';
                timeSpan.textContent = `[${formatTime(ev.timestamp)}]`;
                
                const txt = document.createTextNode(ev.metadata);
                
                div.appendChild(timeSpan);
                div.appendChild(txt);
                terminal.appendChild(div);
                added = true;
            }
        });
        
        if (added) {
            terminal.scrollTop = terminal.scrollHeight;
        }
        
    } catch (e) {
        console.error("Failed to fetch events", e);
    }
}

// Poll every second
setInterval(() => {
    fetchMetrics();
    fetchTopology();
    fetchEvents();
}, 1000);

// Initial fetch
fetchMetrics();
fetchTopology();
fetchEvents();

// Modal Logic
const modal = document.getElementById('add-service-modal');
const addBtn = document.getElementById('add-service-btn');
const closeBtn = document.getElementById('close-modal-btn');
const form = document.getElementById('add-service-form');

if (addBtn && modal && closeBtn) {
    addBtn.onclick = () => modal.classList.add('active');
    closeBtn.onclick = () => {
        modal.classList.remove('active');
        form.reset();
    };
}

if (form) {
    form.onsubmit = async (e) => {
        e.preventDefault();
        const serviceName = document.getElementById('service-name').value;
        const listenPort = parseInt(document.getElementById('listen-port').value);
        const backendHost = document.getElementById('backend-host').value;
        const backendPort = parseInt(document.getElementById('backend-port').value);

        const payload = {
            name: serviceName,
            listen_port: listenPort,
            backends: [{ host: backendHost, port: backendPort }]
        };

        try {
            const res = await fetch(`${API_BASE}/service`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (res.ok) {
                modal.classList.remove('active');
                form.reset();
                fetchTopology();
            } else {
                console.error("Failed to save service");
            }
        } catch (err) {
            console.error(err);
        }
    };
}

async function startBackend(port) {
    try {
        await fetch(`${API_BASE}/backend/start`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ port })
        });
        setTimeout(fetchTopology, 500); // short delay to let it come up
    } catch (e) { console.error(e); }
}

async function stopBackend(port) {
    try {
        await fetch(`${API_BASE}/backend/stop`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ port })
        });
        setTimeout(fetchTopology, 500); // short delay to let it go down
    } catch (e) { console.error(e); }
}

