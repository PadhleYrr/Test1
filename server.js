// language: Node.js, file: server.js, runtime: Express + CommonJS
// Loads each Nuvio provider, calls getStreams, streams results to client via SSE
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

const PROVIDERS_DIR = path.join(__dirname, 'providers');
const MANIFEST = JSON.parse(fs.readFileSync(path.join(__dirname, 'providers-meta.json')));

// Dead providers from previous audit - skip immediately
const DEAD_DOMAINS = new Set(['cineby', 'cinefreak', 'nakios', 'peachify', 'persianstremio', 'videasy', 'hdhub4u', 'goated-api']);

// Load a provider module safely
function loadProvider(filename) {
  try {
    const fullPath = path.join(__dirname, filename);
    delete require.cache[require.resolve(fullPath)];
    return require(fullPath);
  } catch (e) {
    return null;
  }
}

// GET /providers - list all providers with metadata
app.get('/providers', (req, res) => {
  const list = MANIFEST.map(p => ({
    id: p.id,
    name: p.name,
    types: p.types,
    dead: DEAD_DOMAINS.has(p.id)
  }));
  res.json(list);
});

// GET /search?tmdbId=&type=movie|tv&season=&episode=&title=
// Streams results via SSE as each provider responds
app.get('/search', async (req, res) => {
  const { tmdbId, type, season, episode } = req.query;

  if (!tmdbId || !type) {
    return res.status(400).json({ error: 'tmdbId and type required' });
  }

  // SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (event, data) => {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };

  let completed = 0;
  const total = MANIFEST.length;

  // Fan out to all providers in parallel, stream each result back
  const tasks = MANIFEST.map(async (meta) => {
    const pid = meta.id;

    if (DEAD_DOMAINS.has(pid)) {
      send('result', {
        provider: meta.name,
        providerId: pid,
        status: 'dead',
        streams: [],
        error: 'Domain offline (NXDOMAIN)'
      });
      completed++;
      if (completed === total) send('done', { total });
      return;
    }

    // Check type support
    if (!meta.types.includes(type)) {
      send('result', {
        provider: meta.name,
        providerId: pid,
        status: 'skip',
        streams: [],
        error: `Does not support type "${type}"`
      });
      completed++;
      if (completed === total) send('done', { total });
      return;
    }

    const mod = loadProvider(meta.filename);
    if (!mod || typeof mod.getStreams !== 'function') {
      send('result', {
        provider: meta.name,
        providerId: pid,
        status: 'error',
        streams: [],
        error: 'Module load failed or no getStreams export'
      });
      completed++;
      if (completed === total) send('done', { total });
      return;
    }

    try {
      const timeout = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Timeout after 20s')), 20000)
      );
      const streams = await Promise.race([
        mod.getStreams(tmdbId, type, season ? parseInt(season) : undefined, episode ? parseInt(episode) : undefined),
        timeout
      ]);

      const result = Array.isArray(streams) ? streams : [];
      send('result', {
        provider: meta.name,
        providerId: pid,
        status: result.length > 0 ? 'ok' : 'empty',
        streams: result.map(s => ({
          name: s.name || '',
          url: s.url || '',
          quality: s.quality || '',
          title: s.title || s.description || '',
          language: s.language || '',
          subtitles: s.subtitles || []
        }))
      });
    } catch (err) {
      send('result', {
        provider: meta.name,
        providerId: pid,
        status: 'error',
        streams: [],
        error: err.message
      });
    }

    completed++;
    if (completed === total) send('done', { total });
  });

  // Keep connection alive until all done or client disconnects
  req.on('close', () => {});
  await Promise.allSettled(tasks);
});

const PORT = process.env.PORT || 3737;
app.listen(PORT, () => {
  console.log(`Nuvio test server running at http://localhost:${PORT}`);
  console.log(`Open http://localhost:${PORT}/index.html in browser`);
});
