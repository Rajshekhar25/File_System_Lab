'use strict';
/**
 * Performance analysis tool.
 *
 * It runs N clients at the same time, first uploading and then downloading a
 * file of a chosen size, and reports per-client time plus the aggregate
 * throughput. Running it with 1, 2, 4 and 8 clients is what produces the table
 * for the report: it shows whether one event loop really does overlap many
 * transfers, and how the balancer spread the work over the nodes.
 *
 *     npm run bench -- --size 50 --clients 4
 *     node src/bench/benchmark.js --size 50 --clients 4
 */

const crypto = require('crypto');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');

const clientModule = require('../client/client');
const { Client } = clientModule;
const config = require('../common/config');
const { humanSize } = require('../common/protocol');

const WORK_DIR = path.join(config.BASE_DIR, 'benchdata');
const REPORT = path.join(config.RUNTIME_DIR, 'perf_report.txt');

/** Create (once) a file of the requested size to push around. */
async function makeTestFile(target, sizeMb) {
  const wanted = sizeMb * 1024 * 1024;
  const existing = await fsp.stat(target).catch(() => null);
  if (existing && existing.size === wanted) return target;

  const block = crypto.randomBytes(1024 * 1024);
  const out = fs.createWriteStream(target);
  for (let i = 0; i < sizeMb; i += 1) {
    if (!out.write(block)) {
      await new Promise((resolve) => out.once('drain', resolve));
    }
  }
  await new Promise((resolve) => out.end(resolve));
  return target;
}

async function uploadWorker(index, source, sizeBytes) {
  const client = new Client(`bench${index}`);
  const result = { worker: index, node: '-', seconds: 0, ok: false };
  const started = Date.now();

  try {
    const node = await client.pickNode();
    result.node = node.node;
    await client._push(node, source, `bench_${index}.bin`, 0, sizeBytes);
    result.ok = true;
  } catch (err) {
    console.log(`  client ${index} upload failed: ${err.message}`);
  }

  result.seconds = (Date.now() - started) / 1000;
  return result;
}

async function downloadWorker(index, _source, sizeBytes) {
  const client = new Client(`bench${index}`);
  const result = { worker: index, node: '-', seconds: 0, ok: false };
  const target = path.join(WORK_DIR, `out_${index}.bin`);
  const started = Date.now();

  try {
    const node = await client.pickNode();
    result.node = node.node;
    await client._pull(node, `bench_${index}.bin`, `${target}.part`, 0);
    await fsp.rm(target, { force: true });
    await fsp.rename(`${target}.part`, target);
    result.ok = true;
  } catch (err) {
    console.log(`  client ${index} download failed: ${err.message}`);
  }

  result.seconds = (Date.now() - started) / 1000;
  return result;
}

async function runPhase(name, worker, clients, sizeBytes, source) {
  console.log(`\n${name}: ${clients} client(s) x ${humanSize(sizeBytes)}`);
  const wallStart = Date.now();

  const jobs = [];
  for (let index = 0; index < clients; index += 1) {
    jobs.push(worker(index, source, sizeBytes));
  }
  const results = await Promise.all(jobs);
  const wall = Math.max((Date.now() - wallStart) / 1000, 1e-6);

  const lines = ['', `${name}  (${clients} clients, ${humanSize(sizeBytes)} each)`];
  lines.push('  client   node     time(s)   MB/s     status');
  for (const r of results) {
    const rate = r.seconds ? sizeBytes / r.seconds / (1024 * 1024) : 0;
    lines.push(`  ${String(r.worker).padEnd(8)} ${r.node.padEnd(8)} `
      + `${r.seconds.toFixed(2).padEnd(9)} ${rate.toFixed(2).padEnd(8)} `
      + `${r.ok ? 'ok' : 'FAILED'}`);
  }

  const done = results.filter((r) => r.ok).length;
  const totalBytes = done * sizeBytes;
  lines.push(`  total: ${humanSize(totalBytes)} in ${wall.toFixed(2)}s  ->  `
    + `aggregate ${(totalBytes / wall / (1024 * 1024)).toFixed(2)} MB/s  `
    + `(${done}/${clients} succeeded)`);

  const spread = {};
  for (const r of results) spread[r.node] = (spread[r.node] || 0) + 1;
  lines.push('  balancer spread: '
    + Object.entries(spread).sort().map(([k, v]) => `${k}=${v}`).join(', '));

  const text = lines.join('\n');
  console.log(text);
  return text;
}

function parseArgs(argv) {
  const args = { size: 20, clients: 4, skipDownload: false };
  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i] === '--size') args.size = Number(argv[i + 1]);
    else if (argv[i] === '--clients' || argv[i] === '--threads') {
      args.clients = Number(argv[i + 1]);
    } else if (argv[i] === '--skip-download') args.skipDownload = true;
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv);

  await fsp.mkdir(WORK_DIR, { recursive: true });
  config.ensureDirs();
  clientModule.options.quiet = true; // progress bars from 8 clients are unreadable

  const sizeBytes = args.size * 1024 * 1024;
  const source = await makeTestFile(
    path.join(WORK_DIR, `payload_${args.size}MB.bin`), args.size,
  );
  console.log(`test payload: ${source} (${humanSize(sizeBytes)})`);

  const report = [
    '='.repeat(62),
    `performance run at ${new Date().toLocaleString()}`,
    `chunk size: ${humanSize(config.CHUNK_SIZE)}`,
  ];

  report.push(await runPhase('UPLOAD', uploadWorker, args.clients, sizeBytes, source));
  if (!args.skipDownload) {
    report.push(await runPhase('DOWNLOAD', downloadWorker, args.clients, sizeBytes, null));
  }

  await fsp.appendFile(REPORT, report.join('\n') + '\n', 'utf8');
  console.log(`\nreport appended to ${REPORT}`);
}

if (require.main === module) {
  main().catch((err) => {
    console.error('benchmark failed:', err.message);
    process.exitCode = 1;
  });
}
