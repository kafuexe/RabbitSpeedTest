#!/usr/bin/env node
/* Throughput benchmark for @kafuexe/rabbit-client.
 *
 * Publishes COUNT messages of SIZE bytes with publishMany (pipelined
 * confirms), then drains them with consume() (per-message acks), printing
 * msg/s for each side as JSON. Mirrors the publish/consume-throughput
 * methodology of the rabbit-benchmark suite closely enough for a
 * cross-language sanity comparison, not a rigorous shootout.
 *
 * Usage: node bench/bench.cjs [amqp-url] [count] [size]
 * (run `npm run build` first — this loads the compiled dist/)
 */
const { RabbitClient } = require('../dist/index.js');

const URL = process.argv[2] || 'amqp://guest:guest@localhost:5672/';
const COUNT = Number(process.argv[3] || 50000);
const SIZE = Number(process.argv[4] || 1024);
// Unique per run so a crashed/parallel run can't leave messages behind for us.
const QUEUE = `ts-bench-${process.pid}`;
const INACTIVITY_MS = 30000;

async function main() {
  const client = new RabbitClient(URL, { prefetch: 200 });
  await client.connect({ timeoutMs: 10000 });
  try {
    const body = Buffer.alloc(SIZE, 'x');
    const bodies = Array.from({ length: COUNT }, () => body);

    const p0 = process.hrtime.bigint();
    await client.publishMany(QUEUE, bodies);
    const publishSecs = Number(process.hrtime.bigint() - p0) / 1e9;

    let consumed = 0;
    const c0 = process.hrtime.bigint();
    let consumeSecs;
    let handle;
    let watchdog;
    try {
      await new Promise((resolve, reject) => {
        // Fail fast if consumption stalls: reject when the consumed count has
        // not moved for INACTIVITY_MS (otherwise a lost consumer hangs forever).
        let lastSeen = 0;
        watchdog = setInterval(() => {
          if (consumed === lastSeen) {
            reject(new Error(
              `consume stalled: ${consumed}/${COUNT} messages after ` +
              `${INACTIVITY_MS / 1000}s of inactivity (queue=${QUEUE})`,
            ));
          } else {
            lastSeen = consumed;
          }
        }, INACTIVITY_MS);
        client.consume(QUEUE, async () => {
          consumed += 1;
          if (consumed === COUNT) {
            consumeSecs = Number(process.hrtime.bigint() - c0) / 1e9;
            resolve();
          }
        }).then((h) => { handle = h; }, reject); // consume() failure fails the run
      });
    } finally {
      clearInterval(watchdog);
      if (handle) {
        // Teardown must never mask the real error (e.g. the stall diagnostic):
        // a cancel against a wedged channel can itself throw.
        try {
          await handle.cancel(); // stop the consumer before queue delete/close
        } catch (cancelErr) {
          console.error('teardown: consumer cancel failed:', cancelErr.message);
        }
      }
    }

    console.log(JSON.stringify({
      client: '@kafuexe/rabbit-client',
      url: URL.replace(/\/\/.*@/, '//***@'),
      count: COUNT,
      size_bytes: SIZE,
      publish_msgs_per_sec: Math.round(COUNT / publishSecs),
      consume_msgs_per_sec: Math.round(COUNT / consumeSecs),
      publish_secs: Number(publishSecs.toFixed(2)),
      consume_secs: Number(consumeSecs.toFixed(2)),
    }, null, 2));
  } finally {
    await client.deleteQueue(QUEUE).catch(() => {}); // best-effort cleanup
    await client.close();
  }
}

main().then(() => process.exit(0), (err) => { console.error(err); process.exit(1); });
