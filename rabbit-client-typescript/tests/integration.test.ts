/**
 * Integration tests for RabbitClient against a REAL RabbitMQ broker.
 *
 * These complement the mocked unit suite (rabbit-client.test.ts), which can
 * only verify that we CALL amqp-connection-manager correctly — not that the
 * broker agrees. Everything here exercises the live wire protocol: real
 * publisher confirms, real basic.qos prefetch enforcement, real
 * redelivery-on-nack, real basic.cancel.
 *
 * Broker discovery: a quick TCP probe of localhost:5672. When no broker is
 * listening the whole suite is skipped (cleanly, so CI without a broker
 * stays green). When one is listening, the tests MUST pass.
 *
 * Hygiene: every queue is uniquely named with the `ts-inttest-` prefix and
 * deleted in afterEach, so a concurrently running benchmark on the same
 * broker is never disturbed and nothing is left behind.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { RabbitClient } from '../src';

const AMQP_HOST = process.env['RABBIT_HOST'] ?? 'localhost';
const AMQP_PORT = Number(process.env['RABBIT_PORT'] ?? 5672);
const AMQP_URL = `amqp://guest:guest@${AMQP_HOST}:${AMQP_PORT}/`;

/**
 * True iff something is accepting TCP connections on host:port.
 * Synchronous (tiny child process) because describe.skipIf needs the answer
 * at collection time, and this file compiles as CJS where top-level await
 * is unavailable.
 */
function probeBrokerSync(host: string, port: number, timeoutMs = 1500): boolean {
    const script =
        `const s=require('net').connect(${port},${JSON.stringify(host)});` +
        `s.setTimeout(${timeoutMs},()=>process.exit(1));` +
        `s.once('error',()=>process.exit(1));` +
        `s.once('connect',()=>{s.end();process.exit(0);});`;
    const result = spawnSync(process.execPath, ['-e', script], {
        timeout: timeoutMs + 2000,
    });
    return result.status === 0;
}

const brokerUp = probeBrokerSync(AMQP_HOST, AMQP_PORT);

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

async function waitFor(
    condition: () => boolean,
    timeoutMs: number,
    what: string,
): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!condition()) {
        if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs}ms waiting for ${what}`);
        await sleep(20);
    }
}

describe.skipIf(!brokerUp)(`integration against live broker at ${AMQP_HOST}:${AMQP_PORT}`, () => {
    let client: RabbitClient;
    /** Extra clients opened by individual tests; closed in afterEach. */
    let extraClients: RabbitClient[] = [];
    /** Queues declared during the current test; deleted in afterEach. */
    let queues: string[] = [];

    /** Mint a unique, tracked queue name. */
    const mintQueue = (label: string): string => {
        const name = `ts-inttest-${label}-${randomUUID().slice(0, 8)}`;
        queues.push(name);
        return name;
    };

    beforeAll(async () => {
        client = new RabbitClient(AMQP_URL);
        await client.connect({ timeoutMs: 10_000 });
    }, 15_000);

    afterEach(async () => {
        for (const extra of extraClients) {
            await extra.close().catch(() => {});
        }
        extraClients = [];
        // Delete queues through the long-lived client so a failed test can
        // never leave ts-inttest-* debris on the shared broker.
        for (const queue of queues) {
            await client.deleteQueue(queue).catch(() => {});
        }
        queues = [];
    }, 15_000);

    afterAll(async () => {
        await client.close();
    }, 15_000);

    it(
        'publish → consume round-trip preserves payloads byte-for-byte',
        async () => {
            const queue = mintQueue('roundtrip');
            // Binary-unfriendly payloads on purpose: NUL bytes, high bytes, UTF-8.
            const payloads = [
                Buffer.from([0x00, 0x01, 0xff, 0xfe, 0x80]),
                Buffer.from('plain text payload'),
                Buffer.from('unicode: héllo — 世界 🐇', 'utf8'),
                Buffer.alloc(64 * 1024, 0xab), // 64 KiB blob
            ];
            for (const p of payloads) {
                await client.publish(queue, p);
            }

            const received: Buffer[] = [];
            const consumer = await client.consume(queue, async (body) => {
                received.push(Buffer.from(body)); // copy: body may be pooled
            });
            await waitFor(() => received.length >= payloads.length, 10_000, 'all payloads');
            await consumer.cancel();

            expect(received).toHaveLength(payloads.length);
            const wire = received.map((b) => b.toString('base64')).sort();
            const sent = payloads.map((b) => b.toString('base64')).sort();
            expect(wire).toEqual(sent);
        },
        20_000,
    );

    it(
        'a rejected handler nacks with requeue: the broker redelivers, then the retry is acked',
        async () => {
            const queue = mintQueue('redeliver');
            await client.publish(queue, Buffer.from('poison-then-fine'));

            let attempts = 0;
            let succeeded = false;
            const consumer = await client.consume(queue, async () => {
                attempts += 1;
                if (attempts === 1) {
                    throw new Error('simulated handler failure (expect nack+requeue)');
                }
                succeeded = true;
            });
            await waitFor(() => succeeded, 10_000, 'redelivery after nack');
            await consumer.cancel();

            expect(attempts).toBe(2); // exactly one redelivery, then success

            // The retry must have been ACKED: a fresh consumer sees nothing.
            let extraDeliveries = 0;
            const checker = await client.consume(queue, async () => {
                extraDeliveries += 1;
            });
            await sleep(750);
            await checker.cancel();
            expect(extraDeliveries).toBe(0);
        },
        20_000,
    );

    it(
        'publishMany pushes 3000 confirmed messages and a consumer fully drains them',
        async () => {
            const queue = mintQueue('bulk');
            const COUNT = 3000;
            const bodies = Array.from({ length: COUNT }, (_, i) => Buffer.from(`bulk-${i}`));
            await client.publishMany(queue, bodies); // resolves only on broker confirms

            const seen = new Set<string>();
            const consumer = await client.consume(queue, async (body) => {
                seen.add(body.toString());
            });
            await waitFor(() => seen.size >= COUNT, 60_000, `${COUNT} unique messages`);
            await consumer.cancel();

            expect(seen.size).toBe(COUNT); // no dupes (Set) and no losses
            for (const i of [0, 1, COUNT / 2, COUNT - 1]) {
                expect(seen.has(`bulk-${i}`)).toBe(true);
            }
        },
        90_000,
    );

    it(
        'prefetch=2 caps concurrent handler invocations at 2 (real basic.qos)',
        async () => {
            const queue = mintQueue('prefetch');
            const prefetchClient = new RabbitClient(AMQP_URL, { prefetch: 2 });
            extraClients.push(prefetchClient);
            await prefetchClient.connect({ timeoutMs: 10_000 });

            const TOTAL = 12;
            await client.publishMany(
                queue,
                Array.from({ length: TOTAL }, (_, i) => Buffer.from(`p${i}`)),
            );

            let inFlight = 0;
            let maxInFlight = 0;
            let handled = 0;
            const consumer = await prefetchClient.consume(queue, async () => {
                inFlight += 1;
                maxInFlight = Math.max(maxInFlight, inFlight);
                await sleep(100); // hold the slot so overlap is observable
                inFlight -= 1;
                handled += 1;
            });
            await waitFor(() => handled >= TOTAL, 30_000, 'all prefetch messages');
            await consumer.cancel();

            expect(handled).toBe(TOTAL);
            expect(maxInFlight).toBeLessThanOrEqual(2); // qos actually enforced
            expect(maxInFlight).toBeGreaterThan(1); // ...and handlers DID overlap
        },
        40_000,
    );

    it(
        'consumer.cancel() stops deliveries; unconsumed messages stay queued',
        async () => {
            const queue = mintQueue('cancel');
            let deliveredAfterCancel = 0;
            const consumer = await client.consume(queue, async () => {
                deliveredAfterCancel += 1;
            });
            await consumer.cancel();
            await consumer.cancel(); // idempotent against the real broker too

            const LATE = 5;
            await client.publishMany(
                queue,
                Array.from({ length: LATE }, (_, i) => Buffer.from(`late-${i}`)),
            );
            await sleep(750); // window in which a live consumer WOULD have fired
            expect(deliveredAfterCancel).toBe(0);

            // The messages were not consumed — a fresh consumer drains all 5.
            let drained = 0;
            const fresh = await client.consume(queue, async () => {
                drained += 1;
            });
            await waitFor(() => drained >= LATE, 10_000, 'late messages drained');
            await fresh.cancel();
            expect(drained).toBe(LATE);
        },
        20_000,
    );

    it(
        'deleteQueue destroys the queue and its messages; redeclare starts empty',
        async () => {
            const queue = mintQueue('delete');
            await client.publishMany(queue, [
                Buffer.from('doomed-1'),
                Buffer.from('doomed-2'),
                Buffer.from('doomed-3'),
            ]);
            await client.deleteQueue(queue);

            // Publishing again re-declares the queue (declare cache was
            // dropped); only the post-delete message may ever arrive.
            await client.publish(queue, Buffer.from('survivor'));
            const received: string[] = [];
            const consumer = await client.consume(queue, async (body) => {
                received.push(body.toString());
            });
            await waitFor(() => received.length >= 1, 10_000, 'post-delete message');
            await sleep(500); // grace period: any doomed-* would surface here
            await consumer.cancel();

            expect(received).toEqual(['survivor']);
        },
        20_000,
    );
});
