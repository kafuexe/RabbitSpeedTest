/**
 * Unit tests for RabbitClient. amqp-connection-manager is fully mocked —
 * no broker needed. The mock mimics the parts of the real API the client
 * relies on: connect(), createChannel(), addSetup/removeSetup (setup
 * functions run immediately against a fake amqplib channel), sendToQueue
 * with confirm semantics, consume/cancel, ack/nack, deleteQueue.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { connect } from 'amqp-connection-manager';
import { RabbitClient } from '../src';

/* ------------------------------ fakes ------------------------------ */

interface FakeConsumer {
    queue: string;
    onMessage: (msg: unknown) => void;
    options: Record<string, unknown>;
    consumerTag: string;
}

function makeFakeChannelWrapper() {
    const fakeAmqplibChannel = {
        assertQueue: vi.fn(async () => ({})),
    };
    const setups: Array<(ch: unknown) => Promise<void>> = [];
    const consumers: FakeConsumer[] = [];
    const listeners = new Map<string, Array<(...args: unknown[]) => void>>();
    let tagCounter = 0;
    return {
        fakeAmqplibChannel,
        setups,
        consumers,
        listeners,
        /**
         * Simulate a full channel reconnect with the REAL ChannelWrapper
         * ordering (_onConnect): re-run every setup function FIRST, then
         * re-establish consumers — the point where amqplib can already
         * dispatch deliveries, synchronously from the same TCP burst as the
         * consume-ok — and only THEN emit 'connect'.
         * `deliverDuringReestablish` injects deliveries at that exact
         * mid-point (after setups, before the 'connect' event) to model the
         * earliest possible post-reconnect delivery.
         */
        async simulateReconnect(deliverDuringReestablish?: () => void): Promise<void> {
            for (const fn of setups) await fn(fakeAmqplibChannel);
            deliverDuringReestablish?.(); // consumers re-established; deliveries may fire NOW
            for (const fn of listeners.get('connect') ?? []) fn();
        },
        addSetup: vi.fn(async (fn: (ch: unknown) => Promise<void>) => {
            setups.push(fn);
            await fn(fakeAmqplibChannel);
        }),
        removeSetup: vi.fn(async (fn: (ch: unknown) => Promise<void>) => {
            const i = setups.indexOf(fn);
            if (i >= 0) setups.splice(i, 1);
        }),
        sendToQueue: vi.fn(
            async (_queue: string, _body: Buffer, _options?: Record<string, unknown>) => true,
        ),
        consume: vi.fn(
            async (
                queue: string,
                onMessage: (msg: unknown) => void,
                options: Record<string, unknown>,
            ) => {
                const consumerTag = `ctag-${++tagCounter}`;
                consumers.push({ queue, onMessage, options, consumerTag });
                return { consumerTag };
            },
        ),
        cancel: vi.fn(async () => {}),
        ack: vi.fn(),
        nack: vi.fn(),
        deleteQueue: vi.fn(async () => ({ messageCount: 0 })),
        waitForConnect: vi.fn(async () => {}),
        on: vi.fn((event: string, fn: (...args: unknown[]) => void) => {
            const fns = listeners.get(event) ?? [];
            fns.push(fn);
            listeners.set(event, fns);
        }),
        removeListener: vi.fn((event: string, fn: (...args: unknown[]) => void) => {
            const fns = listeners.get(event);
            if (!fns) return;
            const i = fns.indexOf(fn);
            if (i >= 0) fns.splice(i, 1);
        }),
        close: vi.fn(async () => {}),
    };
}
type FakeChannelWrapper = ReturnType<typeof makeFakeChannelWrapper>;

function makeFakeManager() {
    const channels: FakeChannelWrapper[] = [];
    return {
        channels,
        connect: vi.fn(async () => {}),
        createChannel: vi.fn((_opts?: Record<string, unknown>) => {
            const ch = makeFakeChannelWrapper();
            channels.push(ch);
            return ch;
        }),
        isConnected: vi.fn(() => true),
        close: vi.fn(async () => {}),
    };
}
type FakeManager = ReturnType<typeof makeFakeManager>;
type RealManager = ReturnType<typeof connect>;

const managers: FakeManager[] = [];

vi.mock('amqp-connection-manager', () => ({
    connect: vi.fn(() => {
        const manager = makeFakeManager();
        managers.push(manager);
        return manager;
    }),
}));
const connectMock = vi.mocked(connect);

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Narrow away `undefined` from indexed access (noUncheckedIndexedAccess). */
function must<T>(value: T | undefined, what = 'value'): T {
    if (value === undefined) throw new Error(`expected ${what} to exist`);
    return value;
}

async function connectedClient(opts: { prefetch?: number; durable?: boolean } = {}) {
    const client = new RabbitClient('amqp://guest:guest@localhost/', opts);
    await client.connect();
    const pubManager = must(managers.at(-2), 'publish manager');
    const conManager = must(managers.at(-1), 'consume manager');
    return {
        client,
        pubManager,
        conManager,
        pubChannel: must(pubManager.channels[0], 'publish channel'),
        conChannel: must(conManager.channels[0], 'consume channel'),
    };
}

beforeEach(() => {
    managers.length = 0;
    connectMock.mockClear();
});

/* ------------------------------ tests ------------------------------ */

describe('connect', () => {
    it('opens SEPARATE connections for publish and consume', async () => {
        const { pubManager, conManager } = await connectedClient();
        expect(connectMock).toHaveBeenCalledTimes(2);
        expect(managers).toHaveLength(2);
        expect(pubManager).not.toBe(conManager);
        // one channel per connection: confirm channel on the publish side
        expect(pubManager.createChannel).toHaveBeenCalledTimes(1);
        expect(conManager.createChannel).toHaveBeenCalledTimes(1);
        expect(pubManager.createChannel).toHaveBeenCalledWith(
            expect.objectContaining({ confirm: true, json: false }),
        );
        expect(conManager.createChannel).toHaveBeenCalledWith(
            expect.objectContaining({ confirm: false, json: false }),
        );
    });

    it('publish uses the publish connection, consume uses the consume connection', async () => {
        const { client, pubChannel, conChannel } = await connectedClient();
        await client.publish('q', Buffer.from('x'));
        await client.consume('q', async () => {});
        expect(pubChannel.sendToQueue).toHaveBeenCalledTimes(1);
        expect(pubChannel.consume).not.toHaveBeenCalled();
        expect(conChannel.consume).toHaveBeenCalledTimes(1);
        expect(conChannel.sendToQueue).not.toHaveBeenCalled();
    });

    it('a second connect() closes the previous manager pair before opening a new one', async () => {
        const { client, pubManager, conManager } = await connectedClient();
        await client.connect(); // reentrant: must tear down the stale pair first
        // The abandoned managers would otherwise reconnect (and consume) forever.
        expect(pubManager.close).toHaveBeenCalledTimes(1);
        expect(conManager.close).toHaveBeenCalledTimes(1);
        expect(managers).toHaveLength(4); // a FRESH pair was created
        expect(must(managers[2]).close).not.toHaveBeenCalled();
        expect(must(managers[3]).close).not.toHaveBeenCalled();
        expect(client.isConnected()).toBe(true); // now backed by the new pair
    });

    it('closes the surviving connection when only one side fails', async () => {
        const boom = new Error('connection limit reached');
        const failing = makeFakeManager();
        failing.connect.mockRejectedValue(boom);
        const surviving = makeFakeManager();
        connectMock
            .mockImplementationOnce(() => failing as unknown as RealManager)
            .mockImplementationOnce(() => surviving as unknown as RealManager);

        const client = new RabbitClient('amqp://localhost/');
        await expect(client.connect()).rejects.toBe(boom);
        expect(failing.close).toHaveBeenCalled();
        expect(surviving.close).toHaveBeenCalled(); // survivor must not leak
        expect(client.isConnected()).toBe(false);
    });
});

describe('isConnected', () => {
    it('is false before connect, true when both live, false when either drops', async () => {
        const client = new RabbitClient('amqp://localhost/');
        expect(client.isConnected()).toBe(false);
        await client.connect();
        expect(client.isConnected()).toBe(true);
        must(managers[1]).isConnected.mockReturnValue(false); // consume side mid-reconnect
        expect(client.isConnected()).toBe(false);
    });
});

describe('publish', () => {
    it('declares the queue durable=true exactly once per queue (declare caching)', async () => {
        const { client, pubChannel } = await connectedClient();
        await client.publish('jobs', Buffer.from('a'));
        await client.publish('jobs', Buffer.from('b'));
        await client.publish('jobs', Buffer.from('c'));
        expect(pubChannel.addSetup).toHaveBeenCalledTimes(1);
        expect(pubChannel.fakeAmqplibChannel.assertQueue).toHaveBeenCalledTimes(1);
        expect(pubChannel.fakeAmqplibChannel.assertQueue).toHaveBeenCalledWith('jobs', {
            durable: true,
        });
        expect(pubChannel.sendToQueue).toHaveBeenCalledTimes(3);

        await client.publish('other', Buffer.from('d')); // new queue -> new declare
        expect(pubChannel.fakeAmqplibChannel.assertQueue).toHaveBeenCalledTimes(2);
    });

    it('publishes non-persistent by default and persistent when durable=true', async () => {
        const { client, pubChannel } = await connectedClient();
        await client.publish('q', Buffer.from('x'));
        expect(pubChannel.sendToQueue).toHaveBeenLastCalledWith('q', Buffer.from('x'), {
            persistent: false,
        });

        const durable = await connectedClient({ durable: true });
        await durable.client.publish('q', Buffer.from('x'));
        expect(durable.pubChannel.sendToQueue).toHaveBeenLastCalledWith('q', Buffer.from('x'), {
            persistent: true,
        });
    });
});

describe('publishMany', () => {
    it('pipelines confirms in batches of 1000', async () => {
        const { client, pubChannel } = await connectedClient();
        const pending: Array<(v: boolean) => void> = [];
        pubChannel.sendToQueue.mockImplementation(
            () => new Promise<boolean>((resolve) => pending.push(resolve)),
        );

        const bodies = Array.from({ length: 2500 }, (_, i) => Buffer.from(`m${i}`));
        const done = client.publishMany('bulk', bodies);

        await tick();
        expect(pubChannel.sendToQueue).toHaveBeenCalledTimes(1000); // batch 1 in flight

        pending.splice(0).forEach((resolve) => resolve(true)); // confirm batch 1
        await tick();
        expect(pubChannel.sendToQueue).toHaveBeenCalledTimes(2000); // batch 2 fired only after

        pending.splice(0).forEach((resolve) => resolve(true));
        await tick();
        expect(pubChannel.sendToQueue).toHaveBeenCalledTimes(2500); // final partial batch

        pending.splice(0).forEach((resolve) => resolve(true));
        await done;
    });

    it('applies one SHARED options object to every message (built once per call)', async () => {
        const { client, pubChannel } = await connectedClient();
        await client.publishMany(
            'q',
            [Buffer.from('a'), Buffer.from('b'), Buffer.from('c')],
            { correlationId: 'bulk-42', expiration: 1 },
        );
        const optionArgs = pubChannel.sendToQueue.mock.calls.map((call) => call[2]);
        expect(optionArgs).toHaveLength(3);
        for (const arg of optionArgs) {
            expect(arg).toBe(optionArgs[0]); // same object — hot path stays allocation-free
        }
        expect(optionArgs[0]).toEqual({
            persistent: false,
            correlationId: 'bulk-42',
            expiration: '1000',
        });
    });

    it('declares once even across publish and publishMany', async () => {
        const { client, pubChannel } = await connectedClient();
        await client.publish('q', Buffer.from('x'));
        await client.publishMany('q', [Buffer.from('y'), Buffer.from('z')]);
        expect(pubChannel.fakeAmqplibChannel.assertQueue).toHaveBeenCalledTimes(1);
    });
});

describe('publish options', () => {
    it('maps message properties straight to amqplib, converting expiration SECONDS → ms string', async () => {
        const { client, pubChannel } = await connectedClient();
        await client.publish('q', Buffer.from('x'), {
            persistent: true,
            headers: { 'x-retry-count': 3 },
            correlationId: 'corr-1',
            messageId: 'msg-1',
            contentType: 'application/json',
            expiration: 2.5,
            priority: 7,
        });
        expect(pubChannel.sendToQueue).toHaveBeenLastCalledWith('q', Buffer.from('x'), {
            persistent: true,
            headers: { 'x-retry-count': 3 },
            correlationId: 'corr-1',
            messageId: 'msg-1',
            contentType: 'application/json',
            expiration: '2500', // seconds in the API → string milliseconds on the wire
            priority: 7,
        });
    });

    it('omitted properties are not sent at all (bare persistent-only options object)', async () => {
        const { client, pubChannel } = await connectedClient();
        await client.publish('q', Buffer.from('x'), {});
        expect(pubChannel.sendToQueue).toHaveBeenLastCalledWith('q', Buffer.from('x'), {
            persistent: false,
        });
    });

    it('persistent falls back to the constructor durable and overrides it in BOTH directions', async () => {
        const durable = await connectedClient({ durable: true });
        await durable.client.publish('q', Buffer.from('x'));
        expect(durable.pubChannel.sendToQueue).toHaveBeenLastCalledWith(
            'q',
            Buffer.from('x'),
            { persistent: true },
        );
        await durable.client.publish('q', Buffer.from('x'), { persistent: false });
        expect(durable.pubChannel.sendToQueue).toHaveBeenLastCalledWith(
            'q',
            Buffer.from('x'),
            { persistent: false },
        );

        const transient = await connectedClient({ durable: false });
        await transient.client.publish('q', Buffer.from('x'), { persistent: true });
        expect(transient.pubChannel.sendToQueue).toHaveBeenLastCalledWith(
            'q',
            Buffer.from('x'),
            { persistent: true },
        );
    });
});

describe('consume', () => {
    it('applies per-consumer prefetch and declares the queue on the consume side', async () => {
        const { client, conChannel } = await connectedClient({ prefetch: 42 });
        await client.consume('jobs', async () => {});
        expect(conChannel.fakeAmqplibChannel.assertQueue).toHaveBeenCalledWith('jobs', {
            durable: true,
        });
        expect(must(conChannel.consumers[0]).options).toMatchObject({ prefetch: 42, noAck: false });
    });

    it('acks only AFTER the handler resolves', async () => {
        const { client, conChannel } = await connectedClient();
        let resolveHandler!: () => void;
        const gate = new Promise<void>((resolve) => (resolveHandler = resolve));
        await client.consume('jobs', () => gate);

        const msg = { content: Buffer.from('payload') };
        must(conChannel.consumers[0]).onMessage(msg);
        await tick();
        expect(conChannel.ack).not.toHaveBeenCalled(); // handler still running

        resolveHandler();
        await tick();
        expect(conChannel.ack).toHaveBeenCalledTimes(1);
        expect(conChannel.ack).toHaveBeenCalledWith(msg);
        expect(conChannel.nack).not.toHaveBeenCalled();
    });

    it('nacks with requeue=true when the handler rejects — that one message only', async () => {
        const { client, conChannel } = await connectedClient();
        await client.consume('jobs', async (body) => {
            if (body.toString() === 'bad') throw new Error('handler failed');
        });

        const good = { content: Buffer.from('good') };
        const bad = { content: Buffer.from('bad') };
        must(conChannel.consumers[0]).onMessage(bad);
        must(conChannel.consumers[0]).onMessage(good);
        await tick();

        expect(conChannel.nack).toHaveBeenCalledTimes(1);
        expect(conChannel.nack).toHaveBeenCalledWith(bad, false, true);
        expect(conChannel.ack).toHaveBeenCalledTimes(1);
        expect(conChannel.ack).toHaveBeenCalledWith(good);
    });

    it('handler receives the raw body Buffer', async () => {
        const { client, conChannel } = await connectedClient();
        const seen: Buffer[] = [];
        await client.consume('jobs', async (body) => {
            seen.push(body);
        });
        must(conChannel.consumers[0]).onMessage({ content: Buffer.from('hello') });
        await tick();
        expect(seen).toEqual([Buffer.from('hello')]);
    });

    it('cancel() cancels the consumer by tag and is idempotent', async () => {
        const { client, conChannel } = await connectedClient();
        const handle = await client.consume('jobs', async () => {});
        expect(handle.consumerTag).toBe(must(conChannel.consumers[0]).consumerTag);
        await handle.cancel();
        await handle.cancel();
        expect(conChannel.cancel).toHaveBeenCalledTimes(1);
        expect(conChannel.cancel).toHaveBeenCalledWith(handle.consumerTag);
    });

    it('per-consume prefetch overrides the constructor value; absent falls back to it', async () => {
        const { client, conChannel } = await connectedClient({ prefetch: 42 });
        await client.consume('overridden', async () => {}, { prefetch: 7 });
        await client.consume('defaulted', async () => {});
        expect(must(conChannel.consumers[0]).options).toMatchObject({
            prefetch: 7,
            noAck: false,
        });
        expect(must(conChannel.consumers[1]).options).toMatchObject({
            prefetch: 42,
            noAck: false,
        });
    });

    it('concurrent cancel() calls share ONE in-flight RPC; a failure rejects both and allows retry', async () => {
        const { client, conChannel } = await connectedClient();
        const handle = await client.consume('jobs', async () => {});

        let rejectRpc!: (err: Error) => void;
        conChannel.cancel.mockImplementationOnce(
            () => new Promise<never>((_, reject) => (rejectRpc = reject)),
        );
        const first = handle.cancel();
        const second = handle.cancel(); // issued while the first RPC is still in flight
        expect(conChannel.cancel).toHaveBeenCalledTimes(1); // ONE shared RPC — no double basic.cancel

        // The second caller must NOT have been told "success" already: when
        // the shared RPC fails, BOTH callers see the failure.
        const boom = new Error('broker refused the cancel');
        rejectRpc(boom);
        await expect(first).rejects.toBe(boom);
        await expect(second).rejects.toBe(boom);

        // The failure cleared the latch: a retry re-issues the RPC...
        await handle.cancel();
        expect(conChannel.cancel).toHaveBeenCalledTimes(2);
        // ...and after success the handle is a latched no-op again.
        await handle.cancel();
        expect(conChannel.cancel).toHaveBeenCalledTimes(2);
    });

    it('drops the ack when the channel reconnected mid-handler (stale delivery tag)', async () => {
        const { client, conChannel } = await connectedClient();
        let resolveHandler!: () => void;
        const gate = new Promise<void>((resolve) => (resolveHandler = resolve));
        await client.consume('jobs', () => gate);

        must(conChannel.consumers[0]).onMessage({ content: Buffer.from('x') });
        await tick();
        await conChannel.simulateReconnect(); // new channel, new epoch — the tag is stale

        resolveHandler();
        await tick();
        // Settling the stale tag on the new channel would be a broker 406
        // (or a silent mis-ack); the client must do NOTHING and let the
        // broker redeliver the unacked message.
        expect(conChannel.ack).not.toHaveBeenCalled();
        expect(conChannel.nack).not.toHaveBeenCalled();
    });

    it('drops the nack too when the handler rejects after a reconnect', async () => {
        const { client, conChannel } = await connectedClient();
        let rejectHandler!: (err: Error) => void;
        const gate = new Promise<void>((_, reject) => (rejectHandler = reject));
        await client.consume('jobs', () => gate);

        must(conChannel.consumers[0]).onMessage({ content: Buffer.from('x') });
        await conChannel.simulateReconnect();
        rejectHandler(new Error('handler failed'));
        await tick();
        expect(conChannel.nack).not.toHaveBeenCalled();
        expect(conChannel.ack).not.toHaveBeenCalled();
    });

    it('still acks when delivery and settle happen in the SAME post-reconnect epoch', async () => {
        const { client, conChannel } = await connectedClient();
        let resolveHandler!: () => void;
        const gate = new Promise<void>((resolve) => (resolveHandler = resolve));
        await client.consume('jobs', () => gate);

        await conChannel.simulateReconnect(); // a reconnect happened BEFORE the delivery...
        const msg = { content: Buffer.from('fresh') };
        must(conChannel.consumers[0]).onMessage(msg); // ...so this tag belongs to the new epoch
        resolveHandler();
        await tick();
        expect(conChannel.ack).toHaveBeenCalledTimes(1);
        expect(conChannel.ack).toHaveBeenCalledWith(msg);
    });

    it('acks deliveries dispatched DURING consumer re-establishment, before the connect event (wedge regression)', async () => {
        // The real stack re-establishes consumers BEFORE emitting 'connect',
        // and amqplib dispatches deliveries synchronously from the same TCP
        // burst as the consume-ok. A 'connect'-event-based epoch bump ran
        // AFTER those deliveries: they captured the stale epoch, every ack
        // in the prefetch window was dropped, and the consumer wedged
        // (prefetch exhausted, nothing ever acked). The epoch must be
        // bumped by a SETUP function, which runs before any delivery.
        const { client, conChannel } = await connectedClient();
        let resolveHandler!: () => void;
        const gate = new Promise<void>((resolve) => (resolveHandler = resolve));
        await client.consume('jobs', () => gate);

        const msg = { content: Buffer.from('first-after-reconnect') };
        await conChannel.simulateReconnect(() => {
            // Earliest possible post-reconnect delivery: after setups and
            // consumer re-establishment, BEFORE the 'connect' event fires.
            must(conChannel.consumers[0]).onMessage(msg);
        });
        resolveHandler();
        await tick();
        expect(conChannel.ack).toHaveBeenCalledTimes(1);
        expect(conChannel.ack).toHaveBeenCalledWith(msg);
    });

    it('aborting the AbortSignal cancels the consumer', async () => {
        const { client, conChannel } = await connectedClient();
        const controller = new AbortController();
        await client.consume('jobs', async () => {}, { signal: controller.signal });
        expect(conChannel.cancel).not.toHaveBeenCalled();
        controller.abort();
        await tick();
        expect(conChannel.cancel).toHaveBeenCalledTimes(1);
    });
});

describe('deleteQueue', () => {
    it('deletes the queue and clears the declare caches on both sides', async () => {
        const { client, pubChannel, conChannel } = await connectedClient();
        await client.publish('jobs', Buffer.from('x'));
        await client.consume('jobs', async () => {});
        expect(pubChannel.setups).toHaveLength(1);
        expect(conChannel.setups).toHaveLength(2); // epoch-bump setup + declare setup

        await client.deleteQueue('jobs');
        expect(pubChannel.deleteQueue).toHaveBeenCalledWith('jobs');
        // declare setups removed, so a reconnect can no longer resurrect it
        expect(pubChannel.removeSetup).toHaveBeenCalledTimes(1);
        expect(conChannel.removeSetup).toHaveBeenCalledTimes(1);
        expect(pubChannel.setups).toHaveLength(0);
        expect(conChannel.setups).toHaveLength(1); // only the epoch-bump setup remains

        // cache cleared -> the next publish re-declares
        await client.publish('jobs', Buffer.from('y'));
        expect(pubChannel.fakeAmqplibChannel.assertQueue).toHaveBeenCalledTimes(2);
    });

    it('cancels active consumers on the queue BEFORE deleting it', async () => {
        const { client, pubChannel, conChannel } = await connectedClient();
        const handleA = await client.consume('jobs', async () => {});
        const handleB = await client.consume('jobs', async () => {});
        await client.consume('other', async () => {}); // different queue: untouched

        await client.deleteQueue('jobs');

        // Both 'jobs' consumers cancelled, the 'other' one left alone.
        expect(conChannel.cancel).toHaveBeenCalledTimes(2);
        expect(conChannel.cancel).toHaveBeenCalledWith(handleA.consumerTag);
        expect(conChannel.cancel).toHaveBeenCalledWith(handleB.consumerTag);
        // Order: every cancel must hit the broker before queue.delete does —
        // a consumer surviving the delete would be re-established against a
        // 404 forever.
        const deleteOrder = must(pubChannel.deleteQueue.mock.invocationCallOrder[0]);
        for (const cancelOrder of conChannel.cancel.mock.invocationCallOrder) {
            expect(cancelOrder).toBeLessThan(deleteOrder);
        }

        // The handles were cancelled for the caller too: cancel() is now a no-op.
        await handleA.cancel();
        expect(conChannel.cancel).toHaveBeenCalledTimes(2);
    });

    it('proceeds with removeSetup + delete even when a consumer cancel RPC fails (best-effort)', async () => {
        // ChannelWrapper.cancel() removes the consumer from its registry
        // SYNCHRONOUSLY, before the RPC — a failed cancel can never be
        // resurrected on reconnect, so it must not abort the delete (which
        // previously left queue AND declare setups fully intact).
        const { client, pubChannel, conChannel } = await connectedClient();
        await client.publish('jobs', Buffer.from('x'));
        await client.consume('jobs', async () => {});
        conChannel.cancel.mockRejectedValueOnce(new Error('cancel RPC failed'));

        await client.deleteQueue('jobs'); // must NOT reject
        expect(conChannel.cancel).toHaveBeenCalledTimes(1);
        expect(pubChannel.removeSetup).toHaveBeenCalledTimes(1);
        expect(conChannel.removeSetup).toHaveBeenCalledTimes(1);
        expect(pubChannel.deleteQueue).toHaveBeenCalledWith('jobs');

        // The failed handle was deregistered anyway: a second delete finds
        // no consumers left to cancel.
        await client.deleteQueue('jobs');
        expect(conChannel.cancel).toHaveBeenCalledTimes(1);
    });
});

describe('close', () => {
    it('closes both connections and drops state', async () => {
        const { client, pubManager, conManager } = await connectedClient();
        await client.close();
        expect(pubManager.close).toHaveBeenCalledTimes(1);
        expect(conManager.close).toHaveBeenCalledTimes(1);
        expect(client.isConnected()).toBe(false);
        await expect(client.publish('q', Buffer.from('x'))).rejects.toThrow(/connect\(\)/);
    });

    it('cancels every registered consumer handle best-effort before closing the connections', async () => {
        const { client, conChannel, pubManager, conManager } = await connectedClient();
        const handleA = await client.consume('jobs', async () => {});
        const handleB = await client.consume('other', async () => {});
        // One cancel failing must not abort close() — the connections are
        // going away regardless.
        conChannel.cancel.mockRejectedValueOnce(new Error('cancel failed'));

        await client.close(); // must not reject

        expect(conChannel.cancel).toHaveBeenCalledTimes(2);
        expect(conChannel.cancel).toHaveBeenCalledWith(handleA.consumerTag);
        expect(conChannel.cancel).toHaveBeenCalledWith(handleB.consumerTag);
        expect(pubManager.close).toHaveBeenCalledTimes(1);
        expect(conManager.close).toHaveBeenCalledTimes(1);
        // A handle whose cancel succeeded is now a resolved no-op for its
        // holder — close() invalidated it cleanly.
        await handleB.cancel();
        expect(conChannel.cancel).toHaveBeenCalledTimes(2);
    });
});
