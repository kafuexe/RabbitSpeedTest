/**
 * Unit tests for SimpleRabbit. amqp-connection-manager is fully mocked —
 * no broker needed. The mock mimics the parts of the real API the client
 * relies on: connect(), createChannel(), addSetup/removeSetup (setup
 * functions run immediately against a fake amqplib channel), sendToQueue
 * with confirm semantics, consume/cancel, ack/nack, deleteQueue.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { connect } from 'amqp-connection-manager';
import { SimpleRabbit } from '../src';

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
    let tagCounter = 0;
    return {
        fakeAmqplibChannel,
        setups,
        consumers,
        addSetup: vi.fn(async (fn: (ch: unknown) => Promise<void>) => {
            setups.push(fn);
            await fn(fakeAmqplibChannel);
        }),
        removeSetup: vi.fn(async (fn: (ch: unknown) => Promise<void>) => {
            const i = setups.indexOf(fn);
            if (i >= 0) setups.splice(i, 1);
        }),
        sendToQueue: vi.fn(async () => true),
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
        on: vi.fn(),
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

async function connectedClient(opts: { prefetch?: number; durable?: boolean } = {}) {
    const client = new SimpleRabbit('amqp://guest:guest@localhost/', opts);
    await client.connect();
    const [pubManager, conManager] = managers.slice(-2);
    return {
        client,
        pubManager,
        conManager,
        pubChannel: pubManager.channels[0],
        conChannel: conManager.channels[0],
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
        expect(pubManager.createChannel.mock.calls[0][0]).toMatchObject({
            confirm: true,
            json: false,
        });
        expect(conManager.createChannel.mock.calls[0][0]).toMatchObject({
            confirm: false,
            json: false,
        });
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

    it('closes the surviving connection when only one side fails', async () => {
        const boom = new Error('connection limit reached');
        const failing = makeFakeManager();
        failing.connect.mockRejectedValue(boom);
        const surviving = makeFakeManager();
        connectMock
            .mockImplementationOnce(() => failing as unknown as RealManager)
            .mockImplementationOnce(() => surviving as unknown as RealManager);

        const client = new SimpleRabbit('amqp://localhost/');
        await expect(client.connect()).rejects.toBe(boom);
        expect(failing.close).toHaveBeenCalled();
        expect(surviving.close).toHaveBeenCalled(); // survivor must not leak
        expect(client.isConnected()).toBe(false);
    });
});

describe('isConnected', () => {
    it('is false before connect, true when both live, false when either drops', async () => {
        const client = new SimpleRabbit('amqp://localhost/');
        expect(client.isConnected()).toBe(false);
        await client.connect();
        expect(client.isConnected()).toBe(true);
        managers[1].isConnected.mockReturnValue(false); // consume side mid-reconnect
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

    it('declares once even across publish and publishMany', async () => {
        const { client, pubChannel } = await connectedClient();
        await client.publish('q', Buffer.from('x'));
        await client.publishMany('q', [Buffer.from('y'), Buffer.from('z')]);
        expect(pubChannel.fakeAmqplibChannel.assertQueue).toHaveBeenCalledTimes(1);
    });
});

describe('consume', () => {
    it('applies per-consumer prefetch and declares the queue on the consume side', async () => {
        const { client, conChannel } = await connectedClient({ prefetch: 42 });
        await client.consume('jobs', async () => {});
        expect(conChannel.fakeAmqplibChannel.assertQueue).toHaveBeenCalledWith('jobs', {
            durable: true,
        });
        expect(conChannel.consumers[0].options).toMatchObject({ prefetch: 42, noAck: false });
    });

    it('acks only AFTER the handler resolves', async () => {
        const { client, conChannel } = await connectedClient();
        let resolveHandler!: () => void;
        const gate = new Promise<void>((resolve) => (resolveHandler = resolve));
        await client.consume('jobs', () => gate);

        const msg = { content: Buffer.from('payload') };
        conChannel.consumers[0].onMessage(msg);
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
        conChannel.consumers[0].onMessage(bad);
        conChannel.consumers[0].onMessage(good);
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
        conChannel.consumers[0].onMessage({ content: Buffer.from('hello') });
        await tick();
        expect(seen).toEqual([Buffer.from('hello')]);
    });

    it('cancel() cancels the consumer by tag and is idempotent', async () => {
        const { client, conChannel } = await connectedClient();
        const handle = await client.consume('jobs', async () => {});
        expect(handle.consumerTag).toBe(conChannel.consumers[0].consumerTag);
        await handle.cancel();
        await handle.cancel();
        expect(conChannel.cancel).toHaveBeenCalledTimes(1);
        expect(conChannel.cancel).toHaveBeenCalledWith(handle.consumerTag);
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
        expect(conChannel.setups).toHaveLength(1);

        await client.deleteQueue('jobs');
        expect(pubChannel.deleteQueue).toHaveBeenCalledWith('jobs');
        // declare setups removed, so a reconnect can no longer resurrect it
        expect(pubChannel.removeSetup).toHaveBeenCalledTimes(1);
        expect(conChannel.removeSetup).toHaveBeenCalledTimes(1);
        expect(pubChannel.setups).toHaveLength(0);
        expect(conChannel.setups).toHaveLength(0);

        // cache cleared -> the next publish re-declares
        await client.publish('jobs', Buffer.from('y'));
        expect(pubChannel.fakeAmqplibChannel.assertQueue).toHaveBeenCalledTimes(2);
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
});
