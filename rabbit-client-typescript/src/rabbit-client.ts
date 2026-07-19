/**
 * Minimal RabbitMQ client for Node apps: amqplib + amqp-connection-manager,
 * zero hand-rolled AMQP logic.
 *
 * TypeScript counterpart of the canonical Python client
 * (rabbit-client-python/src/rabbit_client). Everything subtle is delegated to
 * amqp-connection-manager, which is maintained for you:
 *
 * - Reconnect: `amqp-connection-manager` re-establishes connections after a
 *   broker restart or network blip (the equivalent of aio-pika's
 *   `connect_robust`). Every queue declare is registered as a ChannelWrapper
 *   *setup function*, so queues are re-declared automatically on reconnect —
 *   the declare cache stays valid across outages.
 * - Consumer resurrection: ChannelWrapper.consume() re-establishes consumers
 *   on reconnect AND when the broker cancels them (a broker-sent Basic.Cancel
 *   surfaces in amqplib as a `null` delivery; amqp-connection-manager reacts
 *   by re-consuming immediately). This is why there is no
 *   `ConsumerCancelledError` here: the Python client needs a watchdog because
 *   aio-pika silently drops broker-cancelled consumers until the next
 *   reconnect, but the JS stack resurrects them for us. `consume()` therefore
 *   genuinely runs until *you* cancel it.
 * - Delivery safety: each message is acked only AFTER your handler resolves;
 *   if the handler rejects, that one message is nacked with requeue. Per-
 *   message acks are inherently safe under concurrency — no batch ack can
 *   ever cover an unfinished handler.
 * - Concurrency: deliveries are dispatched without awaiting the previous
 *   handler, so up to `prefetch` handlers run concurrently. For a DB-bound
 *   consumer this, not client speed, decides real throughput.
 *
 * Built for many queues:
 *
 * - Publishing and consuming use SEPARATE connections, so broker flow control
 *   on a busy publisher can never stall your consumers.
 * - Queue declares are cached (once per queue per side) and re-run on
 *   reconnect by the ChannelWrapper setup machinery.
 * - `consume()` can be called once per queue on one client — consumers are
 *   cheap, multiplexed on the consume connection. Prefetch applies per
 *   consumer: with many busy queues, size it accordingly (e.g. prefetch=50).
 *
 * Usage:
 *     const client = new RabbitClient("amqp://user:pass@host/");
 *     await client.connect();
 *     await client.publishMany("jobs", Array(1000).fill(Buffer.from("payload")));
 *
 *     const consumer = await client.consume("jobs", async (body) => {
 *         await db.insert(body);   // your async work; reject/throw to requeue
 *     });
 *     // ... later:
 *     await consumer.cancel();
 */
import { connect } from 'amqp-connection-manager';
import type {
    AmqpConnectionManager,
    ChannelWrapper,
    SetupFunc,
} from 'amqp-connection-manager';
import type { Channel, ConsumeMessage } from 'amqplib';

/** Confirm-pipeline depth for publishMany; measured knee for bulk publishing. */
const PIPELINE = 1000;

export interface RabbitClientOptions {
    /**
     * Per-consumer prefetch (basic.qos with global=false). Deliveries overlap
     * up to this many concurrent handler invocations per consumer.
     * Default 200.
     */
    prefetch?: number;
    /**
     * When true, messages are published persistent (delivery mode 2).
     * NOTE: this governs MESSAGE persistence only — queues are always
     * declared durable, because RabbitMQ 4 denies transient non-exclusive
     * queues. Default false.
     */
    durable?: boolean;
}

export interface ConnectOptions {
    /**
     * Optional cap on how long connect() waits for the initial connection.
     * Without it, amqp-connection-manager retries until the broker is
     * reachable (its normal robust behavior).
     */
    timeoutMs?: number;
}

export interface ConsumeOptions {
    /** Abort this signal to cancel the consumer (same effect as `cancel()`). */
    signal?: AbortSignal;
}

/** Handle returned by consume(); the consumer runs until cancelled. */
export interface ConsumerHandle {
    readonly consumerTag: string;
    /** Stop consuming. Idempotent: subsequent calls are no-ops. */
    cancel(): Promise<void>;
}

export type MessageHandler = (body: Buffer) => Promise<void>;

export class RabbitClient {
    private readonly url: string;
    private readonly prefetch: number;
    private readonly durable: boolean;

    private pubConn: AmqpConnectionManager | null = null;
    private conConn: AmqpConnectionManager | null = null;
    private pubChannel: ChannelWrapper | null = null;
    private conChannel: ChannelWrapper | null = null;

    // queue name -> the setup function that declares it, so deleteQueue can
    // removeSetup() it (otherwise the reconnect machinery would resurrect
    // the queue after a broker restart).
    private declaredPub = new Map<string, SetupFunc>();
    private declaredCon = new Map<string, SetupFunc>();

    constructor(amqpUrl: string, options: RabbitClientOptions = {}) {
        this.url = amqpUrl;
        this.prefetch = options.prefetch ?? 200;
        this.durable = options.durable ?? false;
    }

    /**
     * Open the two connections (publish + consume) and their channels.
     * The publish channel is a confirm channel (publisher confirms); the
     * consume channel is a plain channel with per-consumer prefetch.
     */
    async connect(options: ConnectOptions = {}): Promise<void> {
        const pub = connect(this.url);
        const con = connect(this.url);
        const timeout = options.timeoutMs;
        const results = await Promise.allSettled([
            pub.connect(timeout !== undefined ? { timeout } : undefined),
            con.connect(timeout !== undefined ? { timeout } : undefined),
        ]);
        const failure = results.find(
            (r): r is PromiseRejectedResult => r.status === 'rejected',
        );
        if (failure) {
            // One side can succeed while the other fails (connection limit,
            // broker mid-restart). Close BOTH or the survivor leaks —
            // reconnect machinery and all — on every connect retry.
            await Promise.allSettled([pub.close(), con.close()]);
            throw failure.reason;
        }
        this.pubConn = pub;
        this.conConn = con;
        // json:false — bodies are Buffers; confirm:true — sendToQueue resolves
        // only once the broker confirms the publish.
        this.pubChannel = pub.createChannel({ json: false, confirm: true });
        this.conChannel = con.createChannel({ json: false, confirm: false });
        // ChannelWrapper emits 'error' for transient setup/re-consume failures
        // that its own reconnect machinery goes on to recover from. Without a
        // listener, an EventEmitter 'error' would crash the process.
        this.pubChannel.on('error', () => {});
        this.conChannel.on('error', () => {});
        await Promise.all([
            this.pubChannel.waitForConnect(),
            this.conChannel.waitForConnect(),
        ]);
        this.declaredPub.clear();
        this.declaredCon.clear();
    }

    /** Close both connections (and with them all channels and consumers). */
    async close(): Promise<void> {
        const conns = [this.pubConn, this.conConn];
        this.pubConn = this.conConn = null;
        this.pubChannel = this.conChannel = null;
        this.declaredPub.clear();
        this.declaredCon.clear();
        await Promise.all(conns.filter((c) => c !== null).map((c) => c.close()));
    }

    /**
     * True only when BOTH connections are live RIGHT NOW.
     * A manager mid-reconnect after a broker outage reports false here even
     * though it is not "closed" — same semantics as the Python client.
     */
    isConnected(): boolean {
        return (
            this.pubConn !== null &&
            this.conConn !== null &&
            this.pubConn.isConnected() &&
            this.conConn.isConnected()
        );
    }

    /** Delete a queue and drop its cached declares (both sides). */
    async deleteQueue(queue: string): Promise<void> {
        const pubChannel = this.requirePubChannel();
        // Remove the declare setups FIRST, so a reconnect racing this delete
        // cannot re-create the queue we are about to remove.
        const pubSetup = this.declaredPub.get(queue);
        if (pubSetup) {
            this.declaredPub.delete(queue);
            await pubChannel.removeSetup(pubSetup);
        }
        const conSetup = this.declaredCon.get(queue);
        if (conSetup && this.conChannel) {
            this.declaredCon.delete(queue);
            await this.conChannel.removeSetup(conSetup);
        }
        await pubChannel.deleteQueue(queue);
    }

    /** Publish one message to `queue` via the default exchange, confirmed. */
    async publish(queue: string, body: Buffer): Promise<void> {
        const channel = this.requirePubChannel();
        await this.declareForPublish(queue);
        await channel.sendToQueue(queue, body, { persistent: this.durable });
    }

    /**
     * Publish many messages with pipelined confirms: fire a batch of
     * `PIPELINE` (1000) publishes, await all their confirms, then the next
     * batch — the measured sweet spot for bulk publishing.
     */
    async publishMany(queue: string, bodies: Buffer[]): Promise<void> {
        const channel = this.requirePubChannel();
        await this.declareForPublish(queue);
        for (let i = 0; i < bodies.length; i += PIPELINE) {
            await Promise.all(
                bodies
                    .slice(i, i + PIPELINE)
                    .map((b) => channel.sendToQueue(queue, b, { persistent: this.durable })),
            );
        }
    }

    /**
     * Consume `queue`, invoking `handler(body)` for each message. Runs until
     * cancelled via the returned handle's `cancel()` (or the AbortSignal in
     * `options.signal` — both do the same thing; the handle is returned
     * either way).
     *
     * - ack is sent only AFTER the handler resolves;
     * - a rejected handler nacks that ONE message with requeue=true;
     * - deliveries overlap up to `prefetch` concurrent handlers (per
     *   consumer: qos is applied with global=false for each consume call);
     * - reconnects and broker-side cancels are survived automatically:
     *   amqp-connection-manager re-declares the queue (setup function) and
     *   re-establishes the consumer. No watchdog needed — see module docs.
     */
    async consume(
        queue: string,
        handler: MessageHandler,
        options: ConsumeOptions = {},
    ): Promise<ConsumerHandle> {
        const channel = this.requireConChannel();
        await this.declareForConsume(queue);

        const onMessage = (msg: ConsumeMessage): void => {
            // Fire-and-track, do NOT await: awaiting would serialize handlers
            // and waste the prefetch window. Ack/nack decisions are per
            // message, so concurrency is safe.
            handler(msg.content).then(
                () => channel.ack(msg),
                () => channel.nack(msg, false, true),
            );
        };

        const { consumerTag } = await channel.consume(queue, onMessage, {
            // Per-consumer prefetch: amqp-connection-manager issues
            // basic.qos(global=false) before each consume, and re-issues it
            // when the consumer is re-established after a reconnect.
            prefetch: this.prefetch,
            noAck: false,
        });

        let cancelled = false;
        const cancel = async (): Promise<void> => {
            if (cancelled) return;
            cancelled = true;
            await channel.cancel(consumerTag);
        };
        if (options.signal) {
            if (options.signal.aborted) {
                await cancel();
            } else {
                options.signal.addEventListener(
                    'abort',
                    () => {
                        void cancel().catch(() => {});
                    },
                    { once: true },
                );
            }
        }
        return { consumerTag, cancel };
    }

    // ------------------------------------------------------------------ //

    // Queues are always durable: RabbitMQ 4 denies transient non-exclusive
    // queues. The `durable` option governs MESSAGE persistence instead.
    // Declares are registered as ChannelWrapper setup functions: they run
    // once now, are cached here (never re-sent per publish), and re-run
    // automatically on every reconnect — so the cache stays valid.
    private async declareForPublish(queue: string): Promise<void> {
        if (this.declaredPub.has(queue)) return;
        const setup: SetupFunc = async (ch: Channel) => {
            await ch.assertQueue(queue, { durable: true });
        };
        this.declaredPub.set(queue, setup);
        await this.requirePubChannel().addSetup(setup);
    }

    private async declareForConsume(queue: string): Promise<void> {
        if (this.declaredCon.has(queue)) return;
        const setup: SetupFunc = async (ch: Channel) => {
            await ch.assertQueue(queue, { durable: true });
        };
        this.declaredCon.set(queue, setup);
        await this.requireConChannel().addSetup(setup);
    }

    private requirePubChannel(): ChannelWrapper {
        if (!this.pubChannel) {
            throw new Error('RabbitClient is not connected — call connect() first');
        }
        return this.pubChannel;
    }

    private requireConChannel(): ChannelWrapper {
        if (!this.conChannel) {
            throw new Error('RabbitClient is not connected — call connect() first');
        }
        return this.conChannel;
    }
}
