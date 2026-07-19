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
import type { Channel, ConsumeMessage, Options } from 'amqplib';

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
    /**
     * Per-consume prefetch override (basic.qos with global=false for THIS
     * consumer only). Falls back to the constructor `prefetch` (default 200).
     * Passed straight into the ChannelWrapper consume options, exactly like
     * the constructor value — re-issued automatically on reconnect.
     */
    prefetch?: number;
}

/**
 * Optional per-publish message properties. Every field maps STRAIGHT to the
 * corresponding amqplib publish option — no hand-rolled logic — except
 * `expiration`, which is given in SECONDS here (mirroring the Python client)
 * and converted to the string-milliseconds form amqplib expects.
 */
export interface PublishOptions {
    /** Message persistence (delivery mode 2). Overrides the constructor `durable`. */
    persistent?: boolean;
    /** Application headers. */
    headers?: Record<string, unknown>;
    /** AMQP `correlation-id` property. */
    correlationId?: string;
    /** AMQP `message-id` property. */
    messageId?: string;
    /** AMQP `content-type` property (e.g. "application/json"). */
    contentType?: string;
    /**
     * Per-message TTL in SECONDS (matches the Python client's `expiration`).
     * Converted to the string milliseconds amqplib expects on the wire.
     */
    expiration?: number;
    /** Message priority (only meaningful on priority queues). */
    priority?: number;
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

    // Live consumers per queue, so deleteQueue() can cancel them BEFORE the
    // queue disappears (a consumer left on a deleted queue would be endlessly
    // re-established by the reconnect machinery against a 404).
    private activeConsumers = new Map<string, Set<ConsumerHandle>>();

    // Reconnect epoch of the consume channel, bumped by a ChannelWrapper
    // SETUP FUNCTION on every (re)connection. A delivery captures the epoch
    // it arrived in, and its eventual ack/nack is sent ONLY if the epoch is
    // unchanged — delivery tags are per-channel, so settling a pre-reconnect
    // tag on the new channel is a protocol error (406) at best and a silent
    // mis-ack of an unrelated message at worst.
    //
    // Why a setup function and NOT the 'connect' event: ChannelWrapper
    // re-establishes consumers BEFORE emitting 'connect' (setups → consumers
    // → emit), and amqplib dispatches deliveries synchronously from the same
    // TCP burst as consume-ok. An event-based bump therefore ran AFTER the
    // first post-reconnect deliveries, which captured the stale epoch — every
    // ack in the prefetch window was dropped and the consumer wedged with a
    // full window of unacked messages. Setups run strictly before consumer
    // re-establishment, so the epoch is always current before the first
    // possible delivery. Monotonic across connect()/close() cycles.
    private conEpoch = 0;

    constructor(amqpUrl: string, options: RabbitClientOptions = {}) {
        this.url = amqpUrl;
        this.prefetch = options.prefetch ?? 200;
        this.durable = options.durable ?? false;
    }

    /**
     * Open the two connections (publish + consume) and their channels.
     * The publish channel is a confirm channel (publisher confirms); the
     * consume channel is a plain channel with per-consumer prefetch.
     *
     * Safe to call again after a failed or stale connection: if a previous
     * connect() left managers behind, they are torn down via close() first —
     * otherwise the abandoned managers would keep reconnecting (and their
     * still-registered consumers would keep consuming) forever in parallel.
     */
    async connect(options: ConnectOptions = {}): Promise<void> {
        if (this.pubConn !== null || this.conConn !== null) {
            await this.close();
        }
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
        // Epoch bump as a channel SETUP: runs on every (re)connection
        // STRICTLY BEFORE consumers are re-established, so every delivery —
        // including one dispatched synchronously with the consume-ok, before
        // the 'connect' event ever fires — captures the fresh epoch (see
        // conEpoch). Registered here, before any consume() can run, so it
        // always sorts ahead of consumer re-establishment. Note that a
        // broker-side Basic.Cancel makes the wrapper re-consume on the SAME
        // channel without re-running setups — correctly no bump there, since
        // the channel (and thus every outstanding delivery tag) stays valid.
        await this.conChannel.addSetup(async () => {
            this.conEpoch += 1;
        });
        await Promise.all([
            this.pubChannel.waitForConnect(),
            this.conChannel.waitForConnect(),
        ]);
        this.declaredPub.clear();
        this.declaredCon.clear();
    }

    /**
     * Close both connections (and with them all channels and consumers).
     *
     * Every registered consumer handle is cancelled FIRST, best-effort
     * (failures ignored — the connections are going away anyway), so handles
     * still held by callers become resolved no-ops instead of silently dead
     * references. close() therefore INVALIDATES all outstanding handles;
     * this is also what makes connect() reentrancy (which calls close())
     * a clean teardown of the previous connection's consumers.
     */
    async close(): Promise<void> {
        const handles = [...this.activeConsumers.values()].flatMap((set) => [...set]);
        await Promise.allSettled(handles.map((h) => h.cancel()));
        const conns = [this.pubConn, this.conConn];
        this.pubConn = this.conConn = null;
        this.pubChannel = this.conChannel = null;
        this.declaredPub.clear();
        this.declaredCon.clear();
        this.activeConsumers.clear();
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

    /**
     * Delete a queue and drop its cached declares (both sides).
     *
     * Active consumers on the queue are cancelled FIRST: a consumer left
     * registered on a deleted queue would be re-established forever by the
     * reconnect machinery (broker Basic.Cancel -> re-consume -> 404 closes
     * the channel -> reconnect re-runs ALL consumers), starving every other
     * consumer on the channel. Their handles' cancel() becomes a no-op.
     *
     * The cancels are BEST-EFFORT: a failed cancel RPC does not abort the
     * delete. This is safe because ChannelWrapper.cancel() removes the
     * consumer from its internal registry SYNCHRONOUSLY, before the RPC —
     * so even a cancel whose RPC failed can never be resurrected on
     * reconnect. The handles are deregistered here regardless of RPC
     * outcome, then the declare setups are removed and the queue deleted.
     */
    async deleteQueue(queue: string): Promise<void> {
        const pubChannel = this.requirePubChannel();
        // Cancel live consumers before the queue disappears — best-effort:
        // proceed with the delete even if a cancel RPC fails (see doc above).
        const handles = this.activeConsumers.get(queue);
        if (handles) {
            await Promise.allSettled([...handles].map((h) => h.cancel()));
            // Successful cancels deregistered themselves; drop any that
            // failed too — their consumers are already gone from the
            // wrapper's registry and must not pin the queue's entry.
            this.activeConsumers.delete(queue);
        }
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

    /**
     * Publish one message to `queue` via the default exchange, confirmed.
     * Optional per-message properties map straight to amqplib (see
     * {@link PublishOptions}); `persistent` defaults to the constructor
     * `durable`.
     */
    async publish(queue: string, body: Buffer, options: PublishOptions = {}): Promise<void> {
        const channel = this.requirePubChannel();
        await this.declareForPublish(queue);
        await channel.sendToQueue(queue, body, this.buildPublishOptions(options));
    }

    /**
     * Publish many messages with pipelined confirms: fire a batch of
     * `PIPELINE` (1000) publishes, await all their confirms, then the next
     * batch — the measured sweet spot for bulk publishing.
     *
     * `options` applies identically to EVERY message; the amqplib options
     * object is built once per call (not per message) to keep the hot path
     * allocation-free.
     */
    async publishMany(
        queue: string,
        bodies: Buffer[],
        options: PublishOptions = {},
    ): Promise<void> {
        const channel = this.requirePubChannel();
        await this.declareForPublish(queue);
        const publishOptions = this.buildPublishOptions(options);
        for (let i = 0; i < bodies.length; i += PIPELINE) {
            await Promise.all(
                bodies
                    .slice(i, i + PIPELINE)
                    .map((b) => channel.sendToQueue(queue, b, publishOptions)),
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
            // Capture the reconnect epoch this delivery belongs to. If the
            // channel reconnects before the handler settles, the delivery
            // tag is stale: forwarding it to the NEW channel is a broker
            // 406 (killing the whole consume connection) or, worse, a
            // silent ack of an unrelated same-numbered in-flight message.
            // Dropping the settle is the correct at-least-once behavior —
            // the broker redelivers the unacked message after the reconnect.
            const epoch = this.conEpoch;
            // Fire-and-track, do NOT await: awaiting would serialize handlers
            // and waste the prefetch window. Ack/nack decisions are per
            // message, so concurrency is safe.
            handler(msg.content).then(
                () => this.settleIfSameEpoch(epoch, () => channel.ack(msg)),
                () => this.settleIfSameEpoch(epoch, () => channel.nack(msg, false, true)),
            );
        };

        const { consumerTag } = await channel.consume(queue, onMessage, {
            // Per-consumer prefetch: amqp-connection-manager issues
            // basic.qos(global=false) before each consume, and re-issues it
            // when the consumer is re-established after a reconnect. The
            // per-consume override falls back to the constructor value.
            prefetch: options.prefetch ?? this.prefetch,
            noAck: false,
        });

        // Idempotent cancel: ALL callers — concurrent or subsequent — share
        // the same in-flight cancellation promise, so nobody can be told
        // "done" while the RPC is still pending (or after it failed). Only
        // a FAILED RPC clears the latch, so a retry re-issues the cancel.
        let cancelPromise: Promise<void> | null = null;
        const cancel = (): Promise<void> => {
            cancelPromise ??= (async () => {
                try {
                    await channel.cancel(consumerTag);
                } catch (err) {
                    cancelPromise = null; // failed at the broker — allow a retry
                    throw err;
                }
                this.deregisterConsumer(queue, handle);
            })();
            return cancelPromise;
        };
        const handle: ConsumerHandle = { consumerTag, cancel };
        this.registerConsumer(queue, handle);
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
        return handle;
    }

    // ------------------------------------------------------------------ //

    /**
     * Run a settle action (ack/nack) ONLY if the consume channel has not
     * reconnected since `epoch` was captured at delivery time. THE single
     * place the reconnect-epoch guard lives: a stale delivery tag forwarded
     * to the new channel would be a broker 406 (killing the whole consume
     * connection) or, worse, a silent ack of an unrelated same-numbered
     * in-flight message. Dropping the settle is the correct at-least-once
     * behavior — the broker redelivers the unacked message after reconnect.
     */
    private settleIfSameEpoch(epoch: number, settle: () => void): void {
        if (epoch === this.conEpoch) settle();
    }

    /**
     * Map {@link PublishOptions} to the amqplib publish options object.
     * Pure passthrough except: `persistent` falls back to the constructor
     * `durable`, and `expiration` is converted from SECONDS to the string
     * milliseconds amqplib expects. Called once per publish/publishMany
     * call so the bulk hot path stays allocation-free.
     */
    private buildPublishOptions(options: PublishOptions): Options.Publish {
        const out: Options.Publish = {
            persistent: options.persistent ?? this.durable,
        };
        if (options.headers !== undefined) out.headers = options.headers;
        if (options.correlationId !== undefined) out.correlationId = options.correlationId;
        if (options.messageId !== undefined) out.messageId = options.messageId;
        if (options.contentType !== undefined) out.contentType = options.contentType;
        if (options.expiration !== undefined) {
            out.expiration = String(Math.round(options.expiration * 1000));
        }
        if (options.priority !== undefined) out.priority = options.priority;
        return out;
    }

    private registerConsumer(queue: string, handle: ConsumerHandle): void {
        let handles = this.activeConsumers.get(queue);
        if (!handles) {
            handles = new Set();
            this.activeConsumers.set(queue, handles);
        }
        handles.add(handle);
    }

    private deregisterConsumer(queue: string, handle: ConsumerHandle): void {
        const handles = this.activeConsumers.get(queue);
        if (!handles) return;
        handles.delete(handle);
        if (handles.size === 0) this.activeConsumers.delete(queue);
    }

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
