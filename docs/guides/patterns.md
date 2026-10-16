# Patterns

## Transactional outbox

Publishing directly after a database commit loses messages when the process dies in between;
publishing before the commit sends messages for changes that roll back. The outbox makes the
database the source of truth:

1. In the business transaction, store the encoded message: `prepared = await topic.prepare(event)`
   then `INSERT INTO outbox (id, message) VALUES (prepared.message_id, prepared.to_json())`.
   `prepare` does no network I/O for the message (only the cached topic lookup).
2. A relay publishes pending rows and marks them: `OutboxRelay(client, store).run()`.

```sql
CREATE TABLE outbox (
    id         text PRIMARY KEY,
    message    jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    attempts   int NOT NULL DEFAULT 0,
    published_at timestamptz
);
CREATE INDEX outbox_pending ON outbox (created_at) WHERE published_at IS NULL;
```

```python
class PostgresOutbox:   # implements fanferro.outbox.OutboxStore (asyncpg)
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def claim_batch(self, limit: int) -> list[OutboxRecord]:
        # Run the relay in a transaction per round, or use a lease column; SKIP LOCKED lets
        # several relays work in parallel without publishing the same row twice.
        rows = await self.pool.fetch(
            "SELECT id, message, attempts FROM outbox WHERE published_at IS NULL "
            "ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED", limit)
        return [OutboxRecord(r["id"], PreparedMessage.from_json(r["message"]), r["attempts"]) for r in rows]

    async def mark_published(self, ids: Sequence[str]) -> None:
        await self.pool.execute("UPDATE outbox SET published_at = now() WHERE id = ANY($1)", list(ids))

    async def mark_failed(self, record: OutboxRecord, error: BaseException) -> None:
        await self.pool.execute("UPDATE outbox SET attempts = attempts + 1 WHERE id = $1", record.id)
```

Delivery is at least once (a crash between publish and mark republishes). The message id is
stable: FIFO destinations deduplicate it; standard destinations need idempotent consumers
(below). FIFO rows are relayed one by one in order and a failure holds back the rest of its group.
Runnable version: `examples/transactional_outbox.py`.

## Idempotent consumer (inbox)

Make side effects and "I processed message X" one atomic step:

```python
@consumer.route(PaymentCaptured)
async def apply(message: fanferro.Message[PaymentCaptured]) -> None:
    async with db.transaction():
        inserted = await db.execute(
            "INSERT INTO processed_messages (id) VALUES ($1) ON CONFLICT DO NOTHING", message.id)
        if inserted == "INSERT 0 0":
            return                                  # duplicate: already applied
        await db.execute("UPDATE balances SET ...")
```

For handlers whose effects are not in your database, `fanferro.idempotency.IdempotencyMiddleware`
claims the message id in a store (implement `IdempotencyStore` on Redis/DynamoDB; claims expire
so a crashed worker does not block a message forever), skips completed ids (`message.duplicate`)
and retries messages another worker is processing. A handler that hands its message back
(`message.retry()`, `requeue()`, `release()` or `dead_letter()`) leaves the id unclaimed, so the
next delivery runs it again. `InMemoryIdempotencyStore` forgets expired ids; it suits tests and
single-process workers.

## Fan-out with filtering

One topic, one queue per consumer service; each subscription filters what it needs:

```python
events = client.topic("customer-events")
client.subscription(events, client.queue("email-service"), filter_policy={"fanferro.type": ["customer.signed_up.v1"]})
client.subscription(events, client.queue("search-indexer"))          # everything
client.subscription(events, client.queue("eu-billing"), filter_policy={"region": ["eu"]})
```

`fanferro.type` is set on every message published with a type, so filter policies on message
types work without custom attributes. Policies can also match prefixes, wildcards, IP ranges and
numeric ranges, for example `{"region": [{"prefix": "eu-"}], "amount": [{"numeric": [">=", 100]}]}`;
numeric conditions match `Number` attributes only, so publish such attributes as numbers
(`attributes={"amount": 150}`), not as strings. With `filter_policy_scope="MessageBody"`, policies
match (nested) fields of JSON bodies instead.

## Versioned messages

Evolve types with explicit versions and route by type or header:

```python
@fanferro.message_type("customer.signed_up.v2")
class SignedUpV2(BaseModel): ...

@router.route(SignedUpV1)
async def v1(event: SignedUpV1) -> None: await handle(upgrade(event))

@router.route(SignedUpV2)
async def v2(event: SignedUpV2) -> None: await handle(event)
```

Rules of thumb: additive changes keep the version; breaking changes get a new type name; consumers
deploy support for a new version before producers start sending it; `@message_type(name, *aliases)`
lets one class accept old names.

## Per-entity ordering

Use FIFO topics/queues with the entity id as group id (`group_id=lambda m: m.body.account_id`).
Ordering is per group; throughput scales with the number of groups (enable high throughput with
`QueueAttributes(deduplication_scope="messageGroup", fifo_throughput_limit="perMessageGroupId")`).

## Competing consumers and back-pressure

Run the same consumer in several processes/hosts - SQS distributes messages. Inside a process,
tune `concurrency`/`prefetch`; protect fragile dependencies with `rate_limit`; stop receiving
during an outage with `consumer.pause()` (e.g. from a circuit breaker hook) and `resume()` later.

## Long-running jobs

Heartbeats keep messages invisible for up to 12 hours. Use `Heartbeat(max_duration=...)` to give up
earlier, `handler_timeout` to cancel stuck handlers, and make jobs resumable. For jobs longer than
12 hours, acknowledge the message and track the job elsewhere (e.g. Step Functions).

## Large documents

Send references, not documents, when consumers might not need the bytes; otherwise enable
[claim checks](large-payloads.md) and let fanferro move the bytes through S3.
