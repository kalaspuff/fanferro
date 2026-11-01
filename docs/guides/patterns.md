# Patterns

## Transactional outbox

Publishing directly after a database commit loses messages when the process dies in between;
publishing before the commit sends messages for changes that roll back. The outbox makes the
database the source of truth:

1. In the business transaction, store the encoded message: `prepared = await topic.prepare(event)`
   then `INSERT INTO outbox (id, message, fifo_group) VALUES (prepared.message_id,
   prepared.to_json(), prepared.fifo_group)`.
   `prepare` does no network I/O for the message (only the cached topic lookup).
2. A relay publishes pending rows and marks them: `OutboxRelay(client, store).run()`.

```sql
CREATE TABLE outbox (
    id         text PRIMARY KEY,
    message    jsonb NOT NULL,
    fifo_group text,              -- prepared.fifo_group: NULL for standard destinations
    created_at timestamptz NOT NULL DEFAULT now(),
    attempts   int NOT NULL DEFAULT 0,
    published_at timestamptz,
    abandoned_at timestamptz
);
CREATE INDEX outbox_pending ON outbox (created_at) WHERE published_at IS NULL AND abandoned_at IS NULL;
```

```python
class PostgresOutbox:   # implements fanferro.outbox.OutboxStore (asyncpg)
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def claim_batch(self, limit: int) -> list[OutboxRecord]:
        # Run the relay in a transaction per round, or use a lease column. SKIP LOCKED lets several
        # relays work in parallel without publishing the same row twice, and the advisory lock
        # (held until the round's transaction ends) gives each FIFO group to one relay at a time,
        # so that its rows stay in order.
        rows = await self.pool.fetch(
            "SELECT id, message, attempts FROM outbox WHERE published_at IS NULL AND abandoned_at IS NULL "
            "AND (fifo_group IS NULL OR pg_try_advisory_xact_lock(hashtext(fifo_group))) "
            "ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED", limit)
        return [OutboxRecord(r["id"], PreparedMessage.from_json(r["message"]), r["attempts"]) for r in rows]

    async def mark_published(self, ids: Sequence[str]) -> None:
        await self.pool.execute("UPDATE outbox SET published_at = now() WHERE id = ANY($1)", list(ids))

    async def mark_failed(self, record: OutboxRecord, error: BaseException) -> None:
        await self.pool.execute("UPDATE outbox SET attempts = attempts + 1 WHERE id = $1", record.id)

    async def mark_abandoned(self, record: OutboxRecord, error: BaseException) -> None:
        await self.pool.execute(
            "UPDATE outbox SET attempts = attempts + 1, abandoned_at = now() WHERE id = $1", record.id)
```

Several relays and FIFO destinations need claims that are exclusive per message group: with
`SKIP LOCKED` alone, a second relay claims the later rows of a group while the first still sends
the earlier ones, and the group arrives out of order. The advisory lock above does that (a hash
collision only makes two groups wait for each other); without such a claim, run a single relay for
FIFO destinations. `InMemoryOutbox` claims the same way.

A round marks the rows it published as soon as it knows, also when it fails or its task is
cancelled at shutdown, and releases the claims of the rows it did not attempt with the optional
`release(records)` (the lease-column variant clears the lease there; without the method the relay
calls `mark_failed` for them). An error of the store while recording one row's failure is logged
and does not end the round. A row may be marked published more than once.

Delivery is at least once (a crash between publish and mark republishes). The message id is
stable: FIFO destinations deduplicate it; standard destinations need idempotent consumers
(below). Rows for standard destinations go out in one batch per destination, the destinations at
once. FIFO rows are relayed one by one in order within their message group, up to eight groups at
once, and a failure holds back the rest of its group. The relay calls the store one call at a time.
A row that cannot be published (its topic was deleted, the destination refuses it) is given up
after `OutboxRelay(max_attempts=10)` attempts that failed for good, with `mark_abandoned`, so that
it does not hold back the rows behind it; a FIFO group then continues with its next row. Errors
that may pass (throttling, timeouts, network errors) and rounds spent behind an earlier row of a
FIFO group do not count. Inspect given-up rows, and reset
`abandoned_at` (and `attempts`) to relay them again. Runnable version: `examples/transactional_outbox.py`.

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
claims the message id in a store (implement `IdempotencyStore` on Redis/DynamoDB; `claim` may
return a `Claim` or its string value) and skips completed ids (`message.duplicate`). A message
whose id another worker holds is postponed by `retry_delay` (30 s) until that claim is completed
or released, or expires after `lock_ttl` (15 minutes; a worker that crashed holds it that long).
Postponements are not processing attempts, so they never use up `RetryPolicy.max_attempts`: on
standard queues the message is sent back as a copy that keeps its attempt number (which also resets
SQS's receive count); on FIFO queues its visibility changes, and only the consumer that postponed it
leaves its next receive out of the attempt count (another consumer process counts it), and SQS
counts every receive toward `max_receive_count`. A handler that hands its message back (`message.retry()`,
`requeue()`, `release()` or `dead_letter()`) leaves the id unclaimed, so the next delivery runs it
again. When the store fails to mark an id completed after the handler succeeded, the message is
still acknowledged and the error reported (`consumer.error`): its claim then expires after
`lock_ttl`. `InMemoryIdempotencyStore` suits tests and single-process workers. It forgets
expired ids (a sweep at most once per `sweep_interval`, 60 s, touches only the expired ones) and
keeps at most `max_entries` ids (100,000 by default; beyond it the oldest completed ids are
forgotten first, which only weakens deduplication of old messages). Each id takes about 200 bytes,
and the store holds every id of the last `ttl` seconds up to that bound: at 100 messages per
second, the default `ttl` of 7 days would be 60 million ids (about 12 GB) with `max_entries=None`.
Give the in-memory store a shorter `ttl`, such as `idempotent(ttl=timedelta(hours=1))`: duplicates
from publishers and redeliveries after a lost delete usually arrive within minutes.

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
SNS delivers to FIFO queues from FIFO topics only, so subscribing a FIFO queue to a standard topic
raises `ConfigurationError` when it is declared (a FIFO topic can deliver to standard queues too).
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
