# data-mongo — Notes for AI Agents

- **String-match operators are escaped against regex injection.** `BeginsWith` / `Contains` / `EndsWith` compile to MongoDB `$regex`, so the user value is run through `regexp.QuoteMeta` before embedding. Removing that escaping would let input inject metacharacters (a `.` matching anything) or a pathological pattern (ReDoS). See `operatorBSON` in [expression.go](expression.go).

- **An empty expression translates to an EMPTY `bson.M`, never a nil one.** `exp.All()`, `exp.And()`, `exp.Or()`, and `exp.Empty()` all mean "match everything" and all return a writable map. `ExpressionToBSON` is exported, and returning nil for the empty cases meant that a caller who added a condition of their own to the result panicked with "assignment to entry in nil map" — but only when the criteria happened to be empty. `exp.And()` avoids the identical trap for the same reason; see the RULE comment there.

- **A type-mismatched value fails CLOSED.** A number given to `CONTAINS`, a string given to `EXISTS`, and friends fall through to an empty comparison, which MongoDB matches against *no* records. Malformed input must narrow a query, never widen it — the sibling `exp-builder` package (BUG-131) depends on this. Do not "tidy" the fall-through into the `default` branch's `$eq`, and do not drop a predicate whose comparison is empty: either change makes it match everything.

- **`Iterator.Count()` reports the current BATCH, not the total.** A cursor only knows what the server already sent it, so a result set larger than one batch (101 documents by default) reports low. This is a knowing deviation from the `data.Iterator` interface, which specifies the total and which `data-mock` honors. Use `Collection.Count` for a real total.

- **`Iterator.Next` records decode errors; always check `Error()` after the loop.** A record that will not decode into the caller's target ends iteration, and without checking `Error()` a short read looks exactly like a complete one. `Iterator` uses POINTER receivers for this reason — `Next` has to write the error where `Error` can read it.

- **`Save` of a record that is not new always writes it, even when its document is gone.** The replace path upserts, because a journal stamped by an insert that was rolled back looks the same as a saved record, and a transaction the driver retries saves it again (BUG-188). The cost is that saving a record another writer hard-deleted recreates it. Do not remove the upsert to stop that; the caller needs a transaction, which turns the race into a write conflict and a retry.
- **`Delete` is a *virtual* delete; `HardDelete` is physical.** `Delete` marks the object deleted and re-saves it (the row stays in the database); only `HardDelete` issues a real `DeleteMany`. Don't assume `Delete` removes data.

- **`Session.Close` is intentionally a no-op.** Connections are owned by the long-lived `*mongo.Client` pool, not the session. Per-request cleanup happens by cancelling the `context.Context` passed to `Server.Session`, not by calling `Close`. The method exists only to satisfy the interface.

- **The query `context` is carried on the `Collection`/`Session` structs**, set when the session is opened. This is a deliberate deviation from "don't store a context in a struct," dictated by the `data` interface shape — methods like `Load`/`Save` take no `ctx` argument.

- **Slow-query logging is off by default and global.** Call `SetLogTimeout(ms)` to enable it. The threshold is read atomically, so it is safe to change while queries are in flight. When disabled, the per-query timer is skipped entirely (no `time.Now()` cost).

- **Transactions require a replica set or mongos.** `Server.WithTransaction` uses majority read/write concern and causal consistency; it will fail against a standalone `mongod`.

- **Geometry values are passed through as-is.** The `GeoWithin` / `GeoIntersects` operators expect the value to already be a GeoJSON `map[string]any` — produced upstream by `exp.GeoWithin` / `exp.GeoIntersects` calling `GeoJSON()` on a `geo` shape. This package does no GeoJSON conversion of its own.

## Compare driver sentinels with `errors.Is`, never `==`

`Collection.Load` tests `err == mongo.ErrNoDocuments` by identity, and that decision picks **404 vs 500** for every record load in Emissary. It works today only because the driver happens to return the sentinel unwrapped; any future wrapping turns every missing record into a 500. Filed as BUG-132 and deliberately left open — no failing test can be written for it while the driver behaves. Write `errors.Is` in new code here, and treat the existing sites as latent rather than correct.

## `ExpressionToBSON` must never return a nil map

It is exported, so a caller may add conditions of their own to the result. An empty AND/OR (the common `exp.All()` spelling) used to return nil, and writing to it panicked with "assignment to entry in nil map" — but only for that one input shape.

## The driver-compatibility harness imports BOTH mongo-driver majors

`go.mongodb.org/mongo-driver` and `.../v2` are separate module paths, so [driverCompat_test.go](driverCompat_test.go) imports them side by side and diffs them in one process. It exists for the v2 upgrade (BUG-122) and pins three things: the shapes that encode to **identical bytes** under both majors, the one decode difference that matters, and the trap that motivates the whole exercise.

The trap is that `ValueMarshaler` changed signature between majors — `MarshalBSONValue() (bsontype.Type, …)` became `(byte, …)`. Interfaces are satisfied structurally, so a type written for v1 does not fail to compile under v2; it silently stops being recognized and the driver encodes it with the default struct codec instead, changing the shape on disk. `TestDriverCompat_ValueMarshalerSignatureChanged` demonstrates it.

The other difference is that v2 decodes a nested document into `bson.D` where v1 gave a plain `map[string]any`, which makes rosetta's `mapof.Any` accessors read as empty with no error anywhere. `BSONOptions{DefaultDocumentMap: true}` restores the v1 shape and must be set on the client when the upgrade lands. `DefaultDocumentM` is **not** equivalent — `bson.M` is a named type and fails the same assertion.

The v2 dependency is test-only. It does not reach consumers of this module, because `go mod tidy` resolves test dependencies for the main module only.
