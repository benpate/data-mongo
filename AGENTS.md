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
