# data-mongo

[![GoDoc](https://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://pkg.go.dev/github.com/benpate/data-mongo)
[![Version](https://img.shields.io/github/v/release/benpate/data-mongo?include_prereleases&style=flat-square&color=brightgreen)](https://github.com/benpate/data-mongo/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/benpate/data-mongo/go.yml?style=flat-square)](https://github.com/benpate/data-mongo/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/benpate/data-mongo?style=flat-square)](https://goreportcard.com/report/github.com/benpate/data-mongo)
[![Codecov](https://img.shields.io/codecov/c/github/benpate/data-mongo.svg?style=flat-square)](https://codecov.io/gh/benpate/data-mongo)

## MongoDB adapter for the "data" interface

`data-mongo` implements the [`benpate/data`](https://github.com/benpate/data) interfaces (`Server`, `Session`, `Collection`, `Iterator`) on top of the official MongoDB Go driver. It lets application code perform CRUD operations against MongoDB through the generic `data` abstraction, without importing the driver directly. Queries are written as [`benpate/exp`](https://github.com/benpate/exp) expressions, which this package translates into native BSON.

```go
server, err := mongodb.New("mongodb://localhost:27017", "mydatabase", nil)
session, err := server.Session(ctx)
collection := session.Collection("users")

err = collection.Load(exp.Equal("username", "sarah"), &user)
```

## What matters here

- **String-match operators are escaped against regex injection.** `BeginsWith` / `Contains` / `EndsWith` compile to MongoDB `$regex`, so the user value is run through `regexp.QuoteMeta` before embedding. Removing that escaping would let input inject metacharacters (a `.` matching anything) or a pathological pattern (ReDoS). See `operatorBSON` in [expression.go](expression.go).

- **`Delete` is a *virtual* delete; `HardDelete` is physical.** `Delete` marks the object deleted and re-saves it (the row stays in the database); only `HardDelete` issues a real `DeleteMany`. Don't assume `Delete` removes data.

- **`Session.Close` is intentionally a no-op.** Connections are owned by the long-lived `*mongo.Client` pool, not the session. Per-request cleanup happens by cancelling the `context.Context` passed to `Server.Session`, not by calling `Close`. The method exists only to satisfy the interface.

- **The query `context` is carried on the `Collection`/`Session` structs**, set when the session is opened. This is a deliberate deviation from "don't store a context in a struct," dictated by the `data` interface shape — methods like `Load`/`Save` take no `ctx` argument.

- **Slow-query logging is off by default and global.** Call `SetLogTimeout(ms)` to enable it. The threshold is read atomically, so it is safe to change while queries are in flight. When disabled, the per-query timer is skipped entirely (no `time.Now()` cost).

- **Transactions require a replica set or mongos.** `Server.WithTransaction` uses majority read/write concern and causal consistency; it will fail against a standalone `mongod`.

- **Geometry values are passed through as-is.** The `GeoWithin` / `GeoIntersects` operators expect the value to already be a GeoJSON `map[string]any` — produced upstream by `exp.GeoWithin` / `exp.GeoIntersects` calling `GeoJSON()` on a `geo` shape. This package does no GeoJSON conversion of its own.
