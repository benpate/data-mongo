// Package mongodb implements the benpate/data interfaces (Server, Session,
// Collection, Iterator) on top of the official MongoDB Go driver, so that
// application code can run CRUD operations against MongoDB without importing
// the driver directly.
//
// Queries are written as benpate/exp expressions, which this package translates
// into native BSON:
//
//	server, err := mongodb.New("mongodb://localhost:27017", "mydatabase", nil)
//	session, err := server.Session(ctx)
//	collection := session.Collection("users")
//
//	err = collection.Load(exp.Equal("username", "sarah"), &user)
//
// # String matching is escaped, and type mismatches fail closed
//
// The BEGINS, CONTAINS, and ENDS operators compile to a MongoDB $regex, so the
// comparison value is escaped with regexp.QuoteMeta before it is embedded. An
// untrusted value therefore matches as a literal: it can neither change the
// meaning of the pattern nor inject a pathological one (ReDoS).
//
// A value of the wrong Go type -- a number given to CONTAINS, a string given to
// EXISTS -- produces an empty comparison, which MongoDB matches against no
// records at all. Malformed input narrows a query; it never widens one.
//
// The guarantee covers the comparison VALUE, not the field name. A caller who
// builds an expression from an unvalidated field name is passing that name
// straight through to MongoDB.
//
// # Delete is virtual; HardDelete is physical
//
// Collection.Delete marks an object deleted and re-saves it, leaving the record
// in the database. Only HardDelete issues a real DeleteMany.
//
// # Iterator.Count reports one batch
//
// A cursor only knows what the server has already sent it, so Iterator.Count
// returns the records remaining in the CURRENT batch rather than the size of
// the whole result set. This is a deliberate deviation from the data.Iterator
// interface, which specifies the total; reporting the total here would mean an
// extra round trip on every call. Use Collection.Count for a true total.
//
// Iterator.Error reports the first failure encountered while iterating,
// including a record that would not decode into the caller's target. Always
// check it after the loop -- a decode failure ends iteration early, and without
// this check a short read is indistinguishable from a complete one.
//
// # Sessions, contexts, and transactions
//
// The query context is carried on the Session and Collection structs, set when
// the session is opened. This is a deliberate deviation from "don't store a
// context in a struct," dictated by the shape of the data interfaces, whose
// methods take no ctx argument.
//
// Session.Close is a no-op. Connections belong to the long-lived *mongo.Client
// pool, not the session, so per-request cleanup happens by cancelling the
// context passed to Server.Session.
//
// Server.WithTransaction uses majority read/write concern and causal
// consistency, and therefore requires a replica set or mongos; it fails against
// a standalone mongod.
//
// # Slow-query logging
//
// Slow-query logging is off by default and global. SetLogTimeout enables it for
// the whole process, and is safe to call while queries are in flight.
package mongodb
