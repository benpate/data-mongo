# 🍃 data-mongo

[![Go Reference](https://pkg.go.dev/badge/github.com/benpate/data-mongo.svg)](https://pkg.go.dev/github.com/benpate/data-mongo)
[![Version](https://img.shields.io/github/v/release/benpate/data-mongo?include_prereleases&style=flat-square&color=brightgreen)](https://github.com/benpate/data-mongo/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/benpate/data-mongo/go.yml?style=flat-square)](https://github.com/benpate/data-mongo/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/benpate/data-mongo?style=flat-square)](https://goreportcard.com/report/github.com/benpate/data-mongo)
[![Codecov](https://img.shields.io/codecov/c/github/benpate/data-mongo.svg?style=flat-square)](https://codecov.io/gh/benpate/data-mongo)

## MongoDB Adapter for the "data" Interface

`data-mongo` implements the [`benpate/data`](https://github.com/benpate/data) interfaces (`Server`, `Session`, `Collection`, `Iterator`) on top of the official MongoDB Go driver. It lets application code perform CRUD operations against MongoDB through the generic `data` abstraction, without importing the driver directly. Queries are written as [`benpate/exp`](https://github.com/benpate/exp) expressions, which this package translates into native BSON.

```go
server, err := mongodb.New("mongodb://localhost:27017", "mydatabase", nil)
session, err := server.Session(ctx)
collection := session.Collection("users")

err = collection.Load(exp.Equal("username", "sarah"), &user)
```

## Pull Requests Welcome

I'm trying to make data-mongo the best it can be, and your help is greatly appreciated. If you find a bug or have an idea for a new feature, please open an issue or submit a pull request. We're all in this together! 🍃
