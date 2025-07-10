# GPABun

A Bun adapter for the Go Persistence API (GPA), providing a type-safe database abstraction layer with support for PostgreSQL, MySQL, and SQLite.

## Features

- **Type-safe repositories** with Go generics
- **Multi-database support** (PostgreSQL, MySQL, SQLite)
- **Transaction support** with savepoints
- **Connection pooling** configuration
- **Query hooks** for debugging and logging
- **Comprehensive error handling** with typed errors

## Installation

```bash
go get github.com/lemmego/gpabun
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    
    "github.com/lemmego/gpa"
    "github.com/lemmego/gpabun"
)

type User struct {
    ID   int64  `bun:"id,pk,autoincrement"`
    Name string `bun:"name"`
}

func main() {
    // Configure database connection
    config := gpa.Config{
        Driver:   "postgres",
        Host:     "localhost",
        Port:     5432,
        Database: "myapp",
        Username: "user",
        Password: "password",
    }
    
    // Create provider
    provider, err := gpabun.NewProvider(config)
    if err != nil {
        log.Fatal(err)
    }
    defer provider.Close()
    
    // Get type-safe repository
    userRepo := gpabun.GetRepository[User](provider)
    
    // Use the repository
    user := &User{Name: "John Doe"}
    err = userRepo.Create(context.Background(), user)
    if err != nil {
        log.Fatal(err)
    }
}
```

## Supported Databases

- **PostgreSQL** (`postgres`, `postgresql`)
- **MySQL** (`mysql`)
- **SQLite** (`sqlite`, `sqlite3`)

## Configuration

```go
config := gpa.Config{
    Driver:           "postgres",
    Host:            "localhost",
    Port:            5432,
    Database:        "myapp",
    Username:        "user",
    Password:        "password",
    MaxOpenConns:    25,
    MaxIdleConns:    5,
    ConnMaxLifetime: time.Hour,
    ConnMaxIdleTime: time.Minute * 5,
    Options: map[string]interface{}{
        "bun": map[string]interface{}{
            "log_level": "debug", // Enable query logging
        },
    },
}
```

## Repository Operations

```go
// Create
user := &User{Name: "Alice"}
err := userRepo.Create(ctx, user)

// Find by ID
user, err := userRepo.FindByID(ctx, 1)

// Find all
users, err := userRepo.FindAll(ctx)

// Update
user.Name = "Alice Updated"
err = userRepo.Update(ctx, user)

// Delete
err = userRepo.Delete(ctx, 1)

// Count
count, err := userRepo.Count(ctx)

// Transactions
err = userRepo.Transaction(ctx, func(tx gpa.Transaction[User]) error {
    // Perform multiple operations within transaction
    return nil
})
```

## Raw Queries

```go
// Raw query
users, err := userRepo.RawQuery(ctx, "SELECT * FROM users WHERE age > ?", []interface{}{18})

// Raw execution
result, err := userRepo.RawExec(ctx, "UPDATE users SET active = ? WHERE id = ?", []interface{}{true, 1})
```

## Error Handling

GPABun provides typed errors for common database scenarios:

```go
user, err := userRepo.FindByID(ctx, 999)
if err != nil {
    var gpaErr gpa.GPAError
    if errors.As(err, &gpaErr) {
        switch gpaErr.Type {
        case gpa.ErrorTypeNotFound:
            // Handle not found
        case gpa.ErrorTypeDuplicate:
            // Handle duplicate key
        case gpa.ErrorTypeConstraint:
            // Handle constraint violation
        }
    }
}
```

## License

MIT License - see [LICENSE.md](LICENSE.md) for details.