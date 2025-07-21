# Schema Registry Mock

The `srfake` package provides an in-memory, concurrency-safe implementation of the Confluent Schema Registry REST API for unit and integration testing.

## Features

- **In-memory storage**: Fast tests with no external dependencies
- **Concurrency-safe**: All public methods are thread-safe
- **HTTP test server**: Uses `httptest.Server` for realistic testing
- **Interceptors**: Customize behavior for error testing

## Quick Start

```go
import (
    "github.com/twmb/franz-go/pkg/sr"
    "github.com/twmb/franz-go/pkg/sr/srfake"
)

func TestSchemaRegistry(t *testing.T) {
    // Create mock registry
    reg := srfake.New(
        srfake.WithAuth("Bearer token"),
        srfake.WithGlobalCompat(sr.CompatBackward),
    )
    defer reg.Close()

    // Seed with a schema
    userSchema := sr.Schema{
        Schema: `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`,
        Type:   sr.TypeAvro,
    }
    reg.SeedSchema("user-value", 1, 1, userSchema)

    // Create client and test
    client, _ := sr.NewClient(sr.URLs(reg.URL()))
    schema, err := client.SchemaByID(context.Background(), 1)
    if err != nil {
        t.Fatal(err)
    }
    
    // Verify schema
    if schema.Schema != userSchema.Schema {
        t.Errorf("Schema mismatch")
    }
}
```

## Core Usage

### Schema Registration

```go
// Register programmatically
id, version, err := reg.RegisterSchema("subject", schema)

// Or seed directly
reg.SeedSchema("subject", version, id, schema)
```

### Schema References

```go
// Register base schema
reg.SeedSchema("address-value", 1, 1, addressSchema)

// Register schema with reference
userSchema := sr.Schema{
    Schema: `{"type":"record","name":"User","fields":[{"name":"address","type":"Address"}]}`,
    Type:   sr.TypeAvro,
    References: []sr.SchemaReference{
        {Name: "Address", Subject: "address-value", Version: 1},
    },
}
reg.RegisterSchema("user-value", userSchema)
```

## Interceptors

Interceptors allow customizing the mock's behavior for testing error conditions. They receive HTTP requests first and can return custom responses.

```go
type Interceptor func(w http.ResponseWriter, r *http.Request) (handled bool)
```

### Example: Testing Schema Incompatibility

```go
func TestSchemaIncompatibility(t *testing.T) {
    reg := srfake.New()
    defer reg.Close()

    // Intercept compatibility check requests
    reg.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
        if strings.Contains(r.URL.Path, "/compatibility/") {
            // Return registry-style error for incompatible schema
            w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
            w.WriteHeader(http.StatusConflict)
            response := sr.ResponseError{
                ErrorCode: 42201,
                Message:   "Schema being registered is incompatible with earlier schema",
            }
            json.NewEncoder(w).Encode(response)
            return true
        }
        return false
    })

    client, _ := sr.NewClient(sr.URLs(reg.URL()))
    
    // This will return a properly typed ResponseError
    compatible, err := client.Compatible(context.Background(), schema, "test-subject", sr.VersionLatest)
    if err == nil {
        t.Error("Expected compatibility error")
    }
    
    // Error can be checked as ResponseError
    var respErr *sr.ResponseError
    if errors.As(err, &respErr) {
        if respErr.ErrorCode != 42201 {
            t.Errorf("Expected error code 42201, got %d", respErr.ErrorCode)
        }
    }
}
```

### Managing Interceptors

```go
// Add interceptor
reg.Intercept(interceptorFunc)

// Clear all interceptors
reg.ClearInterceptors()
```

## API Coverage

The mock implements the core Schema Registry endpoints:

- **Schemas**: `GET /schemas/ids/{id}`, `GET /schemas/ids/{id}/schema`, `GET /schemas/ids/{id}/versions`
- **Subjects**: `GET /subjects`, `GET /subjects/{subject}/versions`, `POST /subjects/{subject}/versions`
- **Versions**: `GET /subjects/{subject}/versions/{version}`, `DELETE /subjects/{subject}/versions/{version}`
- **Config**: `GET /config`, `PUT /config`, `GET /config/{subject}`, `PUT /config/{subject}`
- **Compatibility**: `POST /compatibility/subjects/{subject}/versions/{version}`
- **References**: `GET /subjects/{subject}/versions/{version}/referencedby`

## Options

```go
// Authentication
reg := srfake.New(srfake.WithAuth("Bearer token"))

// Global compatibility
reg := srfake.New(srfake.WithGlobalCompat(sr.CompatBackward))

// Combined
reg := srfake.New(
    srfake.WithAuth("Bearer token"),
    srfake.WithGlobalCompat(sr.CompatForward),
)
```

## Testing Utilities

```go
// Check if subject exists
exists := reg.SubjectExists("subject-name")

// Get schema by subject and version
schema, found := reg.GetSchema("subject", 1)

// Reset all state between tests
reg.Reset()
```

## Limitations

This mock implements only the basic Schema Registry functionality. Newer advanced features are not supported:

- **Compatibility checking**: Always returns `true` (use interceptors for testing failures)
- **Schema validation**: Basic validation only, no full schema evolution checking
- **Authentication**: Simplified bearer token only
- **Import mode**: Not supported
- **Schema contexts**: No support for schema grouping/contexts
- **Advanced features**: Role-based access control, audit logs, data contracts, etc.