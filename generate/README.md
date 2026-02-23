krammar: kafka grammar
=======

- [Overview](#overview)
- [File structure](#file_structure)
- [Comments and field versioning](#comments_and_field_versioning)
- [Types](#types)
  - [Primitives](#primitives)
  - [Text and bytes](#text_and_bytes)
  - [Arrays](#arrays)
  - [Structs](#structs)
  - [Enums](#enums)
- [Default values](#default_values)
- [Flexible versions and tags](#flexible_versions_and_tags)
- [Special fields](#special_fields)
- [Named struct modifiers](#named_struct_modifiers)
- [Spacing rules](#spacing_rules)

Overview
--------

This directory contains a custom DSL and code generator that produces Go
structs, encoding/decoding functions, and related methods for the Kafka wire
protocol. The generator reads definition files from `definitions/` and an
`enums` file, then outputs the generated Go code to `../pkg/kmsg/generated.go`.

The DSL is narrowly scoped to Kafka's binary protocol with versioning,
flexible encoding, tags, and special encoding rules. Definition files are
numbered for ordering (e.g. `00_produce`, `01_fetch`).

File structure
--------------

Each definition file contains one or more struct definitions separated by
blank lines. Request/response pairs must appear together, with the response
immediately following its request.

The `enums` file contains all enum definitions used across the protocol.

The `misc` file contains non-top-level structs like `Record`, `RecordBatch`,
and various internal Kafka metadata types.

Comments and field versioning
--------

All comments begin with `//`.
There are two types of comments: struct/field comments and field version comments.

**Struct** or **field** comments are above what they are commenting:

```
// FooRequest is a foo request.
FooRequest =>
  // FooField is a string.
  FooField: string
```

**Field version** comments follow the field on the same line:

```
FooRequest =>
  FooField: string // v1+
```

Version comments must be of the form `v\d+(\+|\-v\d+)`:

```
v1+        field exists from version 1 onward
v3-v5      field exists from version 3 through 5 inclusive
v0-v8      field exists up to version 8
```

If a version comment applies to a struct field,
it is implied that all fields within that struct are bounded
by the overarching struct field version constraints:

```
FooRequest =>
  // All fields in FooField are bounded between version 3 and 5.
  FooField: [=>] // v3-v5
    Biz: int8
    Bar: int8
    Baz: int8
```

Fields that do not have version comments
are valid for all versions of a struct.

Types
-----

Kafka has a few basic primitive types and some more confusing, one off types.

### Primitives

```
bool               one byte; 0 == false
int8               one byte, signed
int16              two bytes, signed big endian
uint16             two bytes, unsigned big endian
int32              four bytes, signed big endian
int64              eight bytes, signed big endian
float64            eight bytes, big endian
uint32             four bytes, unsigned big endian
varint             one to five bytes, int32 using protocol buffer encoding
varlong            one to ten bytes, int64 using protocol buffer encoding
uuid               sixteen bytes, fixed-size UUID
```

### Text and bytes

```
string             int16 size specifier followed by that many bytes of a string
nullable-string    like string, but a negative size signifies a null string
varint-string      like nullable-string, but with a varint size specifier
bytes              int32 size specifier followed by that many bytes
nullable-bytes     like bytes, but a negative size signifies null
varint-bytes       like nullable-bytes, but with a varint size specifier
```

There is also `nullable-string-v<N>+`, which behaves as a regular `string`
before version N and as a `nullable-string` from version N onward:

```
Protocol: nullable-string-v7+
```

### Arrays

There are three types of arrays: normal, nullable, and varint length.
Normal arrays have int32 size specifiers;
nullable arrays allow the size specifier to be negative while varint arrays allow the size to be a varint.
Arrays are specified wrapping a type.
If the type is an inner unnamed struct, the array is specified as `[=>]`,
where the next lines continue specifying the struct.
If the inner struct is not anonymous, the struct must have been already defined.

```
[bool]             an array of booleans
nullable[int8]     a nullable array of int8s
varint[Header]     a variadic length array containing Header structs
[=>]               an array of anonymous structs specified on the following lines
```

Arrays can become nullable at a specific version:

```
Topics: nullable-v9+[=>]
```

This means the array is non-nullable before version 9 and nullable from
version 9 onward.

### Structs

There are two types of structs: named structs and anonymous structs.
Named structs exist as a top level definition and have a name.
Anonymous structs exist anonymously inside a top level struct and do not have a name.
However, parsers may name anonymous structs themselves so that users can more easily create types.
Named structs have potential modifiers to change how they are parsed.

Only structs are allowed in definition files;
there is no primitive nor array at the top level.
Struct fields can be anything, including other struct fields.
It is impossible for a struct definition to be circular.

If a struct field is an anonymous array,
the `[=>]` may be followed with a singular name hint.
This is to aid generators that do want to de-anonymize the anonymous array.

Nullable anonymous structs use `nullable=>`:

```
Assignment: nullable=>
  TopicPartitions: [=>]
    TopicID: uuid
    Partitions: [int32]
```

Top level structs do not have a colon following their name,
but all struct fields do have a colon following the field names.

Examples:
```
SimpleType =>
  Field1: int8

FooRequest =>
  Field1: string
  Field2: int8
  Field3Pluralies: [=>]Field3Pluraly
    InnerField1: int8
    InnerField2: nullable-bytes
    InnerField3: [SimpleType]
    InnerField4s: [=>]
      InnerInner: string
    InnerField5: =>
      SuperIn: varint
  Field4: int8
```

The above shows two type definitions,
the second with multiple nested inner structs.
Two of the nested structs are anonymous and in arrays,
one is named and in an array,
and the other is anonymous but not in an array.
One of the anonymous array fields (`Field3Pluralies`) has a name hint.

There is one special type of struct field: `length-field-minus`.
This field is raw bytes whose size is determined from another field
earlier in the struct.
That is, if there exists a field `Length` in the struct,
and then some other fields,
and then finally the `length-field-minus` field `Foo`,
the `Foo` field can be specified as `Foo: length-field-minus => Length - 49`
signifying that `Foo` is a struct that is encoded as raw bytes of length `Length` minus 49.

The only spot this is used is in the `RecordBatch` field,
and mostly for a historic reason of the length field being
defined separately from the message set in Kafka's original message set encoding.

### Enums

Enums are defined in the `enums` file. Each enum has a name, a backing
primitive type, and a list of numeric values:

```
// A type of config.
ConfigResourceType int8 (
  2: TOPIC
  4: BROKER
  8: BROKER_LOGGER
)
```

Values can have comments:

```
ConfigSource int8 (
  // Dynamic topic config for a specific topic.
  1: DYNAMIC_TOPIC_CONFIG
  // Dynamic broker config for a specific broker.
  2: DYNAMIC_BROKER_CONFIG
)
```

By default, enum value names are UPPER_CASE. Adding `camelcase` after the
type changes generation to use CamelCase:

```
TransactionState int8 camelcase (
  0: Empty
  1: Ongoing
)
```

Enums can be referenced in struct fields with the `enum-` prefix:

```
ErrorCode: enum-ErrorCode
```

Default values
--------------

Fields can have default values specified in parentheses after the type:

```
LogAppendTime: int64(-1) // v2+
RebalanceTimeoutMillis: int32(-1) // v1+
FinalizedFeaturesEpoch: int64(-1) // tag 1
```

Defaults are supported for:
- Numeric types: `int32(-1)`, `int64(0)`, `float64(3.14)`
- Booleans: `bool(true)`
- Nullable types: `nullable-string(null)`, `nullable-bytes(null)`
- Arrays: `[int32](null)`

Default values affect generated code in two ways:
1. The `Default()` method initializes fields to their defaults.
2. In flexible encoding, fields equal to their default are not written.

Flexible versions and tags
--------------------------

Kafka 2.4+ (KIP-482) introduced "flexible versions" with compact encoding
and tagged fields. The `flexible v<N>+` modifier on a struct enables this:

```
ProduceRequest => key 0, max version 13, flexible v9+
```

From the flexible version onward:
- Strings and bytes use compact (varint-length) encoding.
- Arrays use compact (uvarint-length) encoding.
- Structs gain an `UnknownTags` field for round-tripping unknown tags.

Tagged fields are appended at the end of a struct when the version is
flexible. They are only written if their value differs from the default.
Tags are specified in field version comments:

```
SupportedFeatures: [=>] // tag 0
FinalizedFeaturesEpoch: int64(-1) // tag 1
FinalizedFeatures: [=>] // tag 2
ZkMigrationReady: bool // tag 3
```

Tags can also combine with version constraints:

```
ConfigErrorCode: int16 // v8+, tag 0
```

Tag numbers within a struct must be unique and monotonically increasing
starting from 0. Every tagged field must have a default value (so the
encoder knows when to skip it).

Special fields
--------------

Two field types have special syntax without a `: ` separator:

### ThrottleMillis

```
ThrottleMillis
ThrottleMillis(4)
ThrottleMillis // v2+
ThrottleMillis(4) // v2+
```

This generates an int32 field with `Throttle()` and `SetThrottle()` methods.
The optional number in parentheses is the version at which Kafka switched
from pre-response throttling to post-response throttling. The optional
version comment indicates when the field was introduced.

### TimeoutMillis

```
TimeoutMillis
TimeoutMillis(60000)
TimeoutMillis // v1+
```

This generates an int32 field with `Timeout()` and `SetTimeout()` methods.
The optional number in parentheses overrides the default timeout of 15000ms.
The optional version comment indicates when the field was introduced.

Named struct modifiers
----------------------

Named structs have a few potential modifiers.

If a struct is not top level,
meaning it is not a request or response type struct,
the first modifier must be `not top level`.
These non-top-level structs can be used to define
structs that are embedded in requests or responses
that are commonly used / more special than anonymous structs.
For example, a `Record`, `RecordBatch`, or `StickyMemberMetadata`.

For "not top level" structs, there are two other potential mutually exclusive modifiers:

1) `, with version field` indicates that the struct contains a Version field.
If a version field exists, it must be the first field.
This modifier allows non-top-level structs to still have proper version fields.
2) `, no encoding` indicates that encode and decode functions should not be generated for this struct.

Either modifier can optionally be followed by `, flexible v<N>+`.

Examples:
```
Record => not top level
  Key: bytes
  Value: bytes

RecordBatch => not top level
  Length: int32
  ...

OffsetCommitValue => not top level, with version field, flexible v4+
  Offset: int64
  ...

Assignment => not top level, no encoding, flexible v0+
  TopicPartitions: [=>]
    ...
```

If a struct is top level,
if the struct is a request type,
the modifiers are comma-separated after `=>`:
`key <N>`, `max version <N>`, and optionally
`admin`, `group coordinator`, or `txn coordinator`,
and optionally `flexible v<N>+`.

These three coordinator modifiers signify whether the request needs to go to
the admin controller, group coordinator, or transaction coordinator.

If the request is a response type,
there must be no modifier,
and the response type must have the same name (`s/Request/Response/`)
as the request it is for,
and it must follow the request in the definitions file.
This is required to enforce that responses are tied to their requests appropriately.

Examples:
```
ProduceRequest => key 0, max version 13, flexible v9+
  ...

ProduceResponse =>
  ...

DeleteTopicsRequest => key 20, max version 6, flexible v4+, admin
  ...

JoinGroupRequest => key 11, max version 9, flexible v6+, group coordinator
  ...

AddOffsetsToTxnRequest => key 25, max version 4, flexible v3+, txn coordinator
  ...
```

Spacing rules
-------------

- Between every field name and type, there must be exactly `: ` (colon space).
- Between every word / symbol / number in modifiers, there must be only one space.
- Lines cannot have trailing spaces.
- Internal struct fields must be nested two more spaces than the encompassing struct.
  Top-level fields start at two spaces; nested struct fields at four; and so on.
- There must be one blank line between type definitions.

Validation against Kafka JSON definitions
------------------------------------------

The test in `validate_test.go` compares the DSL definitions against the
canonical JSON message definitions in the Apache Kafka source tree. It catches
structural drift: wrong types, missing version constraints, incorrect field
ordering, missing tags, etc.

### Running the test

Set `KAFKA_DIR` to the root of a Kafka checkout and run:

```
KAFKA_DIR=/path/to/kafka go test -run TestValidateDSLAgainstKafkaJSON -v
```

The test is skipped when `KAFKA_DIR` is unset.

### What it checks

For each request/response pair (matched by `apiKey`):

- **Max version** — the DSL max version must not exceed the JSON stable max
  (unstable latest versions in JSON are excluded).
- **Flexible version** — the DSL `FlexibleAt` must match the JSON
  `flexibleVersions` start.
- **Per-version field structure** — at every version 0 through the common max,
  non-tagged fields are compared in order and tagged fields are compared by
  tag number. Types, nullability, and nested struct fields are all validated.

### MISSING output

When the DSL is behind Kafka (lower max version or entirely missing messages),
the test prints `MISSING` lines to stderr rather than failing. This lets you
see at a glance what needs to be added to support a new Kafka release:

```
MISSING  SomeNewRequest                                v3..5, new fields: FieldName (type, v4+)
MISSING  AnotherRequest                                not in DSL (apiKey 99)
```

### CI

The GitHub Actions workflow at `.github/workflows/validate-definitions.yml`
runs this test on pushes to master and on pull requests that touch
`generate/**`. It sparse-checks out the Apache Kafka repo to get the JSON
definitions.
