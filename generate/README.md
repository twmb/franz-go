krammar: kafka grammar
=======

- [Comments, field versioning](#comments,_field_versioning)
- [Types](#types)
  - [Primitives](#primitives)
    - [Boolean and numeric](#boolean_and_numeric)
    - [Text](#text)
  - [Complex types](#complex_types)
    - [Arrays](#arrays)
    - [Structs](#structs)
- [Named struct modifiers](#named_struct_modifiers)
- [Miscellaneous](#miscellaneous)

Comments, field versioning
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

**Field version** comments follow the field being specified:

```
FooRequest =>
  FooField: string // v1+
```

Version comments must be of the form `v\d+(\+|\-v\d+)`:

```
v1+
v8-v9
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

#### Boolean and numeric

```
bool               one byte; 0 == false
int8               one byte, signed
int16              two bytes, signed big endian
int32              four bytes, signed big endian
int64              eight bytes, signed big endian
uint32             four bytes, unsigned big endian
varint             variadic bytes of an int32 (one to five) using protocol buffer encoding
varlong            variadic bytes of an int64 (one to ten) using protocol buffer encoding
```

#### Text

```
bytes              int32 size specifier followed by that many bytes
nullable-bytes     like bytes, but a negative size signifies null
varint-bytes       like nullable-bytes, but with a varint size specifier
string             int16 size specifier followed by that many bytes of a string
nullable-string    like string, but a negative size signifies a null string
varint-string      like nullable-string, but with a varint size specifier
```

### Complex types

#### Arrays

There are three types of arrays: normal, nullable, and varint length.
Normal arrays have int32 size specifiers;
nullable arrays allow the size specifier to be negative while varint arrays allow the size to be a varint.
Arrays are specified wrapping a type.
If the type is an inner unnamed struct, the array is specified as [=>],
where the next lines continue specifying the struct.
If the inner struct is not anonymous, the struct must have been already defined.

Examples:

```
[bool]             an array of booleans
nullable[int8]     a nullable array of int8s
varint[Header]     a variadic length array containing Header structs
[=>]               an array of anonymous structs that are specified on the following lines
```

#### Structs

There are two types of structs: named structs and anonymous structs.
Named structs exist as a top level definition and have a name.
Anonymous structs exist anonymously inside a top level struct and do not have a name.
However, parsers may name anonymous structs themselves so that users can more easily create types.
Named structs have potential modifiers to change how they are parsed.

Only structs are allowed in the `DEFINITIONS` file;
there is no primitive nor array at the top level.
Struct fields can be anything, including other struct fields.
It is impossible for a struct definition to be circular.

Spacing in struct fields is important:
all fields must have the same amount of spacing.
Inner structs must nest their fields with more spacing.
To stop defining an inner struct and continue defining the outer struct,
simply drop to the outer structs pacing level.

If a struct field is an anonymous array,
the `[=>]` may be followed with a singular name hint.
This is to aid generators that do want to de-anonymize the anonymous array.

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


If a struct is top level,
if the struct is a request type,
the first two modifiers in order must be `key` and `max version`.
These correspond to the request key and max version of that request supported.
A top level request type also supports the modifiers
`admin`, `group coordinator`, or `txn coordinator`.
These three modifiers signify whether the request needs to go to the current
coordinator,
group coordinator,
or transaction coordinator.

If the request is a response type,
there must be no modifier,
and the response type must have the same name (`s/Request/Response/`)
as the request it is for,
and it must follow the request in the `DEFINITIONS` file.
This is required to enforce that responses are tied to their requests appropriately.

Examples:
```
Record => not top level
  Key: bytes
  Value: bytes

ProduceRequest => key 0, max version 10
  Records: [Record]

ProduceResponse =>
  ErrorCode: int16

DeleteTopicRequest => key 1, max version 2, admin
  Topic: string

DeleteTopicResponse =>
  ErrorCode: int16

JoinGroupRequest => key 2, max version 3, group coordinator
  GroupID: string

JoinGroupResponse =>
  ErrorCode: int16

TxnCommitRequest => key 3, max version 8, txn coordinator
  TxnID: string

TxnCommitResponse =>
  ErrorCode: int16
```

Miscellaneous
-------------

- Spacing is important.
- Between every field name and type, there must be only one space.
- Between every word / symbol / number, there must be only one space.
- Lines cannot have trailing spaces.
- Internal struct fields must be nested two more spaces than the encompassing struct.
- There must be one blank line between type definitions.
