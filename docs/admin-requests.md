# Admin Requests

This repo provides two ways to issue admin requests: a high level [kadm][a]
package, and a low level [kmsg][b] package. The kadm package is an opinionated
package that attempts to abstract the low level details of administrating into
some intuitive methods and types that have many helper functions. If you see a
type that could have more helper functions, please create a pull request.

[a]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm
[b]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kmsg

Not all of the low level API can be encapsulated in a high level API, and the
kadm package must sacrifice some details to make things easier to use. If the
kadm package does not give you the control you need, the low level kmsg package
allows you to construct requests to Kafka directly.

All Kafka requests and responses are supported through generated code in the
kmsg package. The package aims to provide some relatively comprehensive
documentation (at least more than Kafka itself provides), but may be lacking
in some areas.

Whenever using a struct from kmsg, create it with a `New` function. This calls
`Default` on fields that have defaults, allowing you to avoid specifying
everything all of the time. This makes your usage of kmsg more future proof,
because any struct within that package can have fields added to it as Kafka
adds fields.

It is also recommended to consider using the kgo package's [`MaxVersions`][1]
option. This will pin requests to a maximum version that you know you are in
control of, which will avoid any field-addition issues as the client version
advances.

[1]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#MaxVersions

It is recommended to always set all fields of a request. If you are talking to
a broker that does not support all fields you intend to use, those fields are
silently not written to that broker. It is recommended to ensure your brokers
support what you are expecting to send them. If you want to absolutely ensure
that some fields will not be ignored, you can use the [`MinVersions`][2]
option. With this, if you try to write to a broker that does not support the
version of a request you are writing, the client will not issue the request and
will return a relevant error.

[2]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#MinVersions

To issue a kmsg request, use the client's [`Request`][3] or
[`RequestSharded`][4] functions. The sharded requests is useful for any request
that internally can be split and issued to many brokers (`ListOffsets`,
`ListGroups`, etc.).  Both functions are pretty overpowered; see the
documentation on both for details.

[3]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.Request
[4]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.RequestSharded

As an example of issuing a `MetadataRequest`,

```go
topics := []string{"foo", "bar"}

req := kmsg.NewMetadataRequest()
for _, topic := range topics {
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, reqTopic)
}

resp, err := req.RequestWith(ctx, client)

// handle response...
```
