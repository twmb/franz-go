# Message requests

With franz-go you can directly construct message requests and send them to your Kafka cluster. There are three options
how you can issue requests:

- Using the broker's `Request` method
- Using the client's `Request` method
- Using the message's `RequestWith` method

## Broker Requests

**Interface:**

```go
func (b *Broker) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error)
```

Reference:  https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Broker.Request

Use this method only if you need to send requests to a specific broker. The actual response type has to be asserted.
Requests sent using this method are not retried.


## Client Requests

```go
func (cl *Client) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error)
```

Reference: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.Request

The client provides a lot functionality making sure that your message request will be sent in the most efficient manner
to the right set of brokers. Additionally it will retry your requests if needed. The actual response type has to be
asserted.

## Message Requests

```go
// Example for ListOffsetsRequest
func (v *ListOffsetsRequest) RequestWith(ctx context.Context, r Requestor) (*ListOffsetsResponse, error)
```

Reference: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kmsg

Each request message in the `kmsg` package has it's own `RequestWith` method which accepts a context and an interface
which `Client` already fulfills. This method uses the client's `Request` method with the advantage that you don't
need to assert the actual response type.

Most commonly you want to use this method to send requests to Kafka.
