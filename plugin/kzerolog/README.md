kzerolog
========

kzerolog is a plug-in package to hook [zerolog](https://github.com/rs/zerolog)
into a [`kgo.Logger`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Logger)

To use,

```go
cl, err := kgo.NewClient(
        kgo.WithLogger(kzerolog.New(logger)),
        // ...other opts
)
```
