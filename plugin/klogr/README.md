klogr
---

klogr is a plug-in package to hook [go-logr](https://github.com/go-logr/logr) in a [`kgo.Logger`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Logger)

To use,

```go
cl, err := kgo.NewClient(
        kgo.WithLogger(klogr.New(logger)),
        // ...other opts
)
```