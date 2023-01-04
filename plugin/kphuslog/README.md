kphuslog
========

kphuslog is a plug-in package to hook [phuslu/log](https://github.com/phuslu/log)
into a [`kgo.Logger`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Logger)

To use,

```go
cl, err := kgo.NewClient(
        kgo.WithLogger(kphuslog.New(logger)),
        // ...other opts
)
```
