klogrus
===

klogrus is a plug-in package to use [logrus](https://github.com/sirupsen/logrus) as a [`kgo.Logger`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Logger)

To use,

```go
cl, err := kgo.NewClient(
        kgo.WithLogger(klogrus.New(logrusLogger)),
        // ...other opts
)
```