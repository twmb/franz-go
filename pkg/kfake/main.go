//go:build none

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/twmb/franz-go/pkg/kfake"
)

func main() {
	// Configure log level from flag
	var logLevelStr string
	flag.StringVar(&logLevelStr, "log-level", "none", "Log level: none, error, warn, info, debug")
	flag.StringVar(&logLevelStr, "l", "none", "Log level (shorthand)")
	flag.Parse()

	logLevel := kfake.LogLevelNone
	switch strings.ToLower(logLevelStr) {
	case "debug":
		logLevel = kfake.LogLevelDebug
	case "info":
		logLevel = kfake.LogLevelInfo
	case "warn":
		logLevel = kfake.LogLevelWarn
	case "error":
		logLevel = kfake.LogLevelError
	case "none":
		logLevel = kfake.LogLevelNone
	}

	c, err := kfake.NewCluster(
		kfake.Ports(9092, 9093, 9094),
		kfake.SeedTopics(-1, "foo"),
		kfake.WithLogger(kfake.BasicLogger(os.Stderr, logLevel)),
	)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	addrs := c.ListenAddrs()
	for _, addr := range addrs {
		fmt.Println(addr)
	}

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt)
	<-sigs
}
