//go:build none

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kversion"
)

func main() {
	// Configure log level from flag
	var logLevelStr string
	var versionStr string
	flag.StringVar(&logLevelStr, "log-level", "none", "Log level: none, error, warn, info, debug")
	flag.StringVar(&logLevelStr, "l", "none", "Log level (shorthand)")
	flag.StringVar(&versionStr, "as-version", "", "Kafka version to emulate (e.g., 2.8, 3.5)")
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

	opts := []kfake.Opt{
		kfake.Ports(9092, 9093, 9094),
		kfake.SeedTopics(-1, "foo"),
		kfake.WithLogger(kfake.BasicLogger(os.Stderr, logLevel)),
	}
	if versionStr != "" {
		v := kversion.FromString(versionStr)
		if v == nil {
			fmt.Fprintf(os.Stderr, "unknown version %q; valid versions: %v\n", versionStr, kversion.VersionStrings())
			os.Exit(1)
		}
		opts = append(opts, kfake.MaxVersions(v))
	}
	c, err := kfake.NewCluster(opts...)
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
