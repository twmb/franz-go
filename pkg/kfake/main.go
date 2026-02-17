//go:build none

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kversion"
)

type brokerConfigFlag map[string]string

func (f brokerConfigFlag) String() string { return "" }

func (f brokerConfigFlag) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok {
		return fmt.Errorf("expected key=value, got %q", s)
	}
	f[k] = v
	return nil
}

func main() {
	var logLevelStr string
	var versionStr string
	bcfgs := make(brokerConfigFlag)
	flag.StringVar(&logLevelStr, "log-level", "none", "Log level: none, error, warn, info, debug")
	flag.StringVar(&logLevelStr, "l", "none", "Log level (shorthand)")
	flag.StringVar(&versionStr, "as-version", "", "Kafka version to emulate (e.g., 2.8, 3.5)")
	flag.Var(bcfgs, "broker-config", "Broker config key=value (repeatable)")
	flag.Var(bcfgs, "c", "Broker config key=value (shorthand, repeatable)")
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
	if len(bcfgs) > 0 {
		opts = append(opts, kfake.BrokerConfigs(bcfgs))
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
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	<-sigs
}
