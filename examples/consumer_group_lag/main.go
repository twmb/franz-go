// This example demonstrates how to monitor consumer group lag using kadm.Lag.
// It periodically prints per-topic, per-partition lag for the specified groups.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	groups      = flag.String("groups", "", "comma delimited consumer groups to monitor")
	interval    = flag.Duration("interval", 5*time.Second, "polling interval")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	if *groups == "" {
		die("missing required -groups flag\n")
	}
	groupList := strings.Split(*groups, ",")

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
	)
	if err != nil {
		die("unable to create client: %v\n", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	defer adm.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	printLag(ctx, adm, groupList)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printLag(ctx, adm, groupList)
		}
	}
}

func printLag(ctx context.Context, adm *kadm.Client, groups []string) {
	lags, err := adm.Lag(ctx, groups...)
	if err != nil {
		fmt.Printf("error fetching lag: %v\n", err)
		return
	}

	lags.Each(func(l kadm.DescribedGroupLag) {
		if err := l.Error(); err != nil {
			fmt.Printf("group %s: error: %v\n", l.Group, err)
			return
		}
		fmt.Printf("group %s (state=%s, protocol=%s, total lag=%d):\n", l.Group, l.State, l.Protocol, l.Lag.Total())
		for _, tl := range l.Lag.TotalByTopic().Sorted() {
			fmt.Printf("  topic %s: lag %d\n", tl.Topic, tl.Lag)
		}
		for _, ml := range l.Lag.Sorted() {
			memberID := "(unassigned)"
			if ml.Member != nil {
				memberID = ml.Member.MemberID
			}
			if ml.Err != nil {
				fmt.Printf("  %s p%d: error: %v\n", ml.Topic, ml.Partition, ml.Err)
			} else {
				fmt.Printf("  %s p%d: lag=%d committed=%d end=%d member=%s\n",
					ml.Topic, ml.Partition, ml.Lag, ml.Commit.At, ml.End.Offset, memberID)
			}
		}
		fmt.Println()
	})
}
