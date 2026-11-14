package kfake

import (
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DescribeLogDirs: v0-5
//
// Behavior:
// * Returns every log dir (log.dirs, else log.dir) with this broker's
//   replicas in it; a dir with none is still listed
// * If Topics is null, returns all of this broker's partitions
// * Returns in-memory size tracking, not actual disk usage
//
// Version notes:
// * v1: ThrottleMillis
// * v2: Flexible versions
// * v3: Top-level ErrorCode
// * v4: TotalBytes, UsableBytes
// * v5: IsCordoned (we never cordon a dir)

func init() { regKey(35, 0, 5) }

// logDirTotalBytes is the size we report for the volume of every log dir.
const logDirTotalBytes = 64 << 30

func (c *Cluster) handleDescribeLogDirs(creq *clientReq) (kmsg.Response, error) {
	var (
		b    = creq.cc.b
		req  = creq.kreq.(*kmsg.DescribeLogDirsRequest)
		resp = req.ResponseKind().(*kmsg.DescribeLogDirsResponse)
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if !c.allowedClusterACL(creq, kmsg.ACLOperationDescribe) {
		resp.ErrorCode = kerr.ClusterAuthorizationFailed.Code
		return resp, nil
	}
	// v0-2 has no top-level ErrorCode on the wire, so a request-level
	// fault goes on every dir instead. An ACL denial there answers no
	// dirs, as in Kafka, which is why we check the ACL apart from the
	// fault.
	allDirs := creq.faults.check(faultKey{})
	if allDirs != nil && req.Version >= 3 {
		resp.ErrorCode = allDirs.Code
		return resp, nil
	}

	var want map[tp]bool // nil means every partition
	if req.Topics != nil {
		want = make(map[tp]bool)
		for _, t := range req.Topics {
			for _, p := range t.Partitions {
				want[tp{t.Topic, p}] = true
			}
		}
	}

	// A dir's usable bytes count every replica in it, not only the ones
	// requested.
	dirs := c.logDirs()
	used := make(map[string]int64)
	sizes := make(map[string]map[string]map[int32]int64) // dir => topic => partition => size
	c.data.tps.each(func(t string, p int32, pd *partData) {
		if !c.hostsReplica(b, t, pd) {
			return
		}
		dir := pd.logDir(b.node, dirs)
		used[dir] += pd.nbytes
		if want != nil && !want[tp{t, p}] {
			return
		}
		ts, ok := sizes[dir]
		if !ok {
			ts = make(map[string]map[int32]int64)
			sizes[dir] = ts
		}
		ps, ok := ts[t]
		if !ok {
			ps = make(map[int32]int64)
			ts[t] = ps
		}
		ps[p] = pd.nbytes
	})

	for _, dir := range dirs {
		rd := kmsg.NewDescribeLogDirsResponseDir()
		rd.Dir = dir
		e := allDirs
		if e == nil {
			e = creq.faults.check(faultKey{resource: dir})
		}
		if e != nil {
			rd.ErrorCode = e.Code
			resp.Dirs = append(resp.Dirs, rd)
			continue
		}
		rd.TotalBytes = logDirTotalBytes
		rd.UsableBytes = max(logDirTotalBytes-used[dir], 0)
		for t, ps := range sizes[dir] {
			rt := kmsg.NewDescribeLogDirsResponseDirTopic()
			rt.Topic = t
			for p, s := range ps {
				rp := kmsg.NewDescribeLogDirsResponseDirTopicPartition()
				rp.Partition = p
				rp.Size = s
				rt.Partitions = append(rt.Partitions, rp)
			}
			rd.Topics = append(rd.Topics, rt)
		}
		resp.Dirs = append(resp.Dirs, rd)
	}

	return resp, nil
}

// logDirs returns the log dirs every broker has: log.dirs if set, else
// log.dir. A dir is only a name: partDir does not use it, so we keep one
// copy of a partition's data whatever dir a broker reports it in.
func (c *Cluster) logDirs() []string {
	for _, k := range []string{"log.dirs", "log.dir"} {
		v := c.bcfgs[k]
		if v == nil {
			continue
		}
		var dirs []string
		for _, d := range strings.Split(*v, ",") {
			if d = strings.TrimSpace(d); d != "" && !slices.Contains(dirs, d) {
				dirs = append(dirs, d)
			}
		}
		if len(dirs) > 0 {
			return dirs
		}
	}
	return []string{defLogDir}
}

// logDir returns the dir the broker with the given node ID keeps its replica
// of pd in: the dir it last moved it to, or the first configured dir.
func (pd *partData) logDir(node int32, dirs []string) string {
	if d, ok := pd.dirs[node]; ok && slices.Contains(dirs, d) {
		return d
	}
	return dirs[0]
}

// hostsReplica reports whether b holds a replica of pd: the leader and the
// brokers after it, as Metadata reports the replicas. A broker RemoveNode
// removed holds none.
func (c *Cluster) hostsReplica(b *broker, t string, pd *partData) bool {
	n := len(c.bs)
	return b.bsIdx < n && c.bs[b.bsIdx] == b && (b.bsIdx-pd.leader.bsIdx+n)%n < min(c.data.treplicas[t], n)
}
