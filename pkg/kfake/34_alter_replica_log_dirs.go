package kfake

import (
	"slices"
	"strconv"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// AlterReplicaLogDirs: v0-2
//
// Behavior:
// * Moves this broker's replica of each partition to one of its log dirs
//   (log.dirs, else log.dir); other brokers keep theirs where they are
// * kfake stores the directory but doesn't actually move data
//
// Version notes:
// * v1: ThrottleMillis
// * v2: Flexible versions

func init() { regKey(34, 0, 2) }

func (c *Cluster) handleAlterReplicaLogDirs(creq *clientReq) (kmsg.Response, error) {
	var (
		b    = creq.cc.b
		req  = creq.kreq.(*kmsg.AlterReplicaLogDirsRequest)
		resp = req.ResponseKind().(*kmsg.AlterReplicaLogDirsResponse)
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if e := c.denyCluster(creq, kmsg.ACLOperationAlter, faultKey{}); e != nil {
		// Return cluster authorization failed for all partitions
		for _, rd := range req.Dirs {
			for _, t := range rd.Topics {
				st := kmsg.NewAlterReplicaLogDirsResponseTopic()
				st.Topic = t.Topic
				for _, p := range t.Partitions {
					sp := kmsg.NewAlterReplicaLogDirsResponseTopicPartition()
					sp.Partition = p
					sp.ErrorCode = e.Code
					st.Partitions = append(st.Partitions, sp)
				}
				resp.Topics = append(resp.Topics, st)
			}
		}
		return resp, nil
	}

	tidx := make(map[string]int)
	donet := func(t string) *kmsg.AlterReplicaLogDirsResponseTopic {
		if i, ok := tidx[t]; ok {
			return &resp.Topics[i]
		}
		tidx[t] = len(resp.Topics)
		st := kmsg.NewAlterReplicaLogDirsResponseTopic()
		st.Topic = t
		resp.Topics = append(resp.Topics, st)
		return &resp.Topics[len(resp.Topics)-1]
	}
	donep := func(t string, p int32, errCode int16) {
		sp := kmsg.NewAlterReplicaLogDirsResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		st := donet(t)
		st.Partitions = append(st.Partitions, sp)
	}

	// Kafka answers a partition named more than once with one entry,
	// moving it to the last dir named.
	var order []tp
	dests := make(map[tp]string)
	for _, rd := range req.Dirs {
		for _, t := range rd.Topics {
			for _, p := range t.Partitions {
				k := tp{t.Topic, p}
				if _, ok := dests[k]; !ok {
					order = append(order, k)
				}
				dests[k] = rd.Dir
			}
		}
	}

	dirs := c.logDirs()
	var moved bool
	for _, k := range order {
		dir := dests[k]
		pd, ok := c.data.tps.getp(k.t, k.p)
		misrouted := ok && !c.hostsReplica(b, k.t, pd)
		if e := creq.faults.check(faultKey{topic: k.t, resource: dir, misrouted: misrouted}.part(k.p)); e != nil {
			donep(k.t, k.p, e.Code)
			continue
		}
		switch {
		case len(k.t)+len(strconv.Itoa(int(k.p))) > 214:
			// Kafka's name for the moved log, the topic and
			// partition plus 41 characters, must fit in 255.
			donep(k.t, k.p, kerr.InvalidTopicException.Code)
		case !slices.Contains(dirs, dir):
			donep(k.t, k.p, kerr.LogDirNotFound.Code)
		case !ok:
			donep(k.t, k.p, kerr.UnknownTopicOrPartition.Code)
		case misrouted:
			donep(k.t, k.p, kerr.ReplicaNotAvailable.Code)
		default:
			if pd.dirs == nil {
				pd.dirs = make(map[int32]string)
			}
			pd.dirs[b.node] = dir
			donep(k.t, k.p, 0)
			moved = true
		}
	}
	if moved {
		c.persistTopicsState()
	}

	return resp, nil
}
