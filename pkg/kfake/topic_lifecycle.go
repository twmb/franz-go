package kfake

import (
	"fmt"
)

// CreateTopic creates a topic with the given configs, nil for none. A topic
// deleted and created again under the same name gets a new topic ID.
func (c *Cluster) CreateTopic(topic string, partitions int32, configs map[string]string) error {
	var err error
	c.admin(func() {
		if _, ok := c.data.tps.gett(topic); ok {
			err = fmt.Errorf("topic %s already exists", topic)
			return
		}
		if norm := normalizeTopicName(topic); c.data.tnorms[norm] != "" {
			err = fmt.Errorf("topic %s collides with existing topic %s", topic, c.data.tnorms[norm])
			return
		}
		nparts := int(partitions)
		if nparts <= 0 {
			nparts = c.cfg.defaultNumParts
		}
		cfgs := make(map[string]*string, len(configs))
		for k, v := range configs {
			cfgs[k] = &v
		}
		c.data.mkt(topic, nparts, min(3, len(c.bs)), cfgs)
		c.notifyTopicChange()
		c.refreshCompactTicker()
		c.persistTopicsState()
	})
	return err
}

// DeleteTopic deletes a topic. Same as Kafka, the topic's commits are dropped
// from every group before the delete finishes.
func (c *Cluster) DeleteTopic(topic string) error {
	var err error
	c.admin(func() {
		t, ok := c.data.tps.gett(topic)
		if !ok {
			err = fmt.Errorf("topic %s does not exist", topic)
			return
		}
		for _, pd := range t {
			for watch := range pd.watch {
				watch.deleted()
			}
		}
		id := c.data.t2id[topic]
		for p, pd := range t {
			pd.closeAllFiles(false)
			pdir := partDir(c.storageDir, topic, p)
			if rmErr := c.fs.RemoveAll(pdir); rmErr != nil {
				c.cfg.logger.Logf(LogLevelWarn, "delete topic %s partition %d dir: %v", topic, p, rmErr)
			}
		}
		delete(c.data.tps, topic)
		delete(c.data.id2t, id)
		delete(c.data.t2id, topic)
		delete(c.data.treplicas, topic)
		delete(c.data.tcfgs, topic)
		delete(c.data.tnorms, normalizeTopicName(topic))
		// Producer state is per-log and dies with the topic.
		// Share-partition state is topic-ID-keyed on a real broker but
		// name-keyed here, so we clear it explicitly.
		for _, pidinf := range c.pids.ids {
			delete(pidinf.windows, topic)
		}
		for _, sg := range c.shareGroups.gs {
			delete(sg.partitions, topic)
		}
		c.dropGroupCommits(topic)
		c.notifyTopicChange()
		c.refreshCompactTicker()
		c.persistTopicsState()
	})
	return err
}

// DeleteRecords truncates the partition to offset, as a DeleteRecords
// request would. An offset of -1 truncates to the high watermark.
func (c *Cluster) DeleteRecords(topic string, partition int32, offset int64) error {
	var err error
	c.admin(func() {
		pd, ok := c.data.tps.getp(topic, partition)
		if !ok {
			err = fmt.Errorf("topic %s partition %d does not exist", topic, partition)
			return
		}
		to := offset
		if to == -1 {
			to = pd.highWatermark
		}
		if to < pd.logStartOffset || to > pd.highWatermark {
			err = fmt.Errorf("offset %d is outside [%d, %d]", to, pd.logStartOffset, pd.highWatermark)
			return
		}
		pd.logStartOffset = to
		c.trimLeft(pd)
	})
	return err
}
