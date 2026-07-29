import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Reads instance descriptions on stdin and prints the assignment Kafka's
 * StickyAssignor / CooperativeStickyAssignor produces for each.
 *
 * Input, line based, one instance per block:
 *   I &lt;instanceID&gt; &lt;sticky|coop&gt;
 *   T &lt;topic&gt; &lt;numPartitions&gt;
 *   M &lt;memberID&gt; &lt;generation&gt; &lt;t1,t2,...&gt; &lt;t:p,t:p,...|-&gt;
 *   E
 * terminated by a lone "Q".
 *
 * Output per instance:
 *   R &lt;instanceID&gt; &lt;rounds&gt;
 *   A &lt;memberID&gt; &lt;t:p,t:p,...|-&gt;
 *   E
 */
public final class Harness {

    static final class Member {
        String id;
        int generation;
        List<String> topics = new ArrayList<>();
        List<TopicPartition> owned = new ArrayList<>();
    }

    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out));

        String line;
        while ((line = in.readLine()) != null) {
            if (line.isEmpty()) continue;
            if (line.equals("Q")) break;
            if (!line.startsWith("I ")) throw new IllegalStateException("expected I, got: " + line);

            String[] head = line.split(" ");
            String instanceID = head[1];
            String kind = head[2];

            Map<String, Integer> partitionsPerTopic = new LinkedHashMap<>();
            List<Member> members = new ArrayList<>();

            while ((line = in.readLine()) != null && !line.equals("E")) {
                String[] f = line.split(" ");
                if (f[0].equals("T")) {
                    partitionsPerTopic.put(f[1], Integer.parseInt(f[2]));
                } else if (f[0].equals("M")) {
                    Member m = new Member();
                    m.id = f[1];
                    m.generation = Integer.parseInt(f[2]);
                    for (String t : f[3].split(",")) {
                        if (!t.isEmpty()) m.topics.add(t);
                    }
                    if (f.length > 4 && !f[4].equals("-")) {
                        for (String tp : f[4].split(",")) {
                            if (tp.isEmpty()) continue;
                            int i = tp.lastIndexOf(':');
                            m.owned.add(new TopicPartition(tp.substring(0, i),
                                    Integer.parseInt(tp.substring(i + 1))));
                        }
                    }
                    members.add(m);
                } else {
                    throw new IllegalStateException("bad line: " + line);
                }
            }

            // AbstractPartitionAssignor.assign(Cluster, GroupSubscription) only
            // ever passes topics somebody subscribes to, so mirror that here.
            Set<String> subscribed = new TreeSet<>();
            for (Member m : members) subscribed.addAll(m.topics);
            Map<String, Integer> visible = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> e : partitionsPerTopic.entrySet()) {
                if (subscribed.contains(e.getKey())) visible.put(e.getKey(), e.getValue());
            }

            Map<String, List<TopicPartition>> result;
            int rounds;
            if (kind.equals("sticky")) {
                result = assignEager(visible, members);
                rounds = 1;
            } else if (kind.equals("coop")) {
                int[] r = new int[1];
                result = assignCooperativeToFixedPoint(visible, members, r);
                rounds = r[0];
            } else {
                throw new IllegalStateException("unknown assignor kind: " + kind);
            }

            out.write("R " + instanceID + " " + rounds + "\n");
            for (Member m : members) {
                List<TopicPartition> got = result.get(m.id);
                out.write("A " + m.id + " " + fmt(got) + "\n");
            }
            out.write("E\n");
            out.flush();
        }
        out.flush();
    }

    /**
     * The eager path: every member revokes everything and republishes its prior
     * assignment as subscription userdata, which is exactly how a real consumer
     * running StickyAssignor rejoins. The userdata bytes are produced by the
     * assignor itself (onAssignment then subscriptionUserData) rather than by
     * hand, so the encoding cannot drift from what Kafka expects.
     */
    static Map<String, List<TopicPartition>> assignEager(Map<String, Integer> partitionsPerTopic,
                                                         List<Member> members) {
        Map<String, Subscription> subscriptions = new LinkedHashMap<>();
        for (Member m : members) {
            ByteBuffer userData = null;
            if (!m.owned.isEmpty()) {
                StickyAssignor encoder = new StickyAssignor();
                encoder.onAssignment(new Assignment(new ArrayList<>(m.owned)),
                        new ConsumerGroupMetadata("g", m.generation, m.id, Optional.empty()));
                userData = encoder.subscriptionUserData(new TreeSet<>(m.topics));
            }
            subscriptions.put(m.id, new Subscription(new ArrayList<>(m.topics), userData));
        }
        // A fresh assignor per instance: maxGeneration and partitionMovements
        // are instance fields that would otherwise carry across instances.
        return new StickyAssignor().assign(partitionsPerTopic, subscriptions);
    }

    /**
     * The cooperative path returns a deliberately incomplete assignment in any
     * round where a partition changes hands: adjustAssignment strips the
     * partitions the new owner may not take until the old owner revokes them.
     * Scoring that intermediate state against a complete assignment would be
     * meaningless, so drive the protocol the way a group does -- rejoin with
     * the partitions still held, in a new generation -- until it stops moving.
     */
    static Map<String, List<TopicPartition>> assignCooperativeToFixedPoint(Map<String, Integer> partitionsPerTopic,
                                                                           List<Member> members,
                                                                           int[] roundsOut) {
        Map<String, List<TopicPartition>> owned = new LinkedHashMap<>();
        Map<String, Integer> generations = new LinkedHashMap<>();
        for (Member m : members) {
            owned.put(m.id, new ArrayList<>(m.owned));
            generations.put(m.id, m.generation);
        }

        Map<String, List<TopicPartition>> result = null;
        int round = 0;
        for (; round < 20; round++) {
            Map<String, Subscription> subscriptions = new LinkedHashMap<>();
            for (Member m : members) {
                // CooperativeStickyAssignor.memberData reads ownedPartitions and
                // generationId off the Subscription directly (the v2+ protocol
                // fields), so no userdata is involved.
                subscriptions.put(m.id, new Subscription(
                        new ArrayList<>(m.topics),
                        null,
                        new ArrayList<>(owned.get(m.id)),
                        generations.get(m.id),
                        Optional.empty()));
            }
            // assign() is inherited from AbstractStickyAssignor and calls
            // assignPartitions virtually, so the cooperative override (which
            // trims partitions awaiting revocation) is the one that runs.
            result = new CooperativeStickyAssignor().assign(partitionsPerTopic, subscriptions);

            boolean changed = false;
            for (Member m : members) {
                List<TopicPartition> got = result.get(m.id);
                if (got == null) got = Collections.emptyList();
                if (!new TreeSet<>(strs(got)).equals(new TreeSet<>(strs(owned.get(m.id))))) changed = true;
            }
            if (!changed) break;
            for (Member m : members) {
                List<TopicPartition> got = result.get(m.id);
                owned.put(m.id, got == null ? new ArrayList<>() : new ArrayList<>(got));
                generations.put(m.id, generations.get(m.id) + 1);
            }
        }
        roundsOut[0] = round + 1;
        return result;
    }

    static List<String> strs(List<TopicPartition> l) {
        List<String> s = new ArrayList<>(l.size());
        for (TopicPartition tp : l) s.add(tp.topic() + ":" + tp.partition());
        return s;
    }

    static String fmt(List<TopicPartition> l) {
        if (l == null || l.isEmpty()) return "-";
        StringBuilder sb = new StringBuilder();
        for (TopicPartition tp : l) {
            if (sb.length() > 0) sb.append(',');
            sb.append(tp.topic()).append(':').append(tp.partition());
        }
        return sb.toString();
    }
}
