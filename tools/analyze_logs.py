#!/usr/bin/env python3
"""Analyze franz-go / kfake test logs.

Subcommands:

  analyze_logs.py client-trace <file> [options]   Trace client debug log events
  analyze_logs.py dual-assign [options]            Diagnose 848 dual-assignment bugs
  analyze_logs.py etl-stuck [options]              Diagnose stuck ETL chain tests
  analyze_logs.py 848-state --groups X [options]   848 group convergence analysis
  analyze_logs.py commit-timeline --groups X       Per-group commit rate timeline
  analyze_logs.py txn-timeout [options]            Transaction timeout root cause
  analyze_logs.py txn-produce [options]            Per-PID produce trace
  analyze_logs.py produce-order [options]           Produce offset order failures
  analyze_logs.py server-stall [options]           Server stall analysis
  analyze_logs.py lso-state [options]              LSO vs HWM per topic
  analyze_logs.py goroutine-dump [options]         Goroutine dump classification
  analyze_logs.py txn-etl [options]                Txn commit/abort analysis per ETL group

Run 'analyze_logs.py <command> --help' for per-command options.

Examples:
  analyze_logs.py client-trace logs.csv -p 118 -s 15000 -e 17000
  analyze_logs.py dual-assign
  analyze_logs.py dual-assign -p 5 --server-only
  analyze_logs.py etl-stuck
  analyze_logs.py etl-stuck -g 89429164
  analyze_logs.py 848-state --groups abc123,def456
  analyze_logs.py 848-state --groups abc123 --after 10:30:00
"""

import argparse
import re
import sys
from collections import defaultdict


def parse_ts(t):
    """Parse a HH:MM:SS.mmm timestamp to seconds."""
    parts = t.split(':')
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])


def extract_partition_fetch_info(line, partition):
    """Extract detailed fetch response info for a specific partition.

    Finds the Partition:N section in a Fetch response, extracts all batch
    BaseOffsets and NumRecords to compute the full range and record count.
    Handles log lines where the partition section may be very large.
    """
    idx = line.find(f'Partition:{partition}')
    if idx == -1:
        return None
    # Find next partition boundary to isolate this partition's data
    next_p = line.find('{Partition:', idx + 12)
    section = line[idx:next_p] if next_p > 0 else line[idx:idx+500000]

    hwm = re.search(r'HighWatermark:([-\d]+)', section)
    lso = re.search(r'LastStableOffset:([-\d]+)', section)
    err = re.search(r'ErrorCode:(\d+)', section)

    # Check for empty batches
    if 'RecordBatches:[]' in section or 'RecordBatches:nil' in section:
        return {
            'empty': True,
            'hwm': hwm.group(1) if hwm else '?',
            'lso': lso.group(1) if lso else '?',
            'err': err.group(1) if err else '?',
        }

    offsets = re.findall(r'BaseOffset:(\d+)', section)
    num_records = re.findall(r'NumRecords:(\d+)', section)
    if not offsets:
        return {
            'empty': True,
            'hwm': hwm.group(1) if hwm else '?',
            'lso': lso.group(1) if lso else '?',
            'err': err.group(1) if err else '?',
        }

    total = sum(int(n) for n in num_records)
    last_end = int(offsets[-1]) + int(num_records[-1]) if num_records else 0
    return {
        'empty': False,
        'first': int(offsets[0]),
        'last_end': last_end,
        'batches': len(offsets),
        'recs': total,
        'hwm': hwm.group(1) if hwm else '?',
        'lso': lso.group(1) if lso else '?',
        'err': err.group(1) if err else '?',
    }


def classify_line(line, partition):
    """Classify a log line into semantic event categories.

    Returns a list of (event_string, category) tuples. Categories are used
    for filtering (--fetch-only, --session-only).

    Categories:
      'txn'     - Transaction lifecycle events
      'group'   - Consumer group session events
      'fetch'   - Fetch request/response events
      'offset'  - Offset fetch/commit events
      'other'   - Other notable events
    """
    events = []
    p = partition

    # --- Transaction lifecycle ---
    if 'transact session' in line:
        if 'beginning transact session' in line:
            events.append(('TXN_BEGIN', 'txn'))
        elif 'resetting to current committed state' in line:
            m = re.search(rf'{p}:\{{(\d+) (\d+)\}}', line)
            offset_str = f" p{p}={{e{m.group(1)} o{m.group(2)}}}" if m else ""
            events.append((f'TXN_RESET{offset_str}', 'txn'))
        elif 'successful' in line:
            m = re.search(rf'{p}:\{{(\d+) (\d+)\}}', line)
            offset_str = f" p{p}={{e{m.group(1)} o{m.group(2)}}}" if m else f" p{p}=NOT_IN_COMMIT"
            events.append((f'TXN_SUCCESS{offset_str}', 'txn'))
        elif 'session ending' in line:
            m = re.search(r'will_try_commit: (\w+)', line)
            commit = m.group(1) if m else '?'
            failed = re.search(r'was_failed: (\w+)', line)
            failed_s = failed.group(1) if failed else '?'
            events.append((f'TXN_ENDING will_commit={commit} was_failed={failed_s}', 'txn'))
        elif 'on_revoke' in line:
            if 'allowing next commit' in line:
                events.append(('TXN_REVOKE_ALLOW', 'txn'))
            else:
                events.append(('TXN_REVOKE_ABORT', 'txn'))
        elif 'on_lost' in line:
            events.append(('TXN_LOST', 'txn'))

    if 'beginning transaction' in line and 'INFO' in line:
        events.append(('BEGIN_TXN', 'txn'))

    if 'end transaction response' in line:
        m = re.search(r'commit: (\w+)', line)
        commit = m.group(1) if m else '?'
        events.append((f'END_TXN commit={commit}', 'txn'))

    if 'aborted buffered records' in line:
        events.append(('ABORT_BUFFERED', 'txn'))

    if 'flushing' in line and 'INFO' in line:
        events.append(('FLUSHING', 'txn'))

    # --- Consumer group events ---
    if 'new group session begun' in line:
        has_p = f'{p}:' in line or f' {p} ' in line
        m = re.search(r'added: (map\[[^\]]*\])', line)
        added = m.group(1)[:120] if m else '?'
        events.append((f'GROUP_SESSION has_p{p}={has_p} added={added}', 'group'))

    if 'assigning partitions' in line:
        m = re.search(r'how: (\w+)', line)
        how = m.group(1) if m else '?'
        m2 = re.search(r'why: ([^,}"]+)', line)
        why = m2.group(1)[:80] if m2 else ''
        events.append((f'ASSIGN how={how} {why}', 'group'))

    if 'newly fetched offsets' in line:
        m = re.search(rf'{p}:\{{([^}}]*)\}}', line)
        offset_str = f" p{p}={{{m.group(1)}}}" if m else ""
        events.append((f'OFFSETS_FETCHED{offset_str}', 'group'))

    if 'assign requires loading offsets' in line:
        events.append(('LOADING_OFFSETS', 'group'))

    if 'beginning heartbeat' in line:
        events.append(('HEARTBEAT_BEGIN', 'group'))

    if 'consumer calling onRevoke' in line:
        events.append(('ON_REVOKE_SESSION_END', 'group'))

    if 'leaving group' in line.lower() or 'LeaveGroup' in line:
        if 'assigning' not in line:  # avoid double-counting with ASSIGN
            events.append(('LEAVE_GROUP', 'group'))

    if 'heartbeat returned' in line and 'RebalanceInProgress' in line:
        events.append(('HB_REBALANCE', 'group'))

    if 'sleeping 200ms' in line:
        events.append(('SLEEP_200MS', 'group'))

    # --- Uncommitted updates ---
    if 'updated uncommitted' in line:
        m = re.search(rf'{p}\{{(\d+)=>(\d+) r(\d+)', line)
        if m:
            events.append((f'UNCOMMITTED p{p} {m.group(1)}=>{m.group(2)} r{m.group(3)}', 'offset'))

    # --- User annotations ---
    if '// HERE' in line or '//HERE' in line:
        events.append((f'*** {line.strip()[:140]}', 'other'))

    return events


def process_wire_event(line, partition, all_partitions=False):
    """Extract wire-level (kafka WRITE/READ) events for the target partition.

    For Fetch requests: extracts FetchOffset, SessionID/Epoch, correlation ID
    For Fetch responses: extracts batch info (BaseOffset range, record count, HWM/LSO)
    For OffsetFetch/TxnOffsetCommit: extracts offset for target partition
    """
    events = []
    if 'kafka WRITE' not in line and 'kafka READ' not in line:
        return events

    rw = 'W' if 'WRITE' in line else 'R'
    # CSV files may use doubled quotes ("") instead of escaped quotes (\")
    api_m = re.search(r'api"*:"*"*([A-Za-z]+ v\d+)', line)
    api = api_m.group(1) if api_m else '?'
    corr_m = re.search(r'corr"*:(\d+)', line)
    corr = corr_m.group(1) if corr_m else '?'

    has_partition = f'Partition:{partition}' in line

    # Fetch requests
    if 'Fetch' in api and 'Offset' not in api:
        if rw == 'W' and has_partition:
            m = re.search(rf'Partition:{partition}[^}}]*FetchOffset:(\d+)', line)
            fo = m.group(1) if m else '?'
            sid = re.search(r'SessionID:(\d+) SessionEpoch:([-\d]+)', line)
            sid_s = f'sid={sid.group(1)}e{sid.group(2)}' if sid else '?'
            events.append((f'FETCH_REQ p{partition} offset={fo} corr={corr} {sid_s}', 'fetch'))
        elif rw == 'R' and has_partition:
            info = extract_partition_fetch_info(line, partition)
            if info and info['empty']:
                hwm_s = f"HWM={info['hwm']}" if info['hwm'] != '?' else ''
                lso_s = f"LSO={info['lso']}" if info['lso'] != '?' else ''
                events.append((f'FETCH_RESP p{partition} EMPTY corr={corr} {hwm_s} {lso_s}', 'fetch'))
            elif info:
                events.append((
                    f'FETCH_RESP p{partition} base={info["first"]}->{info["last_end"]} '
                    f'b={info["batches"]} r={info["recs"]} corr={corr} HWM={info["hwm"]}',
                    'fetch'
                ))

    # OffsetFetch responses
    elif 'OffsetFetch' in api and rw == 'R' and has_partition:
        m = re.search(rf'Partition:{partition}[^}}]*Offset:(\d+)', line)
        le = re.search(rf'Partition:{partition}[^}}]*LeaderEpoch:([-\d]+)', line)
        if m:
            epoch_s = f" epoch={le.group(1)}" if le else ""
            events.append((f'OFFSET_FETCH_RESP p{partition} offset={m.group(1)}{epoch_s} corr={corr}', 'offset'))

    # TxnOffsetCommit
    elif 'TxnOffsetCommit' in api and has_partition:
        m = re.search(rf'Partition:{partition}[^}}]*Offset:(\d+)', line)
        if m:
            if rw == 'W':
                events.append((f'TXN_OFFSET_COMMIT p{partition} offset={m.group(1)} corr={corr}', 'offset'))
            else:
                err = re.search(rf'Partition:{partition}[^}}]*ErrorCode:(\d+)', line)
                err_s = f" err={err.group(1)}" if err else ""
                events.append((f'TXN_OFFSET_COMMIT_RESP p{partition}{err_s} corr={corr}', 'offset'))

    # OffsetForLeaderEpoch
    elif 'OffsetForLeaderEpoch' in api and has_partition:
        events.append((f'EPOCH_VALIDATE {"REQ" if rw == "W" else "RESP"} p{partition} corr={corr}', 'fetch'))

    return events


def main_csv(args):
    """Trace client debug log events.

    Parses franz-go client debug logs and extracts a timeline of events
    for a specific partition: transaction lifecycle, group sessions,
    fetch requests/responses, offset commits, and uncommitted offset
    updates. Useful for debugging produce/consume correctness and
    transaction semantics.
    """

    with open(args.log_file) as f:
        lines = f.readlines()

    end = args.end if args.end > 0 else len(lines)
    partition = args.partition

    # Determine which categories to show
    show_cats = {'txn', 'group', 'fetch', 'offset', 'other'}
    if args.fetch_only:
        show_cats = {'fetch'}
    elif args.session_only:
        show_cats = {'txn', 'group'}

    print(f"Analyzing partition {partition}, lines {args.start}-{end} ({len(lines)} total)")
    print(f"{'LINE':>6} {'TIME':>15} {'EVENT'}")
    print("-" * 120)

    for i in range(args.start - 1, min(end, len(lines))):
        line = lines[i]
        lineno = i + 1

        # Extract timestamp
        m = re.search(r'(\d{2}:\d{2}:\d{2}\.\d{3})Z', line)
        timestamp = m.group(1) if m else ''

        all_events = []

        # Classified events (transaction, group, uncommitted, annotations)
        all_events.extend(classify_line(line, partition))

        # Wire events (fetch, offset fetch, txn offset commit)
        all_events.extend(process_wire_event(line, partition, args.all_partitions))

        # Print matching events
        for event, cat in all_events:
            if cat in show_cats:
                print(f"{lineno:>6} {timestamp:>15} {event}")


# ---------------------------------------------------------------------------
# KIP-848 dual-assignment analysis (kfake server + kgo client INFO logs)
# ---------------------------------------------------------------------------

def detect_failure(client_lines):
    """Find 'saw double offset' lines and extract topic, partition, offset."""
    failures = []
    for line in client_lines:
        m = re.search(r'saw double offset t (\S+) p(\d+)o(\d+)', line)
        if m:
            failures.append({
                'topic': m.group(1),
                'partition': int(m.group(2)),
                'offset': int(m.group(3)),
            })
    return failures


def find_group_for_duplicate(client_lines, topic, partition, offset):
    """Find the group where the duplicate consumption occurred.

    Searches for 'newly fetched offsets' lines that contain the exact
    topic, partition, and offset from the failure. The group with 2+
    such lines is the one with the duplicate.
    """
    # Build a pattern that matches the partition with the exact offset,
    # e.g. "5{7119 " - the trailing space distinguishes "7119" from "71190".
    pat = re.compile(
        rf'group: ([a-f0-9]+).*newly fetched offsets.*'
        + re.escape(topic)
        + rf'.*\b{partition}\{{{offset} '
    )
    group_counts = defaultdict(int)
    for line in client_lines:
        m = pat.search(line)
        if m:
            group_counts[m.group(1)] += 1

    # Return the group with the most hits (should be >= 2 for a duplicate).
    if group_counts:
        return max(group_counts, key=group_counts.get)

    # Fallback: find any group that mentions this topic.
    for line in client_lines:
        if topic in line and 'group: ' in line:
            m = re.search(r'group: ([a-f0-9]+)', line)
            if m:
                return m.group(1)
    return ''


def trace_848_client(client_lines, group, topic, partition):
    """Build a timeline of client events for a partition in a group.

    Returns list of (timestamp, goroutine, member_id, event, details).
    """
    events = []
    p = str(partition)
    gor_member = {}

    for line in client_lines:
        if group not in line:
            continue
        m = re.match(r'\[(\d{2}:\d{2}:\d{2}\.\d+)\s+(\d+)\]\[(\w+)\]\s+(.*)', line)
        if not m:
            continue
        ts, gor, level, msg = m.group(1), int(m.group(2)), m.group(3), m.group(4)

        mid_m = re.search(r'member_id: (\S+)', msg)
        if mid_m and gor:
            gor_member[gor] = mid_m.group(1).rstrip(',')
        mid = gor_member.get(gor, '?')

        if 'initial heartbeat received assignment' in msg:
            gen_m = re.search(r'generation: (\d+)', msg)
            gen = gen_m.group(1) if gen_m else '?'
            events.append((ts, gor, mid, 'JOIN', f'gen={gen}'))

        if 'heartbeat detected an updated assignment' in msg and topic in msg:
            gen_m = re.search(r'generation: (\d+)', msg)
            gen = gen_m.group(1) if gen_m else '?'
            t_m = re.search(re.escape(topic) + r':\[([^\]]*)\]', msg)
            parts = t_m.group(1) if t_m else '?'
            has_p = p in parts.split() if t_m else False
            marker = ' <<<' if has_p else ''
            events.append((ts, gor, mid, 'ASSIGN', f'gen={gen} [{parts}]{marker}'))

        if 'new group session begun' in msg and topic in msg:
            added_m = re.search(r'added: .*?' + re.escape(topic) + r'\[([^\]]*)\]', msg)
            lost_m = re.search(r'lost: .*?' + re.escape(topic) + r'\[([^\]]*)\]', msg)
            added = added_m.group(1) if added_m else ''
            lost = lost_m.group(1) if lost_m else ''
            p_added = p in added.split() if added else False
            p_lost = p in lost.split() if lost else False
            if p_added or p_lost:
                events.append((ts, gor, mid, 'SESSION',
                    f'+[{added}] -[{lost}]'))

        if 'newly fetched offsets' in msg and topic in msg:
            p_m = re.search(rf'\b{p}\{{([^}}]+)\}}', msg)
            if p_m:
                events.append((ts, gor, mid, 'CONSUME_START', f'p{p}={{{p_m.group(1)}}}'))

        if 'revoking' in msg and topic in msg:
            p_m = re.search(rf'\b{p}\{{([^}}]+)\}}', msg)
            if p_m:
                events.append((ts, gor, mid, 'REVOKE', f'p{p}={{{p_m.group(1)}}}'))

    return events


def trace_848_server(server_lines, partition):
    """Build server-side RECONCILE/PRUNE/FENCE timeline for a partition."""
    events = []
    p = str(partition)

    for line in server_lines:
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
        if not m:
            continue
        ts, level, msg = m.group(1), m.group(2), m.group(3)

        if f'p={p} ' in msg or f'p={p}(' in msg or msg.endswith(f'p={p}'):
            mid_m = re.search(r'member=(\S+)', msg)
            mid = mid_m.group(1) if mid_m else '?'
            if 'RECONCILE CONFIRMED' in msg:
                events.append((ts, mid, 'CONFIRMED', msg))
            elif 'RECONCILE ALLOW' in msg:
                events.append((ts, mid, 'ALLOW', msg))
            elif 'RECONCILE BLOCK' in msg:
                kind = 'BLOCK_EPOCH' if 'epoch' in msg else 'BLOCK_SENT'
                events.append((ts, mid, kind, msg))
            elif 'PRUNE' in msg:
                events.append((ts, mid, 'PRUNE', msg))

        if 'FENCE' in msg:
            mid_m = re.search(r'member=(\S+)', msg)
            mid = mid_m.group(1) if mid_m else '?'
            events.append((ts, mid, 'FENCE', msg[:120]))

    return events


def find_dual_allows(server_events):
    """Find cases where two members both got ALLOW for the same partition
    without an intervening release (PRUNE or FENCE of the first member).

    Returns list of (ts1, member1, ts2, member2, detail) tuples.
    """
    last_allow = {}  # (topic, partition) -> (timestamp, member)
    conflicts = []

    for ts, mid, event, detail in server_events:
        tp_m = re.search(r'topic=(\S+)\s+p=(\d+)', detail)
        if not tp_m:
            # FENCE events don't have topic=.../p=... format.
            # Clear all entries for the fenced member.
            if event == 'FENCE':
                last_allow = {k: v for k, v in last_allow.items() if v[1] != mid}
            continue
        key = (tp_m.group(1), tp_m.group(2))

        if event == 'PRUNE':
            # Member released this partition - no longer the owner.
            if last_allow.get(key, (None, None))[1] == mid:
                del last_allow[key]
        elif event == 'ALLOW':
            if key in last_allow:
                prev_ts, prev_mid = last_allow[key]
                if prev_mid != mid:
                    conflicts.append((prev_ts, prev_mid, ts, mid,
                        f'topic={key[0]} p={key[1]}'))
            last_allow[key] = (ts, mid)

    return conflicts


def main_848(args):
    """KIP-848 dual-assignment analysis.

    Parses kfake server logs and kgo client logs to diagnose "saw double
    offset" failures in TestGroupETL/*/848. Auto-detects the failure,
    identifies the group and partition, and produces:

    - CLIENT TIMELINE: partition ownership changes (JOIN, ASSIGN,
      SESSION, CONSUME_START, REVOKE) for the failing partition.
    - SERVER TIMELINE: RECONCILE ALLOW/BLOCK, PRUNE, and FENCE events
      (requires server to be run with --server-log debug).
    - DUAL ALLOW CONFLICTS: cases where two members both got a partition
      without an intervening release - the smoking gun for the bug.
    """

    with open(args.client) as f:
        client_lines = f.readlines()
    with open(args.server) as f:
        server_lines = f.readlines()

    # Auto-detect failure
    failures = detect_failure(client_lines)
    if failures:
        f = failures[0]
        print(f"Detected failure: topic={f['topic'][:20]}... p{f['partition']}o{f['offset']}")
        if args.partition < 0:
            args.partition = f['partition']
        if not args.topic:
            args.topic = f['topic']
    else:
        print("No failures detected in client log")
        if args.partition < 0:
            return

    # Find the group where the duplicate occurred.
    if not args.group and failures:
        args.group = find_group_for_duplicate(
            client_lines, args.topic, args.partition, failures[0]['offset'])
    if not args.group:
        print("Could not determine group")
        return

    print(f"Topic: {args.topic[:20]}...")
    print(f"Group: {args.group[:20]}...")
    print(f"Partition: {args.partition}")
    print()

    # Get events
    client_events = trace_848_client(client_lines, args.group, args.topic, args.partition)
    server_events = trace_848_server(server_lines, args.partition)

    if not args.server_only:
        print("=== CLIENT TIMELINE ===")
        for ts, gor, mid, event, details in client_events:
            print(f"  {ts:>15} gor={gor:<4} {mid[:16]:>16} {event:<14} {details}")
        print()

    print("=== SERVER TIMELINE (partition events) ===")
    for ts, mid, event, details in server_events:
        if 'FENCE' not in event:
            print(f"  {ts:>15} {mid[:16]:>16} {event:<12} {details[:140]}")

    # Detect dual ALLOWs - two members both given the same partition
    # without an intervening release.
    conflicts = find_dual_allows(server_events)
    if conflicts:
        print()
        print("=== DUAL ALLOW CONFLICTS ===")
        for ts1, mid1, ts2, mid2, detail in conflicts:
            try:
                delta_ms = f' ({(parse_ts(ts2) - parse_ts(ts1)) * 1000:.0f}ms apart)'
            except Exception:
                delta_ms = ''
            print(f"  {detail}")
            print(f"    1st: {ts1} {mid1[:20]}")
            print(f"    2nd: {ts2} {mid2[:20]}{delta_ms}")

        # Deep analysis: for the FIRST conflict, show all server events
        # between the two ALLOWs involving either member, plus epoch
        # rebuild events.
        c = conflicts[0]
        ts1, mid1, ts2, mid2 = c[0], c[1], c[2], c[3]
        t1, t2 = parse_ts(ts1), parse_ts(ts2)
        # Widen window slightly to catch context.
        t_start, t_end = t1 - 0.1, t2 + 0.1
        mid1_short, mid2_short = mid1[:8], mid2[:8]
        print()
        print(f"=== DEEP: server events between 1st and 2nd ALLOW ===")
        print(f"    Window: {ts1} - {ts2} (members {mid1_short}... and {mid2_short}...)")
        for line in server_lines:
            m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
            if not m:
                continue
            try:
                t = parse_ts(m.group(1))
            except Exception:
                continue
            if t < t_start or t > t_end:
                continue
            msg = m.group(3)
            # Show: events mentioning either member, epoch rebuilds,
            # or RECONCILE events for the conflict's partition.
            relevant = (mid1_short in msg or mid2_short in msg
                or 'computeTarget' in msg
                or 'RECONCILE' in msg)
            if relevant:
                print(f"  {m.group(1)} [{m.group(2)}] {msg[:160]}")
    elif server_events:
        print("\nNo dual-ALLOW conflicts found in server log.")

    # Show fences separately (less noise)
    fences = [(ts, mid, ev, d) for ts, mid, ev, d in server_events if ev == 'FENCE']
    if fences:
        print(f"\n=== FENCES ({len(fences)} total) ===")
        for ts, mid, event, details in fences[:10]:
            print(f"  {ts:>15} {mid[:16]:>16} {details[:120]}")
        if len(fences) > 10:
            print(f"  ... and {len(fences) - 10} more")


def main_txn_timeout(args):
    """Analyze transaction timeout failures.

    Traces INVALID_PRODUCER_EPOCH errors back to their root cause by
    finding the timed-out transaction in the server log, showing the
    full transaction lifecycle (start, produce, commit/abort/timeout),
    and any RECONCILE events during the stall window that might
    indicate a consumer group reconciliation problem.
    """
    with open(args.client) as f:
        client_lines = f.readlines()
    with open(args.server) as f:
        server_lines = f.readlines()

    # Find the failing producer ID from client logs.
    pid = None
    epoch = None
    for line in client_lines:
        if 'INVALID_PRODUCER_EPOCH' not in line:
            continue
        # Try various formats.
        m = re.search(r'producer_id[=:]\s*(\d+)', line)
        e = re.search(r'producer_epoch[=:]\s*(\d+)', line)
        if m:
            pid = m.group(1)
            epoch = e.group(1) if e else '?'
            break
    if not pid:
        # No INVALID_PRODUCER_EPOCH. Check for txn timeouts in server log.
        has_timeouts = any('txn timeout abort' in line for line in server_lines)
        if not has_timeouts:
            print("No INVALID_PRODUCER_EPOCH or txn timeouts found.")
            return
        print("No INVALID_PRODUCER_EPOCH in client log, but server has txn timeouts.")
        # Pick the first timed-out txn as the one to analyze.
        for line in server_lines:
            if 'txn timeout abort' not in line:
                continue
            tm = re.search(r'producer_id=(\d+)', line)
            em = re.search(r'epoch=(\d+)', line)
            if tm:
                pid = tm.group(1)
                epoch = em.group(1) if em else '?'
                break
        if not pid:
            print("Could not extract producer ID from timeout line.")
            return

    print(f"Failed producer: pid={pid} epoch={epoch}")
    print()

    # Find the txn ID from server logs.
    txid = None
    for line in server_lines:
        if f'pid {pid}' in line or f'producer_id={pid}' in line:
            tm = re.search(r'txid\s+"([^"]*)"', line) or re.search(r'txid=(\S+)', line)
            if tm and tm.group(1):
                txid = tm.group(1)
                break
    if txid:
        print(f"Transaction ID: {txid[:40]}...")
    else:
        print(f"Could not find txn ID for pid {pid}")
        return

    # Extract full lifecycle for this transaction.
    print()
    print("=== TRANSACTION LIFECYCLE ===")
    for line in server_lines:
        if txid in line:
            m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
            if not m:
                continue
            msg = m.group(3)
            if any(kw in msg for kw in ['started', 'ended', 'timeout', 'EndTxn received', 'ending transaction']):
                print(f"  {m.group(1)} {msg[:140]}")

    # Find the associated consumer group (from AddOffsetsToTxn).
    group = None
    for line in server_lines:
        if txid in line and 'AddOffsets' in line and 'group' in line:
            gm = re.search(r'group\s+"([^"]*)"', line) or re.search(r'group (\S+)', line)
            if gm:
                group = gm.group(1)
                break
    if group:
        print(f"Consumer group: {group[:40]}...")

    # Find the first timeout and the last produce before it.
    first_timeout_ts = None
    last_produce_ts = None
    txn_start_ts = None
    for line in server_lines:
        if txid not in line:
            continue
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)', line)
        if not m:
            continue
        ts = m.group(1)
        if 'timeout abort' in line and first_timeout_ts is None:
            first_timeout_ts = ts
        if 'started transaction' in line:
            txn_start_ts = ts
        if 'produce:' in line:
            last_produce_ts = ts

    if not first_timeout_ts:
        print("\nNo timeout found. All transactions committed or aborted by client.")
        return

    # Show last server activity before the first timeout.
    if first_timeout_ts:
        t_timeout = parse_ts(first_timeout_ts)
        print(f"\n=== LAST SERVER ACTIVITY BEFORE FIRST TIMEOUT ({first_timeout_ts}) ===")
        last_lines = []
        for line in server_lines:
            m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
            if not m:
                continue
            try:
                t = parse_ts(m.group(1))
            except Exception:
                continue
            if t > t_timeout:
                break
            msg = m.group(3)
            if not any(kw in msg for kw in ['produce:', 'fetch: watch push']):
                last_lines.append(f"  {m.group(1)} [{m.group(2)}] {msg[:160]}")
        for line in last_lines[-20:]:
            print(line)

    # Show ALL transaction lifecycle events (not just the failing one).
    all_timeout_txids = set()
    for line in server_lines:
        if 'txn timeout abort' in line:
            tm = re.search(r'txn_id=(\S+)', line)
            if tm:
                all_timeout_txids.add(tm.group(1))
    print(f"\nTotal timed-out transactions: {len(all_timeout_txids)}")
    for tid in sorted(all_timeout_txids):
        events = []
        for line in server_lines:
            if tid not in line:
                continue
            m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
            if not m:
                continue
            msg = m.group(3)
            if any(kw in msg for kw in ['started', 'ended', 'timeout', 'EndTxn received']):
                events.append(f"  {m.group(1)} {msg[:120]}")
        if events:
            print(f"\n  txid={tid[:20]}...")
            for e in events:
                print(e)

    # Count produce activity per epoch to understand flow rate.
    epoch_produce_counts = defaultdict(int)
    epoch_start_times = {}
    cur_epoch = None
    for line in server_lines:
        if txid not in line:
            continue
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)', line)
        if not m:
            continue
        if 'started transaction' in line:
            em = re.search(r'epoch (\d+)', line)
            if em:
                cur_epoch = em.group(1)
                epoch_start_times[cur_epoch] = m.group(1)
        if 'produce:' in line and cur_epoch:
            epoch_produce_counts[cur_epoch] += 1

    # Also count total records per epoch.
    epoch_record_counts = defaultdict(int)
    cur_epoch_for_records = None
    for line in server_lines:
        if txid not in line:
            continue
        if 'started transaction' in line:
            em = re.search(r'epoch (\d+)', line)
            if em:
                cur_epoch_for_records = em.group(1)
        if 'produce:' in line and cur_epoch_for_records:
            rm = re.search(r'records (\d+)', line)
            if rm:
                epoch_record_counts[cur_epoch_for_records] += int(rm.group(1))
        if 'ended' in line and 'transaction' in line:
            cur_epoch_for_records = None

    print(f"\nProduce activity per epoch:")
    for ep in sorted(epoch_produce_counts.keys(), key=int):
        start = epoch_start_times.get(ep, '?')
        recs = epoch_record_counts.get(ep, 0)
        print(f"  epoch {ep}: {epoch_produce_counts[ep]} batches, {recs} records (started {start})")

    # The stall is between the last produce and the timeout.
    stall_start = last_produce_ts or txn_start_ts or first_timeout_ts
    try:
        stall_s = parse_ts(first_timeout_ts) - parse_ts(stall_start)
    except Exception:
        stall_s = 0
    if stall_s > 1:
        print(f"\nStall: {stall_start} to {first_timeout_ts} ({stall_s:.1f}s)")
    else:
        print(f"\nNo stall - transaction was active until timeout ({stall_start})")
        print(f"  Transaction ran for 60s without EndTxn")

    # Always check for read_committed blocked fetches during
    # the full transaction lifetime (from start to timeout).
    txn_start_t = parse_ts(txn_start_ts) if txn_start_ts else parse_ts(first_timeout_ts) - 60
    txn_end_t = parse_ts(first_timeout_ts)
    blocked_partitions = defaultdict(int)
    blocked_topics = defaultdict(int)
    for line in server_lines:
        if 'watch push skipped' not in line:
            continue
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)', line)
        if not m:
            continue
        try:
            t = parse_ts(m.group(1))
        except Exception:
            continue
        if txn_start_t <= t <= txn_end_t:
            pm = re.search(r'\[(\d+)\]', line)
            if pm:
                blocked_partitions[pm.group(1)] += 1
    if blocked_partitions:
        total_skips = sum(blocked_partitions.values())
        print(f"\n=== READ_COMMITTED BLOCKED FETCHES DURING TXN ({total_skips} total) ===")
        for part, count in sorted(blocked_partitions.items(), key=lambda x: -x[1])[:10]:
            print(f"  partition {part}: {count} skipped fetches")
    else:
        print(f"\nNo read_committed blocked fetches during transaction.")

    # Show ALL server events during the stall window that are
    # relevant: classic group events, 848 reconcile events,
    # fetch watch events, and anything mentioning the txn's group.
    t_start = parse_ts(stall_start) - 1
    t_end = parse_ts(first_timeout_ts) + 1
    print(f"\n=== SERVER EVENTS DURING STALL ===")
    count = 0
    for line in server_lines:
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
        if not m:
            continue
        try:
            t = parse_ts(m.group(1))
        except Exception:
            continue
        if t < t_start or t > t_end:
            continue
        msg = m.group(3)
        # Show: group events (classic rebalance, join, sync, leave,
        # state transitions), RECONCILE events, fetch stalls, and
        # anything mentioning the txn's group.
        relevant = ('group ' in msg and any(kw in msg for kw in [
            'handleJoin', 'handleSync', 'handleLeave', 'rebalance',
            'state ->', 'completeRebalance', 'completeLeaderSync',
        ])) or 'RECONCILE' in msg or 'TOPICS CHANGED' in msg
        if group and group[:16] in msg:
            relevant = True
        if relevant:
            if count < 50:
                print(f"  {m.group(1)} {msg[:160]}")
            count += 1
    if count > 50:
        print(f"  ... and {count - 50} more")
    if count == 0:
        print("  (none)")

    # Show commit frequency for OTHER transactions (healthy ones)
    # to contrast with the failing one.
    all_txn_commits = defaultdict(int)
    all_txn_aborts = defaultdict(int)
    for line in server_lines:
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
        if not m:
            continue
        msg = m.group(3)
        if 'transaction ended (commit)' in msg:
            tm = re.search(r'txid "([^"]*)"', msg)
            if tm:
                all_txn_commits[tm.group(1)[:16]] += 1
        elif 'transaction ended (abort)' in msg:
            tm = re.search(r'txid "([^"]*)"', msg)
            if tm:
                all_txn_aborts[tm.group(1)[:16]] += 1
    print(f"\n=== TRANSACTION COMMIT/ABORT SUMMARY ===")
    print(f"  Total distinct txn IDs with commits: {len(all_txn_commits)}")
    print(f"  Total distinct txn IDs with aborts: {len(all_txn_aborts)}")
    if all_txn_commits:
        top = sorted(all_txn_commits.items(), key=lambda x: -x[1])[:5]
        print(f"  Top committers: {', '.join(f'{tid}...={c}' for tid, c in top)}")
    failing_short = txid[:16]
    print(f"  Failing txn {failing_short}...: commits={all_txn_commits.get(failing_short, 0)} aborts={all_txn_aborts.get(failing_short, 0)}")

    # Find ALL transaction timeouts and show which topics they
    # produced to. Another producer's uncommitted txn could block
    # the read_committed consumer downstream.
    print(f"\n=== ALL TRANSACTION TIMEOUTS ===")
    timeouts = []
    for line in server_lines:
        if 'txn timeout abort' not in line:
            continue
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
        if m:
            timeouts.append((m.group(1), m.group(3)))
    if timeouts:
        for ts, msg in timeouts:
            print(f"  {ts} {msg[:140]}")
            # Find what topics this timed-out txn produced to.
            tid_m = re.search(r'txn_id=(\S+)', msg)
            if tid_m:
                topics = set()
                for sl in server_lines:
                    if tid_m.group(1) in sl and 'produce:' in sl:
                        tm = re.search(r'" (\S+)\[(\d+)\]', sl)
                        if tm:
                            topics.add(f"{tm.group(1)[:16]}...[{tm.group(2)}]")
                if topics:
                    print(f"         produced to: {', '.join(sorted(topics)[:10])}")
    else:
        print("  (none)")

    # Show fetch watch skips during the stall to identify which
    # partitions are blocked by uncommitted transactions.
    t_stall_start = parse_ts(txn_start_ts) if txn_start_ts else parse_ts(first_timeout_ts) - 60
    t_stall_end = parse_ts(first_timeout_ts)
    blocked_partitions = defaultdict(int)
    for line in server_lines:
        if 'watch push skipped' not in line:
            continue
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)', line)
        if not m:
            continue
        try:
            t = parse_ts(m.group(1))
        except Exception:
            continue
        if t_stall_start <= t <= t_stall_end:
            pm = re.search(r'\[(\d+)\]', line)
            if pm:
                blocked_partitions[pm.group(1)] += 1
    if blocked_partitions:
        print(f"\n=== READ_COMMITTED BLOCKED PARTITIONS DURING STALL ===")
        for part, count in sorted(blocked_partitions.items(), key=lambda x: -x[1])[:20]:
            print(f"  partition {part}: {count} skipped fetches")


# ---------------------------------------------------------------------------
# Produce offset order failure analysis
# ---------------------------------------------------------------------------

def main_produce_order(args):
    """Diagnose 'partition produced offsets out of order' test failures.

    Finds the failing topic, traces its producer lifecycle (InitProducerID,
    epoch changes, produce errors), and checks the server log for relevant
    events (epoch bumps, INVALID_PRODUCER_EPOCH, PRODUCER_FENCED).
    """
    with open(args.client) as f:
        client_lines = f.readlines()
    with open(args.server) as f:
        server_lines = f.readlines()

    # Find the failing topic from "NOT DELETING TOPIC"
    topic = None
    for line in client_lines:
        m = re.search(r'NOT DELETING TOPIC (\S+)', line)
        if m:
            topic = m.group(1)
            break
    if not topic:
        # Try from the offset error lines
        print("No 'NOT DELETING TOPIC' found, checking test errors...")
        for line in client_lines:
            if 'offsets out of order' in line:
                print(f"  {line.rstrip()[:150]}")

    print(f"=== FAILING TOPIC ===")
    print(f"  {topic}")
    topic_short = topic[:16] if topic else '?'

    # Find the offset order errors
    print(f"\n=== OFFSET ORDER ERRORS ===")
    errors = []
    for line in client_lines:
        if 'offsets out of order' in line:
            m = re.search(r'got (\d+) != exp (\d+)', line)
            if m:
                errors.append((int(m.group(1)), int(m.group(2))))
                if len(errors) <= 10:
                    print(f"  {line.rstrip()[:150]}")
    if len(errors) > 10:
        print(f"  ... and {len(errors) - 10} more")

    # Categorize: offset jumped up, or reset to 0?
    jumps_up = [(got, exp) for got, exp in errors if got > exp]
    resets = [(got, exp) for got, exp in errors if got < exp]
    print(f"\n  Offset jumped UP (got > exp): {len(jumps_up)}")
    print(f"  Offset RESET (got < exp): {len(resets)}")
    if resets:
        reset_to_zero = sum(1 for got, _ in resets if got == 0)
        print(f"  Reset to 0: {reset_to_zero}")

    # Find the producer for this topic in client log.
    # The TestGroupETL producer has no logger number prefix and produces
    # to the failing topic. Look for "initializing producer id" near
    # the topic's first mention.
    print(f"\n=== PRODUCER LIFECYCLE ===")
    if topic:
        # Find all producer-related events mentioning this topic or
        # from the same logger number
        for i, line in enumerate(client_lines):
            if topic_short in line and ('produce' in line.lower() or 'metadata' in line.lower()):
                if i < 5:
                    print(f"  L{i+1}: {line.rstrip()[:150]}")

    # Find ALL producer ID initializations and epoch changes
    print(f"\n=== ALL PRODUCER ID EVENTS ===")
    init_events = []
    for i, line in enumerate(client_lines):
        if 'producer id initialization' in line or 'initializing producer id' in line:
            init_events.append((i+1, line.rstrip()))
        elif 'epoch' in line.lower() and ('bump' in line.lower() or 'fail' in line.lower() or 'reset' in line.lower()):
            init_events.append((i+1, line.rstrip()))
        elif 'batch errored' in line or 'data loss' in line.lower():
            init_events.append((i+1, line.rstrip()))
        elif 'PRODUCER_FENCED' in line or 'INVALID_PRODUCER_EPOCH' in line:
            init_events.append((i+1, line.rstrip()))

    # Show events from loggers WITHOUT a number prefix (the TestGroupETL producer)
    print(f"  Producer events without logger number (TestGroupETL producer):")
    for lineno, line in init_events:
        # Lines without [time num] prefix
        if not re.match(r'\[\d{2}:\d{2}:\d{2}', line):
            print(f"    L{lineno}: {line[:150]}")

    # Show events from all loggers
    print(f"\n  All producer ID init events ({len(init_events)} total):")
    for lineno, line in init_events[:20]:
        print(f"    L{lineno}: {line[:150]}")
    if len(init_events) > 20:
        print(f"    ... and {len(init_events) - 20} more")

    # Server-side: check for INVALID_PRODUCER_EPOCH or PRODUCER_FENCED
    print(f"\n=== SERVER PRODUCE ERRORS ===")
    server_errors = []
    for line in server_lines:
        if 'INVALID_PRODUCER_EPOCH' in line or 'PRODUCER_FENCED' in line:
            server_errors.append(line.rstrip())
    print(f"  Total: {len(server_errors)}")
    for line in server_errors[:10]:
        print(f"  {line[:160]}")

    # Server-side: check for epoch bumps from timeout
    print(f"\n=== SERVER TXN TIMEOUT ABORTS ===")
    timeouts = [l.rstrip() for l in server_lines if 'txn timeout abort' in l]
    print(f"  Total: {len(timeouts)}")
    for line in timeouts[:5]:
        print(f"  {line[:160]}")

    # Server-side: check topic activity
    if topic_short and topic_short != '?':
        print(f"\n=== SERVER ACTIVITY FOR TOPIC {topic_short}... ===")
        topic_lines = []
        for line in server_lines:
            if topic_short in line:
                topic_lines.append(line.rstrip())
        print(f"  Total lines mentioning topic: {len(topic_lines)}")
        for line in topic_lines[:5]:
            print(f"  {line[:160]}")
        if len(topic_lines) > 10:
            print(f"  ...")
            for line in topic_lines[-5:]:
                print(f"  {line[:160]}")

    # Client-side: find producer PID for the failing topic.
    # Look for "producing to a new topic" or metadata events for this topic.
    # Then trace that PID's lifecycle.
    if topic:
        print(f"\n=== PRODUCER PID TRACING ===")
        # Find PIDs that produced to this topic by searching for
        # produce callbacks that mention partition offsets.
        # The TestGroupETL producer logs don't have [time num] prefix
        # or have a specific logger number.
        # Let's find InitProducerID events without a logger number
        # (TestGroupETL's producer) and trace their PID.
        plain_pids = set()
        for line in client_lines:
            if 'producer id initialization success' in line:
                # Check if it has a logger number prefix
                if not re.match(r'\[\d{2}:\d{2}:\d{2}', line):
                    m = re.search(r'id: (\d+), epoch: (\d+)', line)
                    if m:
                        plain_pids.add((int(m.group(1)), int(m.group(2))))

        print(f"  PIDs from loggers without number (likely TestGroupETL): {len(plain_pids)}")
        for pid, epoch in sorted(plain_pids):
            print(f"    PID {pid} epoch {epoch}")

        # Now trace ALL events for these PIDs in the client log
        for pid, _ in sorted(plain_pids):
            pid_str = str(pid)
            print(f"\n  Events for PID {pid}:")
            count = 0
            for i, line in enumerate(client_lines):
                if pid_str in line:
                    count += 1
                    if count <= 15:
                        print(f"    L{i+1}: {line.rstrip()[:160]}")
            if count > 15:
                print(f"    ... and {count - 15} more")

    # Client-side: find any produce errors (not "offsets out of order")
    print(f"\n=== CLIENT PRODUCE ERRORS (non-offset) ===")
    prod_errors = []
    for line in client_lines:
        if 'unexpected produce err' in line or 'produce err' in line.lower():
            prod_errors.append(line.rstrip())
    print(f"  Total: {len(prod_errors)}")
    for line in prod_errors[:10]:
        print(f"  {line[:160]}")

    # Client-side: find connection errors / broker deaths during test
    print(f"\n=== CLIENT CONNECTION ERRORS ===")
    conn_errors = []
    for line in client_lines:
        if 'connection has died' in line or 'use of closed' in line or 'i/o timeout' in line:
            conn_errors.append(line.rstrip())
    print(f"  Total: {len(conn_errors)}")
    for line in conn_errors[:10]:
        print(f"  {line[:160]}")


# ---------------------------------------------------------------------------
# Server stall analysis (transaction timeouts, EndTxn gaps, deadlocks)
# ---------------------------------------------------------------------------

def main_server_stall(args):
    """Analyze server-side stalls causing transaction timeouts.

    Finds when the server stopped processing EndTxn requests, what was
    happening during the gap, and whether the pids manage loop or
    Cluster.run() was blocked. Shows:

    - Transaction timeout timeline
    - Last successful EndTxn before stall
    - Server activity during the stall window
    - Blocked warnings (>5s timers)
    - Group state machine events during stall
    """
    with open(args.server) as f:
        server_lines = f.readlines()

    client_lines = []
    if args.client:
        try:
            with open(args.client) as f:
                client_lines = f.readlines()
        except FileNotFoundError:
            pass

    # Find all transaction timeouts
    timeouts = []
    for line in server_lines:
        if 'txn timeout abort' not in line:
            continue
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
        if m:
            timeouts.append((m.group(1), m.group(3)))

    if not timeouts:
        print("No transaction timeouts found in server log.")
        return

    print(f"=== TRANSACTION TIMEOUTS ({len(timeouts)} total) ===")
    first_timeout_ts = timeouts[0][0]
    last_timeout_ts = timeouts[-1][0]
    print(f"  First: {first_timeout_ts}")
    print(f"  Last:  {last_timeout_ts}")
    for ts, msg in timeouts[:5]:
        print(f"  {ts} {msg[:140]}")
    if len(timeouts) > 5:
        print(f"  ... and {len(timeouts) - 5} more")

    # Find all successful EndTxn (transaction ended commit/abort by CLIENT request)
    # vs timeout aborts. Look for "transaction ended" lines.
    ended_by_client = []  # EndTxn from client
    ended_by_timeout = []  # timeout abort
    for line in server_lines:
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
        if not m:
            continue
        ts, msg = m.group(1), m.group(3)
        if 'transaction ended' in msg and 'timeout' not in msg:
            ended_by_client.append((ts, msg))
        elif 'txn timeout abort' in msg:
            ended_by_timeout.append((ts, msg))

    print(f"\n=== ENDTXN TIMELINE ===")
    print(f"  Client-initiated EndTxn: {len(ended_by_client)}")
    print(f"  Timeout aborts: {len(ended_by_timeout)}")

    if ended_by_client:
        last_client_end = ended_by_client[-1]
        print(f"  Last client EndTxn: {last_client_end[0]} {last_client_end[1][:100]}")

        # Find the first timeout after the last client EndTxn
        stall_start = last_client_end[0]
        stall_end = first_timeout_ts
        try:
            stall_secs = parse_ts(stall_end) - parse_ts(stall_start)
        except Exception:
            stall_secs = 0
        print(f"\n  STALL GAP: {stall_start} to {stall_end} ({stall_secs:.1f}s)")
        print(f"  No client EndTxn completed during this window.")
    else:
        stall_start = None
        stall_end = first_timeout_ts
        print(f"  No client EndTxn found at all!")

    # Show client-initiated EndTxn near the stall boundary
    if ended_by_client:
        print(f"\n=== LAST 10 CLIENT ENDTXN ===")
        for ts, msg in ended_by_client[-10:]:
            print(f"  {ts} {msg[:140]}")

    # Compute the window where transactions were active but never completed.
    # Transactions that timed out at T started at T-120s.
    t_first_timeout = parse_ts(first_timeout_ts)
    txn_start_window = t_first_timeout - 120  # when these txns started
    if stall_start:
        t_stall_start = parse_ts(stall_start)
    else:
        t_stall_start = txn_start_window

    # Show server activity during the stall window
    print(f"\n=== SERVER ACTIVITY DURING STALL ({stall_start or '?'} to {first_timeout_ts}) ===")
    categories = defaultdict(int)
    warn_lines = []
    interesting = []
    for line in server_lines:
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
        if not m:
            continue
        try:
            t = parse_ts(m.group(1))
        except Exception:
            continue
        if t < t_stall_start or t > t_first_timeout:
            continue
        ts, level, msg = m.group(1), m.group(2), m.group(3)

        # Categorize
        if 'produce:' in msg:
            categories['produce'] += 1
        elif 'fetch:' in msg:
            categories['fetch'] += 1
        elif 'txn:' in msg or 'EndTxn' in msg:
            categories['txn'] += 1
            interesting.append(f"  {ts} [{level}] {msg[:160]}")
        elif 'group ' in msg or 'RECONCILE' in msg or 'handleJoin' in msg:
            categories['group'] += 1
        elif 'OffsetCommit' in msg or 'OffsetFetch' in msg:
            categories['offset'] += 1
        elif 'client ' in msg and 'disconnected' in msg:
            categories['disconnect'] += 1
        else:
            categories['other'] += 1

        if level == 'WRN':
            warn_lines.append(f"  {ts} [{level}] {msg[:160]}")

    print("  Activity by category:")
    for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"    {count:>6}  {cat}")

    if warn_lines:
        print(f"\n  WARNINGS during stall ({len(warn_lines)}):")
        for line in warn_lines[:20]:
            print(line)

    if interesting:
        print(f"\n  Transaction events during stall ({len(interesting)}):")
        for line in interesting[:30]:
            print(line)
        if len(interesting) > 30:
            print(f"  ... and {len(interesting) - 30} more")

    # Check for endTx c.admin() start/end pairs to find if endTx is blocking
    print(f"\n=== ENDTX C.ADMIN() TIMING ===")
    admin_starts = []
    admin_ends = []
    for line in server_lines:
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
        if not m:
            continue
        ts, msg = m.group(1), m.group(3)
        if 'endTx c.admin() start' in msg:
            admin_starts.append((ts, msg))
        elif 'endTx c.admin() end' in msg or ('transaction ended' in msg and 'endTx' not in msg):
            admin_ends.append((ts, msg))

    if admin_starts:
        print(f"  endTx c.admin() calls: {len(admin_starts)}")
        # Find any that took >1s
        # Match starts with ends by looking at sequential pairs
        for i, (ts, msg) in enumerate(admin_starts[-10:]):
            print(f"  {ts} {msg[:120]}")
    else:
        print(f"  No endTx c.admin() start logs found")

    # Show InitProducerID activity during and after stall
    print(f"\n=== INITPRODUCERID DURING/AFTER STALL ===")
    init_count = 0
    init_lines = []
    for line in server_lines:
        m = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)\s+\[(\w+)\]\s+(.*)', line)
        if not m:
            continue
        try:
            t = parse_ts(m.group(1))
        except Exception:
            continue
        if t < t_stall_start - 10:
            continue
        msg = m.group(3)
        if 'InitProducerID' in msg:
            init_count += 1
            if init_count <= 20:
                init_lines.append(f"  {m.group(1)} {msg[:140]}")
    print(f"  Total InitProducerID events: {init_count}")
    for line in init_lines:
        print(line)
    if init_count > 20:
        print(f"  ... and {init_count - 20} more")

    # Client-side: find fatal InitProducerID retries
    if client_lines:
        print(f"\n=== CLIENT FATAL INITPRODUCERID RETRIES ===")
        retries = 0
        retry_loggers = defaultdict(int)
        for line in client_lines:
            if 'fatal InitProducerID' in line:
                retries += 1
                lm = re.search(r'\[(\d{2}:\d{2}:\d{2}\.\d+)\s+(\d+)\]', line)
                if lm:
                    retry_loggers[lm.group(2)] += 1
        print(f"  Total: {retries}")
        print(f"  By logger:")
        for logger, count in sorted(retry_loggers.items(), key=lambda x: -x[1]):
            print(f"    logger {logger}: {count}")


# ---------------------------------------------------------------------------
# ETL stuck analysis (chain consumption stall / timeout)
# ---------------------------------------------------------------------------


def _find_stuck_test(client_lines):
    """Find the test that timed out from the panic line."""
    for i, line in enumerate(client_lines):
        if 'panic: test timed out' in line:
            # Next lines have "running tests:" and "TestXxx/yyy (NmNNs)"
            for j in range(i + 1, min(i + 5, len(client_lines))):
                m = re.search(r'(Test\S+)\s+\((\S+)\)', client_lines[j])
                if m:
                    return m.group(1), m.group(2), i + 1
    return None, None, None


def _find_848_groups(client_lines):
    """Find all 848 (next-gen) consumer groups and their topics.

    Returns a list of dicts with keys:
      group: full group ID
      group_short: first 16 chars
      consume_topic: topic being consumed
      produce_topic: topic being produced to (if found)
      members: list of (client_id, member_id)
      first_line: first line number where group appears
      first_ts: first timestamp
    """
    groups = {}  # group_short => info dict

    # Pass 1: find all next-gen groups and their first assignment (consume topic)
    for i, line in enumerate(client_lines):
        if 'next-gen group lifecycle' not in line:
            continue
        gm = re.search(r'group: (\S+)', line)
        if not gm:
            continue
        group = gm.group(1).rstrip(',')
        gs = group[:16]
        if gs in groups:
            continue
        ts_m = re.search(r'(\d{2}:\d{2}:\d{2}\.\d+)', line)
        ts = ts_m.group(1) if ts_m else '?'
        cid_m = re.search(r'\[[\d:.]+\s+(\d+)\]', line)
        cid = cid_m.group(1) if cid_m else '?'
        groups[gs] = {
            'group': group,
            'group_short': gs,
            'consume_topic': None,
            'produce_topic': None,
            'client_ids': set(),
            'members': [],
            'first_line': i + 1,
            'first_ts': ts,
        }
        if cid != '?':
            groups[gs]['client_ids'].add(cid)

    # Pass 2: find consume topics from assignment lines
    for line in client_lines:
        if 'now_assigned: map[' not in line:
            continue
        gm = re.search(r'group: (\S+)', line)
        if not gm:
            continue
        gs = gm.group(1).rstrip(',')[:16]
        if gs not in groups:
            continue
        if groups[gs]['consume_topic']:
            continue
        # Extract topic from map[TOPIC:[partitions]]
        tm = re.search(r'now_assigned: map\[(\w+):\[', line)
        if tm:
            groups[gs]['consume_topic'] = tm.group(1)
        # Also collect client IDs
        cid_m = re.search(r'\[[\d:.]+\s+(\d+)\]', line)
        if cid_m:
            groups[gs]['client_ids'].add(cid_m.group(1))
        # Collect member IDs
        mm = re.search(r'member_id: (\S+)', line)
        if mm:
            groups[gs]['members'].append(mm.group(1).rstrip(','))

    # Pass 3: find all client IDs per group (from heartbeat/assignment lines)
    for line in client_lines:
        gm = re.search(r'group: (\S+)', line)
        if not gm:
            continue
        gs = gm.group(1).rstrip(',')[:16]
        if gs not in groups:
            continue
        cid_m = re.search(r'\[[\d:.]+\s+(\d+)\]', line)
        if cid_m:
            groups[gs]['client_ids'].add(cid_m.group(1))

    # Pass 4: find produce topics by checking "producing to a new topic" for
    # each group's client IDs
    client_to_group = {}
    for gs, info in groups.items():
        for cid in info['client_ids']:
            if cid not in client_to_group:
                client_to_group[cid] = gs

    for line in client_lines:
        if 'producing to a new topic for the first time' not in line:
            continue
        cid_m = re.search(r'\[[\d:.]+\s+(\d+)\]', line)
        if not cid_m:
            continue
        cid = cid_m.group(1)
        gs = client_to_group.get(cid)
        if not gs or groups[gs]['produce_topic']:
            continue
        tm = re.search(r'topic: (\S+)', line)
        if tm:
            groups[gs]['produce_topic'] = tm.group(1)

    return list(groups.values())


def _build_etl_chain(groups):
    """Given a list of 848 group info dicts, build ETL chain(s).

    An ETL chain is: level1 consumes topicA, produces topicB;
    level2 consumes topicB, produces topicC; etc.

    Returns list of chains, where each chain is a list of group info dicts
    in order (level 1 first).
    """
    # Build a map: topic => group that consumes it
    consume_map = {}
    for g in groups:
        if g['consume_topic']:
            consume_map[g['consume_topic']] = g

    # Build chains: start from groups whose consume_topic is NOT
    # any other group's produce_topic (i.e., level 1).
    produce_topics = {g['produce_topic'] for g in groups if g['produce_topic']}
    chains = []
    for g in groups:
        ct = g['consume_topic']
        if not ct:
            continue
        if ct not in produce_topics:
            # This is a level-1 group (consumes from a topic not produced by any group)
            chain = [g]
            current = g
            while current['produce_topic'] and current['produce_topic'] in consume_map:
                nxt = consume_map[current['produce_topic']]
                chain.append(nxt)
                current = nxt
            if len(chain) > 1:
                chains.append(chain)
    return chains


def _get_final_offsets(server_lines, group_short):
    """Get the final committed offset per partition for a group.

    Scans all OffsetCommit lines and returns the last offset seen per partition.
    """
    offsets = {}  # partition => offset
    pattern = re.compile(
        r'OffsetCommit: group=' + re.escape(group_short)
        + r'\S*\s+member=\S+\s+topic=\S+\s+p=(\d+)\s+offset=(\d+)'
    )
    for line in server_lines:
        m = pattern.search(line)
        if m:
            p = int(m.group(1))
            o = int(m.group(2))
            offsets[p] = max(offsets.get(p, 0), o)
    return offsets


def _get_hwm(server_lines, topic_short):
    """Get the high water mark per partition for a topic.

    Looks at produce lines to find the highest offset + records.
    """
    hwm = {}  # partition => hwm
    # produce lines: produce: ... TOPIC[PART] offset N records M
    pattern = re.compile(
        re.escape(topic_short) + r'\S*\[(\d+)\]\s+offset\s+(\d+)\s+records\s+(\d+)'
    )
    for line in server_lines:
        m = pattern.search(line)
        if m:
            p = int(m.group(1))
            end_offset = int(m.group(2)) + int(m.group(3))
            hwm[p] = max(hwm.get(p, 0), end_offset)
    return hwm


def _get_offset_commit_timeline(server_lines, group_short):
    """Get timeline of offset commits: list of (timestamp, partition, offset)."""
    results = []
    pattern = re.compile(
        r'(\d{2}:\d{2}:\d{2}\.\d+).*OffsetCommit: group='
        + re.escape(group_short) + r'\S*\s+member=\S+\s+topic=\S+\s+p=(\d+)\s+offset=(\d+)'
    )
    for line in server_lines:
        m = pattern.search(line)
        if m:
            results.append((m.group(1), int(m.group(2)), int(m.group(3))))
    return results


def _find_etl_errors(client_lines, groups):
    """Find ETL-related errors in client log for the given groups."""
    group_shorts = {g['group_short'] for g in groups}
    errors = []
    error_patterns = [
        'saw double offset',
        'unexpected etl produce err',
        'body not what was expected',
        'unable to flush',
        'unable to commit',
        'PRODUCER_FENCED',
        'INVALID_PRODUCER_EPOCH',
    ]
    for i, line in enumerate(client_lines):
        for pat in error_patterns:
            if pat in line:
                # Check if it's related to our groups
                relevant = False
                for gs in group_shorts:
                    if gs in line:
                        relevant = True
                        break
                if not relevant:
                    # Check if any client ID from our groups is in the line
                    for g in groups:
                        for cid in g['client_ids']:
                            if f' {cid}]' in line:
                                relevant = True
                                break
                        if relevant:
                            break
                if not relevant:
                    # Still report it, might be from the producer
                    relevant = True
                errors.append((i + 1, pat, line.rstrip()[:200], relevant))
    return errors


def _analyze_consumption_progress(server_lines, client_lines, chain):
    """Analyze consumption progress for each level in the chain.

    Returns per-level analysis dicts with:
      - final_offsets: {partition: offset}
      - total_consumed: sum of final offsets
      - hwm: {partition: hwm} for the produce topic
      - total_produced: sum of hwm values for produce topic
      - commit_timeline: [(ts, part, offset)]
      - last_commit_ts: timestamp of last commit
      - first_commit_ts: timestamp of first commit
    """
    results = []
    for i, g in enumerate(chain):
        gs = g['group_short']
        final_offsets = _get_final_offsets(server_lines, gs)
        total_consumed = sum(final_offsets.values())

        commit_tl = _get_offset_commit_timeline(server_lines, gs)
        last_commit_ts = commit_tl[-1][0] if commit_tl else '?'
        first_commit_ts = commit_tl[0][0] if commit_tl else '?'

        # HWM of the produce topic
        pt = g.get('produce_topic', '')
        hwm = _get_hwm(server_lines, pt[:16]) if pt else {}
        total_produced = sum(hwm.values())

        # HWM of the consume topic
        ct = g.get('consume_topic', '')
        ct_hwm = _get_hwm(server_lines, ct[:16]) if ct else {}
        ct_total = sum(ct_hwm.values())

        results.append({
            'level': i + 1,
            'group_short': gs,
            'consume_topic': ct[:16] if ct else '?',
            'produce_topic': pt[:16] if pt else '?',
            'final_offsets': final_offsets,
            'total_consumed': total_consumed,
            'consume_hwm': ct_hwm,
            'consume_total': ct_total,
            'produce_hwm': hwm,
            'total_produced': total_produced,
            'commit_count': len(commit_tl),
            'first_commit_ts': first_commit_ts,
            'last_commit_ts': last_commit_ts,
        })
    return results


def _find_stuck_goroutines(client_lines, groups):
    """Find goroutines stuck in PollRecords for the given groups."""
    # Find panic section
    dump_start = -1
    for i, line in enumerate(client_lines):
        if 'panic: test timed out' in line:
            dump_start = i
            break
    if dump_start < 0:
        return []

    goroutines = parse_goroutine_dump(client_lines[dump_start:])
    stuck = []
    group_shorts = {g['group_short'] for g in groups}
    for gor in goroutines:
        frames_str = ' '.join(f[0] + ' ' + f[1] for f in gor['frames'])
        if 'PollRecords' in frames_str or 'PollFetches' in frames_str:
            # Check if this goroutine is for one of our groups
            for gs in group_shorts:
                if gs in frames_str:
                    stuck.append(gor)
                    break
            else:
                # Check for test function names
                if 'etl' in frames_str.lower() or 'transact' in frames_str.lower():
                    stuck.append(gor)
    return stuck


def _find_rebalance_activity(client_lines, group_short, start_ts=None, end_ts=None):
    """Count rebalance events (assignment changes) for a group.

    Returns (total_rebalances, last_rebalance_ts).
    """
    count = 0
    last_ts = None
    for line in client_lines:
        if group_short not in line:
            continue
        if 'updated assignment' not in line and 'now_assigned' not in line:
            continue
        ts_m = re.search(r'(\d{2}:\d{2}:\d{2}\.\d+)', line)
        if ts_m:
            ts = ts_m.group(1)
            if start_ts and parse_ts(ts) < parse_ts(start_ts):
                continue
            if end_ts and parse_ts(ts) > parse_ts(end_ts):
                continue
            count += 1
            last_ts = ts
    return count, last_ts


def _find_heartbeat_activity(client_lines, group_short):
    """Find last heartbeat-related activity for a group.

    Returns (count, first_ts, last_ts).
    """
    count = 0
    first_ts = None
    last_ts = None
    for line in client_lines:
        if group_short not in line:
            continue
        if 'heartbeat' not in line:
            continue
        ts_m = re.search(r'(\d{2}:\d{2}:\d{2}\.\d+)', line)
        if ts_m:
            count += 1
            ts = ts_m.group(1)
            if first_ts is None:
                first_ts = ts
            last_ts = ts
    return count, first_ts, last_ts


def _find_consumption_gaps(server_lines, group_short, window_sec=10):
    """Find gaps in offset commits longer than window_sec.

    Returns list of (gap_start_ts, gap_end_ts, gap_seconds).
    """
    commits = _get_offset_commit_timeline(server_lines, group_short)
    if len(commits) < 2:
        return []
    gaps = []
    for i in range(1, len(commits)):
        t1 = parse_ts(commits[i-1][0])
        t2 = parse_ts(commits[i][0])
        if t2 - t1 > window_sec:
            gaps.append((commits[i-1][0], commits[i][0], t2 - t1))
    return gaps


def main_etl_stuck(args):
    """Diagnose stuck ETL chain tests (TestGroupETL or TestTxnEtl timeouts).

    Analyzes client and server logs to determine why an ETL chain test
    timed out. The ETL chain has 3 levels, each consuming from one topic
    and producing to the next. This tool:

    1. Identifies the stuck test from the panic/timeout line
    2. Discovers all 848 (next-gen) consumer groups and their topics
    3. Builds ETL chains by matching produce/consume topics
    4. Computes final committed offsets per level
    5. Compares HWM of intermediate topics to consumed offsets
    6. Identifies where in the chain consumption stopped
    7. Checks for errors (PRODUCER_FENCED, duplicate offsets, etc.)
    8. Shows rebalance and heartbeat activity
    9. Shows goroutine dump for stuck consumers
    """
    with open(args.client) as f:
        client_lines = f.readlines()
    with open(args.server) as f:
        server_lines = f.readlines()

    # --- Find stuck test ---
    test_name, test_duration, panic_line = _find_stuck_test(client_lines)
    print("=== STUCK TEST ===")
    if test_name:
        print(f"  Test: {test_name}")
        print(f"  Duration: {test_duration}")
        print(f"  Panic at line: {panic_line}")
    else:
        print("  No test timeout found in client log.")
        print("  Analyzing all 848 groups regardless.")
    print()

    # --- Find all 848 groups ---
    all_groups = _find_848_groups(client_lines)
    print(f"=== ALL 848 GROUPS ({len(all_groups)}) ===")
    for g in sorted(all_groups, key=lambda x: x['first_ts']):
        ct = g['consume_topic'][:16] if g['consume_topic'] else '?'
        pt = g['produce_topic'][:16] if g['produce_topic'] else '?'
        print(f"  {g['group_short']}  consumes={ct}  produces={pt}  "
              f"clients={sorted(g['client_ids'])}  started={g['first_ts']}")
    print()

    # --- Build ETL chains ---
    chains = _build_etl_chain(all_groups)
    print(f"=== ETL CHAINS ({len(chains)}) ===")
    if not chains:
        print("  No ETL chains found. Groups may not form a produce/consume chain.")
        print("  This can happen if logging is insufficient to detect produce topics.")
        print()
        # Fall back: just analyze all groups individually
        if all_groups:
            print("  Analyzing all groups individually instead.")
            # Create a "chain" from each group
            chains = [[g] for g in all_groups]

    # Build a quick lookup of total committed offsets per group from server log
    group_commit_totals = {}
    group_last_commit_ts = {}
    for sline in server_lines:
        m = re.match(r'(\d+:\d+:\d+\.\d+) .* OffsetCommit: group=(\S+) .* offset=(\d+)', sline)
        if m:
            ts, gshort, off = m.group(1), m.group(2), int(m.group(3))
            group_commit_totals.setdefault(gshort, {})[
                sline.split('p=')[1].split()[0] if 'p=' in sline else '?'
            ] = off
            group_last_commit_ts[gshort] = ts

    for ci, chain in enumerate(chains):
        print(f"\n  Chain {ci + 1} ({len(chain)} levels):")
        for i, g in enumerate(chain):
            ct = g['consume_topic'][:16] if g['consume_topic'] else '?'
            pt = g['produce_topic'][:16] if g['produce_topic'] else '?'
            total = sum(group_commit_totals.get(g['group_short'], {}).values())
            last_ts = group_last_commit_ts.get(g['group_short'], '?')
            print(f"    Level {i+1}: group={g['group_short']}  "
                  f"consumes={ct}  produces={pt}  "
                  f"committed={total}  last_commit={last_ts}")
    print()

    # --- Filter to the stuck test's chain ---
    target_chain = None

    # If -g was provided, find the chain containing that group prefix.
    if args.group:
        for chain in chains:
            for g in chain:
                if g['group_short'].startswith(args.group) or \
                   g['group'].startswith(args.group):
                    target_chain = chain
                    break
            if target_chain:
                break
        if not target_chain:
            print(f"  WARNING: -g {args.group} did not match any chain.")

    # Otherwise, find the chain whose last OffsetCommit is closest to the
    # panic time (the stuck chain is still actively committing at timeout).
    # Also find chains that did NOT reach 500K (the stuck chain).
    if not target_chain and chains:
        # First: prefer a chain where committed offsets < 500K
        for chain in chains:
            for g in chain:
                total = sum(group_commit_totals.get(g['group_short'], {}).values())
                if 0 < total < 500000:
                    target_chain = chain
                    break
            if target_chain:
                break

    if not target_chain and chains:
        # Fall back: chain whose last commit is closest to the panic
        best_chain = None
        best_ts = 0
        for chain in chains:
            for g in chain:
                ts_str = group_last_commit_ts.get(g['group_short'], '')
                if ts_str:
                    t = parse_ts(ts_str)
                    if t > best_ts:
                        best_ts = t
                        best_chain = chain
        if best_chain:
            target_chain = best_chain

    # Fallback: latest-starting chain.
    if not target_chain and chains:
        latest_start = None
        for chain in chains:
            chain_start = min(g['first_ts'] for g in chain)
            if latest_start is None or chain_start > latest_start:
                latest_start = chain_start
                target_chain = chain
    if not target_chain and chains:
        target_chain = chains[0]

    if not target_chain:
        print("No ETL chain found to analyze.")
        return

    print(f"=== ANALYZING CHAIN (started ~{target_chain[0]['first_ts']}) ===")
    for i, g in enumerate(target_chain):
        ct = g['consume_topic'][:16] if g['consume_topic'] else '?'
        pt = g['produce_topic'][:16] if g['produce_topic'] else '?'
        print(f"  Level {i+1}: group={g['group_short']}  "
              f"consumes={ct}  produces={pt}")
    print()

    # --- Consumption progress per level ---
    progress = _analyze_consumption_progress(server_lines, client_lines, target_chain)
    print("=== CONSUMPTION PROGRESS ===")
    for p in progress:
        print(f"\n  Level {p['level']}: group={p['group_short']}")
        print(f"    Consume topic: {p['consume_topic']}...")
        print(f"    Produce topic: {p['produce_topic']}...")
        print(f"    Committed offsets (final):")
        total = 0
        for part in sorted(p['final_offsets'].keys()):
            off = p['final_offsets'][part]
            total += off
            hwm = p['consume_hwm'].get(part, '?')
            behind = f" (behind by {hwm - off})" if isinstance(hwm, int) and off < hwm else ""
            print(f"      p{part}: offset={off}  hwm={hwm}{behind}")
        print(f"    Total consumed: {total}")
        print(f"    Consume topic HWM total: {p['consume_total']}")
        if p['consume_total'] > 0:
            pct = total / p['consume_total'] * 100
            print(f"    Progress: {pct:.1f}%")
        print(f"    Produce topic HWM total: {p['total_produced']}")
        print(f"    Commits: {p['commit_count']}  "
              f"first={p['first_commit_ts']}  last={p['last_commit_ts']}")
    print()

    # --- Check for consumption gaps ---
    print("=== CONSUMPTION GAPS (>10s between commits) ===")
    for p in progress:
        gaps = _find_consumption_gaps(server_lines, p['group_short'])
        if gaps:
            print(f"  Level {p['level']} ({p['group_short']}):")
            for start, end, dur in gaps:
                print(f"    {start} - {end} ({dur:.1f}s)")
        else:
            print(f"  Level {p['level']} ({p['group_short']}): no gaps > 10s "
                  f"(or too few commits)")
    print()

    # --- Server-side fetch watch analysis per topic ---
    print("=== SERVER FETCH WATCHES (per topic in chain) ===")
    chain_topics = set()
    for g in target_chain:
        if g['consume_topic']:
            chain_topics.add(g['consume_topic'][:8])
        if g['produce_topic']:
            chain_topics.add(g['produce_topic'][:8])
    for topic_prefix in sorted(chain_topics):
        creates = []
        for sline in server_lines:
            if 'watch created' in sline and topic_prefix in sline:
                m = re.match(r'(\d+:\d+:\d+\.\d+)', sline)
                if m:
                    # Extract partitions from the line
                    pm = re.search(r'partitions=(\d+) \(([^)]*)\)', sline)
                    parts = pm.group(2) if pm else '?'
                    creates.append((m.group(1), parts))
        if not creates:
            print(f"  Topic {topic_prefix}: no watches")
            continue
        # Bucket by 10s intervals
        buckets = defaultdict(int)
        part_counts = defaultdict(int)
        for ts, parts in creates:
            # Extract partition numbers
            for pm in re.finditer(r'\[(\d+)\]', parts):
                part_counts[int(pm.group(1))] += 1
            secs = int(parse_ts(ts))
            bucket = f"{secs // 60 % 60:02d}:{secs % 60 // 10}0"
            buckets[bucket] += 1
        print(f"  Topic {topic_prefix}: {len(creates)} watches, "
              f"first={creates[0][0]}, last={creates[-1][0]}")
        print(f"    Per-partition: {dict(sorted(part_counts.items()))}")
        # Show last 5 watches
        if len(creates) > 5:
            print(f"    Last 5 watches:")
            for ts, parts in creates[-5:]:
                print(f"      {ts}  {parts}")
    print()

    # --- Fetch entry analysis per topic ---
    print("=== SERVER FETCH ENTRIES (per topic in chain) ===")
    for topic_prefix in sorted(chain_topics):
        entries = []
        for sline in server_lines:
            if 'fetch: entry' in sline and topic_prefix in sline:
                m = re.match(r'(\d+:\d+:\d+\.\d+)', sline)
                if m:
                    nm = re.search(r'node=(\d+)', sline)
                    rcm = re.search(r'readCommitted=(\w+)', sline)
                    entries.append((m.group(1),
                                    nm.group(1) if nm else '?',
                                    rcm.group(1) if rcm else '?'))
        if not entries:
            print(f"  Topic {topic_prefix}: no fetch entries")
        else:
            # Count per node
            node_counts = defaultdict(int)
            for _, node, _ in entries:
                node_counts[node] += 1
            rc = entries[0][2]
            print(f"  Topic {topic_prefix}: {len(entries)} fetches, "
                  f"readCommitted={rc}, per-node={dict(sorted(node_counts.items()))}, "
                  f"first={entries[0][0]}, last={entries[-1][0]}")
    print()

    # --- No-watch reasons per topic ---
    print("=== NO-WATCH REASONS (per topic in chain) ===")
    for topic_prefix in sorted(chain_topics):
        no_watch = []
        for sline in server_lines:
            if 'fetch: noWatch' in sline:
                # noWatch doesn't include topic names directly -
                # correlate via nearby fetch: entry logs
                m = re.match(r'(\d+:\d+:\d+\.\d+)', sline)
                if m:
                    reason = ''
                    rm = re.search(r'returnEarly=(\w+)\(([^)]*)\)', sline)
                    if rm:
                        reason = f"returnEarly={rm.group(1)}({rm.group(2)})"
                    pm = re.search(r'nbytes=(\d+) MinBytes=(\d+)', sline)
                    if pm:
                        reason += f" nbytes={pm.group(1)} MinBytes={pm.group(2)}"
                    dm = re.search(r'pastDeadline=(\w+)', sline)
                    if dm:
                        reason += f" pastDeadline={dm.group(1)}"
                    no_watch.append((m.group(1), reason))
        if no_watch:
            # Bucket reasons
            reason_counts = defaultdict(int)
            for _, reason in no_watch:
                # Extract key reason
                if 'returnEarly=true' in reason:
                    key = re.search(r'\(([^)]*)\)', reason)
                    key = key.group(1)[:40] if key else 'unknown'
                elif 'pastDeadline=true' in reason:
                    key = 'pastDeadline'
                elif 'nbytes=' in reason:
                    nm = re.search(r'nbytes=(\d+)', reason)
                    key = f'hasData(nbytes={nm.group(1)})' if nm else 'hasData'
                else:
                    key = 'unknown'
                reason_counts[key] += 1
            print(f"  Total no-watch events: {len(no_watch)}, "
                  f"first={no_watch[0][0]}, last={no_watch[-1][0]}")
            print(f"    Reasons: {dict(reason_counts)}")
        else:
            print("  No no-watch events found (all fetches created watches or were watch callbacks)")
    print()

    # --- Txn commit watcher notifications ---
    print("=== TXN COMMIT WATCHER NOTIFICATIONS (per topic in chain) ===")
    for topic_prefix in sorted(chain_topics):
        notifs = []
        for sline in server_lines:
            if 'watches=' in sline and 'rcWatches=' in sline and topic_prefix in sline:
                m = re.match(r'(\d+:\d+:\d+\.\d+)', sline)
                if m:
                    wm = re.search(r'watches=(\d+) rcWatches=(\d+) txnBytes=(\d+)', sline)
                    if wm:
                        notifs.append((m.group(1), int(wm.group(1)),
                                       int(wm.group(2)), int(wm.group(3))))
        if notifs:
            total_w = sum(n[1] for n in notifs)
            total_rc = sum(n[2] for n in notifs)
            zero_w = sum(1 for n in notifs if n[1] == 0)
            print(f"  Topic {topic_prefix}: {len(notifs)} commits, "
                  f"totalWatches={total_w}, totalRCWatches={total_rc}, "
                  f"zeroWatchCommits={zero_w}, "
                  f"first={notifs[0][0]}, last={notifs[-1][0]}")
        else:
            print(f"  Topic {topic_prefix}: no txn commit notifications")
    print()

    # --- Session filtering events ---
    print("=== FETCH SESSION FILTERING ===")
    filter_events = []
    for sline in server_lines:
        if 'session filtered' in sline:
            m = re.match(r'(\d+:\d+:\d+\.\d+)', sline)
            if m:
                fm = re.search(r'parts=(\d+)->(\d+)', sline)
                if fm:
                    filter_events.append((m.group(1),
                                          int(fm.group(1)), int(fm.group(2))))
    if filter_events:
        total_filtered = sum(e[1] - e[2] for e in filter_events)
        print(f"  {len(filter_events)} filter events, "
              f"totalPartitionsFiltered={total_filtered}, "
              f"first={filter_events[0][0]}, last={filter_events[-1][0]}")
    else:
        print("  No session filtering events (all partitions included)")
    print()

    # --- Rebalance activity ---
    print("=== REBALANCE ACTIVITY ===")
    for g in target_chain:
        count, last_ts = _find_rebalance_activity(
            client_lines, g['group_short'])
        print(f"  {g['group_short']}: {count} rebalances, last at {last_ts}")
    print()

    # --- Heartbeat activity ---
    print("=== HEARTBEAT ACTIVITY ===")
    for g in target_chain:
        count, first_ts, last_ts = _find_heartbeat_activity(
            client_lines, g['group_short'])
        print(f"  {g['group_short']}: {count} heartbeat events, "
              f"first={first_ts}, last={last_ts}")
    print()

    # --- ETL errors ---
    all_chain_groups = [g for chain in chains for g in chain]
    errors = _find_etl_errors(client_lines, all_chain_groups)
    print(f"=== ETL ERRORS ({len(errors)}) ===")
    if errors:
        for lineno, pat, text, relevant in errors[:20]:
            tag = "" if relevant else " [maybe unrelated]"
            print(f"  L{lineno} ({pat}){tag}: {text[:160]}")
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more")
    else:
        print("  No ETL errors found.")
    print()

    # --- Stuck goroutines ---
    stuck = _find_stuck_goroutines(client_lines, target_chain)
    print(f"=== STUCK GOROUTINES ({len(stuck)}) ===")
    for g in stuck:
        state = g['state']
        if g['duration']:
            state += f" ({g['duration']})"
        print(f"  goroutine {g['id']}: {state}")
        for func, loc in g['frames'][:6]:
            if 'runtime' not in func and 'internal/' not in func:
                short_loc = loc.split('/')[-1] if loc else ''
                print(f"    {func[:100]}")
                if short_loc:
                    print(f"      {short_loc}")
    print()

    # --- Diagnosis summary ---
    print("=== DIAGNOSIS ===")
    # Check each level: is it stuck?
    for p in progress:
        level = p['level']
        consumed = p['total_consumed']
        ct_hwm = p['consume_total']
        pt_hwm = p['total_produced']
        if ct_hwm > 0 and consumed < ct_hwm:
            deficit = ct_hwm - consumed
            print(f"  Level {level}: UNDERCONSUMED by {deficit} records "
                  f"({consumed}/{ct_hwm})")
            if level > 1:
                prev = progress[level - 2]
                if prev['total_produced'] < ct_hwm:
                    print(f"    -> Upstream level {level-1} only produced "
                          f"{prev['total_produced']} (HWM of consume topic is {ct_hwm})")
        elif ct_hwm > 0:
            print(f"  Level {level}: consumed {consumed}/{ct_hwm} (100%)")
        else:
            print(f"  Level {level}: consumed {consumed}, HWM unknown")

    # Check for the classic problem: all records consumed but counter < limit
    print()
    print("  Key observations:")
    for p in progress:
        consumed = p['total_consumed']
        if consumed == 500000:
            print(f"    - Level {p['level']} committed exactly 500,000 offsets "
                  f"(= testRecordLimit)")
        elif consumed > 450000:
            print(f"    - Level {p['level']} committed {consumed} offsets "
                  f"(close to 500,000)")

    # Check if there's a mismatch between levels
    if len(progress) >= 2:
        for i in range(1, len(progress)):
            prev_produced = progress[i-1]['total_produced']
            curr_consumed = progress[i]['total_consumed']
            curr_ct_hwm = progress[i]['consume_total']
            if prev_produced > 0 and curr_ct_hwm > 0:
                if prev_produced != curr_ct_hwm:
                    print(f"    - Level {i}: produced {prev_produced} but "
                          f"level {i+1} consume topic HWM is {curr_ct_hwm}")


# ---------------------------------------------------------------------------
# Goroutine dump analysis (from test timeout / panic output)
# ---------------------------------------------------------------------------

def parse_goroutine_dump(lines):
    """Parse Go goroutine dump into structured data.

    Returns list of dicts with keys: id, state, duration, frames.
    Each frame is (func, file_line).
    """
    goroutines = []
    current = None
    for line in lines:
        line = line.rstrip('\n')
        # goroutine 4612 [select, 4 minutes]:
        m = re.match(r'goroutine (\d+) \[([^\]]+)\]:', line)
        if m:
            if current:
                goroutines.append(current)
            state = m.group(2)
            duration = ''
            dm = re.search(r',\s*(\d+ \w+)', state)
            if dm:
                duration = dm.group(1)
                state = state[:state.index(',')].strip()
            current = {
                'id': int(m.group(1)),
                'state': state,
                'duration': duration,
                'frames': [],
                'raw_lines': [line],
            }
            continue
        if current is not None:
            current['raw_lines'].append(line)
            if line == '':
                goroutines.append(current)
                current = None
                continue
            # Function line (not file:line)
            if line.startswith('\t'):
                # file:line reference
                if current['frames']:
                    current['frames'][-1] = (current['frames'][-1][0], line.strip())
            else:
                current['frames'].append((line.strip(), ''))
    if current:
        goroutines.append(current)
    return goroutines


def classify_goroutine(g):
    """Classify a goroutine by what subsystem it belongs to."""
    frames_str = ' '.join(f[0] for f in g['frames'])

    if 'TestIssue' in frames_str or 'consumer_direct_test' in frames_str:
        return 'test:Issue'
    if 'TestGroup' in frames_str or 'TestTxn' in frames_str:
        test_m = re.search(r'(Test\w+)', frames_str)
        return f'test:{test_m.group(1)[:30]}' if test_m else 'test:unknown'

    if 'kfake' in frames_str:
        return 'kfake'

    if 'Flush' in frames_str:
        return 'kgo:flush'
    if 'heartbeat' in frames_str:
        return 'kgo:heartbeat'
    if 'manage848' in frames_str or 'manage(' in frames_str:
        return 'kgo:group-manage'
    if 'handleSeqResps' in frames_str:
        return 'kgo:sink-seq'
    if 'readConn' in frames_str or 'readResponse' in frames_str or 'handleResp' in frames_str:
        return 'kgo:broker-read'
    if 'writeRequest' in frames_str:
        return 'kgo:broker-write'
    if 'updateMetadataLoop' in frames_str:
        return 'kgo:metadata'
    if 'PollRecords' in frames_str or 'manageFetchConcurrency' in frames_str:
        return 'kgo:consumer'
    if 'findNewAssignments' in frames_str:
        return 'kgo:group-find'
    if 'loopFetch' in frames_str or 'source.go' in frames_str:
        return 'kgo:fetch-loop'
    if 'sink.go' in frames_str:
        return 'kgo:sink'

    if 'testing.' in frames_str:
        return 'testing'
    if 'runtime.' in frames_str:
        return 'runtime'

    return 'other'


def main_goroutine_dump(args):
    """Analyze goroutine dump from test timeout/panic.

    Parses the goroutine dump embedded in Go test output, classifies
    goroutines by subsystem (kgo, kfake, test, runtime), and shows:

    - State summary: how many goroutines in each state
    - Subsystem summary: goroutine counts per subsystem
    - Mutex contention: what locks are being waited on
    - Stuck tests: which test goroutines are blocked and where
    - Long-blocked goroutines: anything blocked > 1 minute
    """
    with open(args.client) as f:
        all_lines = f.readlines()

    # Find the goroutine dump section (starts after "panic: test timed out")
    dump_start = -1
    for i, line in enumerate(all_lines):
        if 'panic: test timed out' in line or 'goroutine profile' in line:
            dump_start = i
            break
    if dump_start < 0:
        # Try to find any goroutine dump
        for i, line in enumerate(all_lines):
            if re.match(r'goroutine \d+ \[', line):
                dump_start = i
                break
    if dump_start < 0:
        print("No goroutine dump found in client log")
        return

    dump_lines = all_lines[dump_start:]
    goroutines = parse_goroutine_dump(dump_lines)
    print(f"Parsed {len(goroutines)} goroutines from dump")
    print()

    # Find which test timed out
    for line in all_lines[dump_start:dump_start+5]:
        if 'running tests' in line or 'Test' in line:
            print(f"  {line.rstrip()}")

    # --- State summary ---
    state_counts = defaultdict(int)
    for g in goroutines:
        s = g['state']
        if g['duration']:
            s += f" ({g['duration']})"
        state_counts[s] += 1
    print("\n=== GOROUTINE STATES ===")
    for state, count in sorted(state_counts.items(), key=lambda x: -x[1]):
        print(f"  {count:>4}  {state}")

    # --- Subsystem summary ---
    subsystem_counts = defaultdict(int)
    subsystem_goroutines = defaultdict(list)
    for g in goroutines:
        sub = classify_goroutine(g)
        subsystem_counts[sub] += 1
        subsystem_goroutines[sub].append(g)
    print("\n=== SUBSYSTEM SUMMARY ===")
    for sub, count in sorted(subsystem_counts.items(), key=lambda x: -x[1]):
        print(f"  {count:>4}  {sub}")

    # --- Mutex contention ---
    mutex_goroutines = [g for g in goroutines if g['state'] == 'sync.Mutex.Lock']
    if mutex_goroutines:
        print(f"\n=== MUTEX CONTENTION ({len(mutex_goroutines)} goroutines) ===")
        # Group by what function is trying to acquire the lock
        lock_sites = defaultdict(int)
        for g in mutex_goroutines:
            # Find the caller of Lock (skip runtime/sync frames)
            caller = '?'
            for func, loc in g['frames']:
                if 'sync.' in func or 'runtime' in func or 'internal/' in func:
                    continue
                caller = func.split('(')[0]  # strip args
                if loc:
                    caller += f' ({loc.split("/")[-1]})'
                break
            lock_sites[caller] += 1
        for site, count in sorted(lock_sites.items(), key=lambda x: -x[1]):
            print(f"  {count:>4}  {site}")

    # --- Stuck tests ---
    print("\n=== STUCK TEST GOROUTINES ===")
    test_goroutines = [g for g in goroutines
                       if any('_test.go' in f[1] for f in g['frames'])]
    for g in test_goroutines:
        # Find the test function and where it's stuck
        test_func = '?'
        stuck_at = '?'
        for func, loc in g['frames']:
            if '_test.go' in loc:
                test_func = func.split('(')[0]
                stuck_at = loc.split('/')[-1] if loc else '?'
                break
        state = g['state']
        if g['duration']:
            state += f" ({g['duration']})"
        print(f"  gor {g['id']:>7}  {state:<30}  {test_func}")
        # Show top non-runtime frames
        for func, loc in g['frames'][:5]:
            if 'runtime' not in func and 'internal/' not in func:
                short_loc = loc.split('/')[-1] if loc else ''
                print(f"    {func[:80]}")
                if short_loc:
                    print(f"      {short_loc}")

    # --- Long-blocked goroutines ---
    long_blocked = [g for g in goroutines if g['duration'] and
                    any(kw in g['duration'] for kw in ['minute', 'hour'])]
    if long_blocked:
        print(f"\n=== LONG-BLOCKED GOROUTINES ({len(long_blocked)}) ===")
        # Group by top frame
        blocked_by_frame = defaultdict(list)
        for g in long_blocked:
            top = g['frames'][0][0] if g['frames'] else '?'
            # Simplify: just the function name
            top = top.split('(')[0]
            blocked_by_frame[top].append(g)
        for frame, gs in sorted(blocked_by_frame.items(), key=lambda x: -len(x[1])):
            durations = set(g['duration'] for g in gs)
            print(f"  {len(gs):>4}  {frame}")
            print(f"        durations: {', '.join(sorted(durations))}")

    # --- Flush analysis ---
    flush_goroutines = subsystem_goroutines.get('kgo:flush', [])
    if flush_goroutines:
        print(f"\n=== FLUSH GOROUTINES ({len(flush_goroutines)}) ===")
        for g in flush_goroutines:
            state = g['state']
            if g['duration']:
                state += f" ({g['duration']})"
            print(f"  gor {g['id']:>7}  {state}")
            for func, loc in g['frames'][:4]:
                if 'runtime' not in func and 'internal/' not in func:
                    short_loc = loc.split('/')[-1] if loc else ''
                    print(f"    {func[:80]}")
                    if short_loc:
                        print(f"      {short_loc}")

    # --- Deadlock analysis ---
    # For each contended mutex address, find who might be HOLDING it.
    # A goroutine holding a mutex will have Lock() in its stack but
    # be blocked on something ELSE further up.
    print(f"\n=== DEADLOCK ANALYSIS ===")

    # Group mutex waiters by the mutex address they're waiting on.
    mutex_addrs = defaultdict(list)
    for g in goroutines:
        if g['state'] != 'sync.Mutex.Lock':
            continue
        for func, loc in g['frames']:
            # Look for the Lock call with an address
            addr_m = re.search(r'Lock\((0x[0-9a-f]+)\)', func)
            if addr_m:
                mutex_addrs[addr_m.group(1)].append(g)
                break

    for addr, waiters in sorted(mutex_addrs.items(), key=lambda x: -len(x[1])):
        if len(waiters) < 3:
            continue
        print(f"\n  Mutex {addr}: {len(waiters)} goroutines waiting")

        # Categorize what the waiters are trying to do
        waiter_ops = defaultdict(int)
        for g in waiters:
            for func, loc in g['frames']:
                if 'sync.' in func or 'runtime' in func or 'internal/' in func:
                    continue
                op = func.split('(')[0]
                short_loc = loc.split('/')[-1] if loc else ''
                waiter_ops[f"{op} ({short_loc})"] += 1
                break
        print(f"  Waiting to do:")
        for op, count in sorted(waiter_ops.items(), key=lambda x: -x[1]):
            print(f"    {count:>4}  {op}")

        # Find potential holders: goroutines that are NOT waiting on
        # this mutex but have this address somewhere in their stack,
        # OR goroutines in 'running'/'runnable' state.
        potential_holders = []
        for g in goroutines:
            if g in waiters:
                continue
            raw = '\n'.join(g['raw_lines'])
            if addr in raw:
                potential_holders.append(('has_addr', g))
            # Also check: goroutines stuck in Pool.Get/Put that aren't
            # at the Lock line - they might hold the lock
            elif g['state'] in ('running', 'runnable'):
                frames_str = ' '.join(f[0] for f in g['frames'])
                if 'Pool' in frames_str or 'pool' in frames_str:
                    potential_holders.append(('running_pool', g))

        if potential_holders:
            print(f"  Potential holders:")
            for reason, g in potential_holders[:10]:
                state = g['state']
                if g['duration']:
                    state += f" ({g['duration']})"
                top_func = '?'
                for func, loc in g['frames']:
                    if 'runtime' not in func and 'internal/' not in func:
                        top_func = func.split('(')[0]
                        short_loc = loc.split('/')[-1] if loc else ''
                        if short_loc:
                            top_func += f" ({short_loc})"
                        break
                print(f"    gor {g['id']:>7} [{state}] {reason}: {top_func}")
                # Show full stack for potential holders
                for func, loc in g['frames'][:8]:
                    short_loc = loc.split('/')[-1] if loc else ''
                    print(f"      {func[:90]}")
                    if short_loc:
                        print(f"        {short_loc}")
        else:
            print(f"  No obvious holder found (lock may be held by a goroutine")
            print(f"  in a runtime state not visible in the dump)")

    # --- Cross-lock dependencies ---
    # Check if any goroutine holds one mutex while waiting for another.
    print(f"\n=== CROSS-LOCK DEPENDENCIES ===")
    cross_deps = []
    for g in goroutines:
        if g['state'] != 'sync.Mutex.Lock':
            continue
        # This goroutine is waiting on a Lock. Check if it's inside
        # a function that likely already holds another lock.
        waiting_addr = None
        for func, loc in g['frames']:
            addr_m = re.search(r'Lock\((0x[0-9a-f]+)\)', func)
            if addr_m:
                waiting_addr = addr_m.group(1)
                break
        if not waiting_addr:
            continue
        # Check stack for patterns suggesting held locks
        frames_str = ' '.join(f[0] + ' ' + f[1] for f in g['frames'])
        held_patterns = []
        if 'Pool.pin' in frames_str and 'pinSlow' in frames_str:
            held_patterns.append('sync.Pool internal')
        if 'Mutex.Lock' in frames_str:
            # Find ALL mutex addresses in the stack
            all_addrs = set()
            for func, loc in g['frames']:
                for m in re.finditer(r'(0x[0-9a-f]+)', func):
                    all_addrs.add(m.group(1))
            if len(all_addrs) > 1:
                held_patterns.append(f"multiple addrs: {all_addrs}")
        if held_patterns:
            cross_deps.append((g, waiting_addr, held_patterns))

    if cross_deps:
        for g, addr, patterns in cross_deps[:5]:
            print(f"  gor {g['id']}: waiting on {addr}, also: {patterns}")
    else:
        print("  No cross-lock dependencies detected in goroutine stacks.")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze franz-go / kfake test logs.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest='command', help='analysis mode')

    # --- client-trace subcommand ---
    csv_p = sub.add_parser('client-trace', help='trace client debug log events',
        description=main_csv.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    csv_p.add_argument('log_file',
        help='path to the client debug log file')
    csv_p.add_argument('-p', '--partition', type=int, default=118,
        help='partition number to track (default: 118)')
    csv_p.add_argument('-s', '--start', type=int, default=1,
        help='start line number, 1-indexed (default: 1)')
    csv_p.add_argument('-e', '--end', type=int, default=0,
        help='end line number, 0 = end of file (default: 0)')
    csv_p.add_argument('-v', '--verbose', action='store_true',
        help='show ALL wire events, not just target partition')
    csv_p.add_argument('--all-partitions', action='store_true',
        help='show fetch events for all partitions')
    csv_p.add_argument('--fetch-only', action='store_true',
        help='only show fetch request/response events')
    csv_p.add_argument('--session-only', action='store_true',
        help='only show group session and transaction events')

    # --- dual-assign subcommand ---
    p848 = sub.add_parser('dual-assign', help='diagnose 848 dual-assignment bugs',
        description=main_848.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p848.add_argument('--client', default='/tmp/kfake_test_logs/client.log',
        help='kgo test client log file (default: /tmp/kfake_test_logs/client.log)')
    p848.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='kfake server log file (default: /tmp/kfake_test_logs/server.log)')
    p848.add_argument('-p', '--partition', type=int, default=-1,
        help='partition to trace; -1 = auto-detect from failure (default: -1)')
    p848.add_argument('-g', '--group', default='',
        help='consumer group ID to filter (default: auto-detect)')
    p848.add_argument('-t', '--topic', default='',
        help='topic name to filter (default: auto-detect)')
    p848.add_argument('--server-only', action='store_true',
        help='only show server-side RECONCILE/PRUNE/FENCE events')

    # --- txn-timeout subcommand ---
    ptxn = sub.add_parser('txn-timeout', help='diagnose transaction timeout failures',
        description=main_txn_timeout.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ptxn.add_argument('--client', default='/tmp/kfake_test_logs/client.log',
        help='kgo test client log file (default: /tmp/kfake_test_logs/client.log)')
    ptxn.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='kfake server log file (default: /tmp/kfake_test_logs/server.log)')

    # --- produce-order subcommand ---
    pprod = sub.add_parser('produce-order', help='diagnose produce offset order failures',
        description=main_produce_order.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    pprod.add_argument('--client', default='/tmp/kfake_test_logs/client.log',
        help='kgo test client log file')
    pprod.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='kfake server log file')

    # --- server-stall subcommand ---
    pstall = sub.add_parser('server-stall', help='analyze server-side stalls causing txn timeouts',
        description=main_server_stall.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    pstall.add_argument('--client', default='/tmp/kfake_test_logs/client.log',
        help='kgo test client log file (default: /tmp/kfake_test_logs/client.log)')
    pstall.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='kfake server log file (default: /tmp/kfake_test_logs/server.log)')

    # --- etl-stuck subcommand ---
    petl = sub.add_parser('etl-stuck', help='diagnose stuck ETL chain tests',
        description=main_etl_stuck.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    petl.add_argument('--client', default='/tmp/kfake_test_logs/client.log',
        help='kgo test client log file (default: /tmp/kfake_test_logs/client.log)')
    petl.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='kfake server log file (default: /tmp/kfake_test_logs/server.log)')
    petl.add_argument('-g', '--group', default='',
        help='group ID prefix to focus on (default: auto-detect)')
    petl.add_argument('--all-chains', action='store_true',
        help='analyze all chains, not just the stuck one')

    # --- lso-state subcommand ---
    plso = sub.add_parser('lso-state', help='show LSO vs HWM per topic from server logs',
        description=main_lso_state.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    plso.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='kfake server log file (default: /tmp/kfake_test_logs/server.log)')
    plso.add_argument('-t', '--topic', default='',
        help='filter to topics containing this string')
    plso.add_argument('-v', '--verbose', action='store_true',
        help='show all partitions, not just stuck ones')

    # --- goroutine-dump subcommand ---
    pgor = sub.add_parser('goroutine-dump', help='analyze goroutine dump from test timeout',
        description=main_goroutine_dump.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    pgor.add_argument('--client', default='/tmp/kfake_test_logs/client.log',
        help='kgo test client log file (default: /tmp/kfake_test_logs/client.log)')

    # --- commit-timeline subcommand ---
    pcom = sub.add_parser('commit-timeline',
        help='show commit rate timeline for groups',
        description=main_commit_timeline.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    pcom.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='server log file')
    pcom.add_argument('--groups', required=True,
        help='comma-separated group ID prefixes')
    pcom.add_argument('--topics', default='',
        help='comma-separated topic prefixes for fetch watch analysis')

    # --- 848-state subcommand ---
    p848s = sub.add_parser('848-state',
        help='848 group convergence analysis',
        description=main_848_state.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p848s.add_argument('--client', default='/tmp/kfake_test_logs/client.log')
    p848s.add_argument('--server', default='/tmp/kfake_test_logs/server.log')
    p848s.add_argument('--groups', required=True,
        help='comma-separated group ID prefixes to compare')
    p848s.add_argument('--after', default='',
        help='only show events after this timestamp (HH:MM:SS)')

    # --- 848-summary subcommand ---
    p848sum = sub.add_parser('848-summary',
        help='quick overview of all 848 groups',
        description=main_848_summary.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p848sum.add_argument('--server', default='/tmp/kfake_test_logs/server.log')

    p848conv = sub.add_parser('848-convergence',
        help='848 revocation timing and convergence bottleneck analysis',
        description=main_848_convergence.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p848conv.add_argument('--client', default='/tmp/kfake_test_logs/client.log')
    p848conv.add_argument('--server', default='/tmp/kfake_test_logs/server.log')
    p848conv.add_argument('--groups', default='',
        help='comma-separated group ID prefixes (default: auto-detect all)')

    pdr = sub.add_parser('dropped-response',
        help='find hijacked requests that never got a response',
        description=main_dropped_response.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    pdr.add_argument('--server', default='/tmp/kfake_test_logs/server.log')

    pft = sub.add_parser('fetch-trace',
        help='trace fetch activity for a specific topic',
        description='Trace per-minute fetch activity for a topic from server logs.\n'
                    'Shows fetch entries, watches, session filtering, and stall detection.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    pft.add_argument('--server', default='/tmp/kfake_test_logs/server.log')
    pft.add_argument('-t', '--topic', required=True,
        help='topic name or prefix to filter')

    p848at = sub.add_parser('848-assign-trace',
        help='trace 848 assignment flow for a specific group',
        description='Trace 848 assignment lifecycle from client log: handleResp comparisons,\n'
                    'assignment changes, setupAssignedAndHeartbeat returns, keepalive counts,\n'
                    'and response errors. Shows where assignment gets stuck.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p848at.add_argument('--client', default='/tmp/kfake_test_logs/client.log')
    p848at.add_argument('--server', default='/tmp/kfake_test_logs/server.log')
    p848at.add_argument('-g', '--group', required=True,
        help='group ID or prefix to filter')
    p848at.add_argument('--after', default='',
        help='only show events after this timestamp (HH:MM:SS)')

    pcls = sub.add_parser('classic-summary',
        help='quick overview of all classic consumer groups',
        description=main_classic_summary.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    pcls.add_argument('--server', default='/tmp/kfake_test_logs/server.log')

    pgtl = sub.add_parser('group-timeline',
        help='chronological activity timeline for a specific group',
        description=main_group_timeline.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    pgtl.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='server log file')
    pgtl.add_argument('--client', default='/tmp/kfake_test_logs/client.log',
        help='client log file for error scanning')
    pgtl.add_argument('--group', '-g', required=True,
        help='group ID or prefix to filter')
    pgtl.add_argument('--after', default='',
        help='only show events after this timestamp (HH:MM:SS)')
    pgtl.add_argument('--before', default='',
        help='only show events before this timestamp (HH:MM:SS)')
    pgtl.add_argument('--gap', type=float, default=5.0,
        help='minimum gap in seconds to highlight (default: 5)')
    pgtl.add_argument('--show', type=int, default=30,
        help='number of first/last events to display (default: 30)')

    ptxnp = sub.add_parser('txn-produce',
        help='trace produce requests per PID per epoch from server log',
        description='Trace produce requests per PID per epoch from server log.\n'
                    'Groups consecutive produce batches into requests, shows\n'
                    'record counts per epoch, and highlights epoch mismatches.')
    ptxnp.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='server log file (default: /tmp/kfake_test_logs/server.log)')
    ptxnp.add_argument('--pid', type=int, default=0,
        help='filter to a specific producer ID (default: show all)')

    ptxnetl = sub.add_parser('txn-etl',
        help='txn commit/abort analysis per ETL group',
        description=main_txn_etl.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ptxnetl.add_argument('--client', default='/tmp/kfake_test_logs/client.log',
        help='kgo test client log file (default: /tmp/kfake_test_logs/client.log)')
    ptxnetl.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='kfake server log file (default: /tmp/kfake_test_logs/server.log)')
    ptxnetl.add_argument('-g', '--group', default='',
        help='group ID prefix to focus on (default: all)')

    pcs = sub.add_parser('chain-stuck',
        help='investigate why specific groups had zero offset commits',
        description=main_chain_stuck.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    pcs.add_argument('--client', default='/tmp/kfake_test_logs/client.log',
        help='kgo test client log file')
    pcs.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='kfake server log file')
    pcs.add_argument('--groups', required=True,
        help='comma-separated group ID prefixes to investigate')
    pcs.add_argument('-v', '--verbose', action='store_true',
        help='show more lifecycle events per group')

    pov = sub.add_parser('test-overview',
        help='overview of test run: timing, group counts, timeout info',
        description=main_test_overview.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    pov.add_argument('--client', default='/tmp/kfake_test_logs/client.log',
        help='kgo test client log file')
    pov.add_argument('--server', default='/tmp/kfake_test_logs/server.log',
        help='kfake server log file')

    args = parser.parse_args()
    if args.command == 'txn-etl':
        main_txn_etl(args)
    elif args.command == 'txn-produce':
        main_txn_produce(args)
    elif args.command == 'client-trace':
        main_csv(args)
    elif args.command == 'dual-assign':
        main_848(args)
    elif args.command == 'txn-timeout':
        main_txn_timeout(args)
    elif args.command == 'produce-order':
        main_produce_order(args)
    elif args.command == 'server-stall':
        main_server_stall(args)
    elif args.command == 'etl-stuck':
        main_etl_stuck(args)
    elif args.command == 'lso-state':
        main_lso_state(args)
    elif args.command == 'goroutine-dump':
        main_goroutine_dump(args)
    elif args.command == 'commit-timeline':
        main_commit_timeline(args)
    elif args.command == '848-state':
        main_848_state(args)
    elif args.command == '848-summary':
        main_848_summary(args)
    elif args.command == '848-convergence':
        main_848_convergence(args)
    elif args.command == 'classic-summary':
        main_classic_summary(args)
    elif args.command == 'dropped-response':
        main_dropped_response(args)
    elif args.command == 'fetch-trace':
        main_fetch_trace(args)
    elif args.command == '848-assign-trace':
        main_848_assign_trace(args)
    elif args.command == 'group-timeline':
        main_group_timeline(args)
    elif args.command == 'chain-stuck':
        main_chain_stuck(args)
    elif args.command == 'test-overview':
        main_test_overview(args)
    else:
        parser.print_help()


def main_txn_etl(args):
    """Analyze transactional commit/abort ratios per ETL chain group.

    For each consumer in a transactional ETL chain, shows:
    - Number of committed vs aborted transactions
    - Which txids are associated with which groups
    - Timeline of commits and aborts in 5-second buckets
    - Offset progress from TxnOffsetCommit lines

    Useful for diagnosing TestTxnEtl timeouts where consumer groups
    churn and abort all their transactions.
    """
    with open(args.client) as f:
        client_lines = f.readlines()
    with open(args.server) as f:
        server_lines = f.readlines()

    # --- Discover ETL chains ---
    all_groups = _find_848_groups(client_lines)
    chains = _build_etl_chain(all_groups)
    group_shorts = {g['group_short'] for g in all_groups}

    # Also discover classic groups from server OffsetCommit/TxnOffsetCommit
    for line in server_lines:
        m = re.search(r'(?:Txn)?OffsetCommit: group=(\S+)', line)
        if m:
            gs = m.group(1)[:16]
            group_shorts.add(gs)

    # --- Parse EndTxn lines ---
    # "ending transaction (commit|abort), batches=N groups=[g1,g2]"
    p_endtxn = re.compile(
        r'(\d+:\d+:\d+\.\d+) \[DBG\] txn: pid (\d+) epoch (\d+) '
        r'txid "([^"]*)" ending transaction \((\w+)\), batches=(\d+) groups=\[(.*?)\]'
    )
    # txid => {commits, aborts, groups, first_ts, last_ts, total_batches_committed}
    txid_stats = {}
    # group_short => {commits, aborts, txids, commit_times, abort_times}
    group_stats = defaultdict(lambda: {
        'commits': 0, 'aborts': 0, 'txids': set(),
        'commit_times': [], 'abort_times': [],
    })

    for line in server_lines:
        m = p_endtxn.search(line)
        if not m:
            continue
        ts, pid, epoch, txid, action, batches, groups_str = m.groups()
        batches = int(batches)

        if txid not in txid_stats:
            txid_stats[txid] = {
                'commits': 0, 'aborts': 0, 'groups': set(),
                'first_ts': ts, 'last_ts': ts,
                'total_batches_committed': 0, 'pid': int(pid),
            }
        s = txid_stats[txid]
        s['last_ts'] = ts
        if action == 'commit':
            s['commits'] += 1
            s['total_batches_committed'] += batches
        else:
            s['aborts'] += 1

        # Parse groups
        endtxn_groups = [g.strip() for g in groups_str.split(',') if g.strip()]
        for g in endtxn_groups:
            gs = g[:16]
            s['groups'].add(gs)
            gs_info = group_stats[gs]
            gs_info['txids'].add(txid[:16])
            if action == 'commit':
                gs_info['commits'] += 1
                gs_info['commit_times'].append(ts)
            else:
                gs_info['aborts'] += 1
                gs_info['abort_times'].append(ts)

    # --- Parse TxnOffsetCommit lines for offset progress ---
    # "TxnOffsetCommit: group=X topic=Y p=Z offset=W pid=P epoch=E"
    p_txnoc = re.compile(
        r'(\d+:\d+:\d+\.\d+) \[DBG\] TxnOffsetCommit: group=(\S+) '
        r'topic=(\S+) p=(\d+) offset=(\d+) pid=(\d+)'
    )
    # group_short => {topic => {part => max_offset}}
    group_offsets = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    group_txnoc_count = defaultdict(int)

    for line in server_lines:
        m = p_txnoc.search(line)
        if not m:
            continue
        ts, group, topic, part, offset, pid = m.groups()
        gs = group[:16]
        part = int(part)
        offset = int(offset)
        cur = group_offsets[gs][topic][part]
        if offset > cur:
            group_offsets[gs][topic][part] = offset
        group_txnoc_count[gs] += 1

    # --- Print results ---

    # ETL chains
    if chains:
        print(f"=== ETL CHAINS ({len(chains)}) ===")
        for ci, chain in enumerate(chains):
            print(f"\n  Chain {ci + 1}:")
            for i, g in enumerate(chain):
                gs = g['group_short']
                ct = (g['consume_topic'] or '?')[:16]
                pt = (g['produce_topic'] or '?')[:16]
                s = group_stats.get(gs, {'commits': 0, 'aborts': 0})
                total_off = sum(
                    off for topic in group_offsets.get(gs, {}).values()
                    for off in topic.values()
                )
                print(f"    Level {i+1}: {gs}  consumes={ct}  produces={pt}  "
                      f"commits={s['commits']}  aborts={s['aborts']}  "
                      f"txnOC_entries={group_txnoc_count.get(gs, 0)}  "
                      f"max_offset_sum={total_off}")
        print()

    # Per-group detail
    print("=== PER-GROUP TXN DETAIL ===")
    target_groups = sorted(group_stats.keys())
    if args.group:
        target_groups = [g for g in target_groups if g.startswith(args.group)]

    for gs in target_groups:
        s = group_stats[gs]
        total = s['commits'] + s['aborts']
        if total == 0:
            continue
        abort_pct = 100 * s['aborts'] / total if total > 0 else 0
        print(f"\n  Group: {gs}")
        print(f"    Transactions: {total} total, {s['commits']} committed, "
              f"{s['aborts']} aborted ({abort_pct:.0f}% abort rate)")
        print(f"    TxnOffsetCommit entries: {group_txnoc_count.get(gs, 0)}")
        print(f"    Txids: {sorted(s['txids'])}")

        # Offset progress
        if gs in group_offsets:
            for topic in sorted(group_offsets[gs]):
                parts = group_offsets[gs][topic]
                total_off = sum(parts.values())
                print(f"    Topic {topic[:16]}: max_offset_sum={total_off} "
                      f"across {len(parts)} partitions")

        # Timeline in 5-second buckets
        all_times = [(t, 'C') for t in s['commit_times']] + \
                    [(t, 'A') for t in s['abort_times']]
        if all_times:
            all_times.sort()
            first_sec = int(parse_ts(all_times[0][0]))
            last_sec = int(parse_ts(all_times[-1][0]))
            bucket_size = 5
            print(f"    Timeline ({bucket_size}s buckets):")
            for bucket_start in range(first_sec, last_sec + 1, bucket_size):
                bucket_end = bucket_start + bucket_size
                commits = sum(1 for t, a in all_times
                              if bucket_start <= parse_ts(t) < bucket_end and a == 'C')
                aborts = sum(1 for t, a in all_times
                             if bucket_start <= parse_ts(t) < bucket_end and a == 'A')
                if commits or aborts:
                    h = bucket_start // 3600
                    m = (bucket_start % 3600) // 60
                    sec = bucket_start % 60
                    bar_c = 'C' * commits
                    bar_a = 'A' * aborts
                    print(f"      {h:02d}:{m:02d}:{sec:02d}  "
                          f"commits={commits:>3}  aborts={aborts:>3}  "
                          f"|{bar_c}{bar_a}|")

    # Per-txid summary for txids associated with groups
    group_txids = set()
    for gs in target_groups:
        group_txids.update(group_stats[gs]['txids'])

    print("\n=== TXID SUMMARY (group-associated) ===")
    for txid in sorted(txid_stats):
        s = txid_stats[txid]
        if not s['groups']:
            continue
        txid_short = txid[:16]
        if args.group and not any(g.startswith(args.group) for g in s['groups']):
            continue
        groups_list = ','.join(sorted(s['groups']))
        print(f"  {txid_short}  pid={s['pid']}  "
              f"commits={s['commits']}  aborts={s['aborts']}  "
              f"batches_committed={s['total_batches_committed']}  "
              f"groups=[{groups_list}]  "
              f"span={s['first_ts']}-{s['last_ts']}")

    # --- Parse diagnostic logs ---
    # "fetch: readCommitted empty, blocked=N/M partitions behind LSO"
    p_fetch_blocked = re.compile(
        r'(\d+:\d+:\d+\.\d+) .* fetch: readCommitted empty, blocked=(\d+)/(\d+)')
    # "txn: EndTxn slow pid=P epoch=E elapsed=Nms batches=N groups=N"
    p_endtxn_slow = re.compile(
        r'(\d+:\d+:\d+\.\d+) .* txn: EndTxn slow pid=(\d+) epoch=(\d+) elapsed=(\d+)ms')
    # "consumerHeartbeatLate: group=G member=M gap=Nms interval=Nms"
    p_hb_late = re.compile(
        r'(\d+:\d+:\d+\.\d+) .* consumerHeartbeatLate: group=(\S+) member=(\S+) gap=(\d+)ms interval=(\d+)ms')

    fetch_blocked_count = 0
    endtxn_slow_events = []
    hb_late_events = []

    for line in server_lines:
        m = p_fetch_blocked.search(line)
        if m:
            fetch_blocked_count += 1
            continue
        m = p_endtxn_slow.search(line)
        if m:
            ts, pid, epoch, elapsed = m.groups()
            endtxn_slow_events.append((ts, int(pid), int(epoch), int(elapsed)))
            continue
        m = p_hb_late.search(line)
        if m:
            ts, group, member, gap, interval = m.groups()
            gs = group[:16]
            if not args.group or gs.startswith(args.group):
                hb_late_events.append((ts, gs, member, int(gap), int(interval)))

    print("\n=== DIAGNOSTICS ===")
    print(f"  readCommitted empty fetches blocked by LSO: {fetch_blocked_count}")
    if endtxn_slow_events:
        print(f"  Slow EndTxn events (>5ms): {len(endtxn_slow_events)}")
        for ts, pid, epoch, elapsed in endtxn_slow_events[:20]:
            print(f"    {ts} pid={pid} epoch={epoch} elapsed={elapsed}ms")
    else:
        print("  No slow EndTxn events")
    if hb_late_events:
        print(f"  Late heartbeats (>2x interval): {len(hb_late_events)}")
        for ts, gs, member, gap, interval in hb_late_events[:20]:
            print(f"    {ts} group={gs} member={member} gap={gap}ms interval={interval}ms")
    else:
        print("  No late heartbeats")

    # Also show producer-only txids (the initial producer that feeds the chain)
    print("\n=== PRODUCER-ONLY TXIDS (no group offsets) ===")
    for txid in sorted(txid_stats):
        s = txid_stats[txid]
        if s['groups']:
            continue
        if s['commits'] + s['aborts'] < 2:
            continue  # skip trivial
        txid_short = txid[:16]
        print(f"  {txid_short}  pid={s['pid']}  "
              f"commits={s['commits']}  aborts={s['aborts']}  "
              f"batches_committed={s['total_batches_committed']}  "
              f"span={s['first_ts']}-{s['last_ts']}")


def main_txn_produce(args):
    """Trace produce requests per PID per epoch from server log.

    Groups consecutive server-side produce batches into requests,
    shows record counts, and compares across epochs to find anomalies.
    """
    server_file = args.server
    filter_pid = args.pid

    # Patterns
    p_produce = re.compile(
        r'(\d+:\d+:\d+\.\d+) \[DBG\] produce: txnal batch pid (\d+) epoch (\d+) '
        r'txid "([^"]*)" \S+\[(\d+)\] offset (\d+) records (\d+)')
    p_invalid = re.compile(
        r'(\d+:\d+:\d+\.\d+) \[WRN\] produce: INVALID_PRODUCER_EPOCH pid=(\d+) '
        r'req_epoch=(\d+) server_epoch=(\d+) inTx=(\w+) topic=\S+ partition=(\d+)')
    p_endtxn = re.compile(
        r'(\d+:\d+:\d+\.\d+) \[DBG\] txn: EndTxn received pid (\d+) epoch (\d+)')
    p_endtxn_done = re.compile(
        r'(\d+:\d+:\d+\.\d+) \[DBG\] txn: pid (\d+) epoch (\d+) .* ending transaction \((\w+)\), batches=(\d+)')
    p_started = re.compile(
        r'(\d+:\d+:\d+\.\d+) \[DBG\] txn: pid (\d+) epoch (\d+) .* started transaction')

    # Per-PID data: {pid: {epoch: [{ts, partitions, records, type}]}}
    pid_epochs = defaultdict(lambda: defaultdict(list))
    # Track requests: group consecutive produce batches into one request
    current_req = None  # {pid, epoch, ts, partitions: [(p, offset, records)]}

    def flush_req():
        nonlocal current_req
        if current_req:
            pid = current_req['pid']
            epoch = current_req['epoch']
            total = sum(r for _, _, r in current_req['partitions'])
            pid_epochs[pid][epoch].append({
                'ts': current_req['ts'],
                'type': 'produce',
                'partitions': current_req['partitions'],
                'total_records': total,
            })
            current_req = None

    with open(server_file) as f:
        for line in f:
            m = p_produce.match(line)
            if m:
                ts, pid, epoch, txid, part, offset, records = m.groups()
                pid = int(pid)
                if filter_pid and pid != filter_pid:
                    flush_req()
                    continue
                epoch = int(epoch)
                part = int(part)
                offset = int(offset)
                records = int(records)
                if current_req and (current_req['pid'] != pid or current_req['epoch'] != epoch):
                    flush_req()
                if not current_req:
                    current_req = {'pid': pid, 'epoch': epoch, 'ts': ts,
                                   'partitions': [], 'txid': txid}
                current_req['partitions'].append((part, offset, records))
                continue

            # Non-produce line breaks the current request
            flush_req()

            m = p_invalid.match(line)
            if m:
                ts, pid, req_epoch, srv_epoch, in_tx, part = m.groups()
                pid = int(pid)
                if filter_pid and pid != filter_pid:
                    continue
                req_epoch = int(req_epoch)
                # Group consecutive INVALID_PRODUCER_EPOCH into one entry
                entries = pid_epochs[pid][req_epoch]
                if entries and entries[-1]['type'] == 'INVALID_EPOCH':
                    entries[-1]['partitions'].append(int(part))
                else:
                    pid_epochs[pid][req_epoch].append({
                        'ts': ts,
                        'type': 'INVALID_EPOCH',
                        'partitions': [int(part)],
                        'server_epoch': int(srv_epoch),
                    })
                continue

            m = p_endtxn.match(line)
            if m:
                ts, pid, epoch = m.groups()
                pid = int(pid)
                if filter_pid and pid != filter_pid:
                    continue
                pid_epochs[pid][int(epoch)].append({
                    'ts': ts, 'type': 'EndTxn-received',
                })
                continue

            m = p_endtxn_done.match(line)
            if m:
                ts, pid, epoch, action, batches = m.groups()
                pid = int(pid)
                if filter_pid and pid != filter_pid:
                    continue
                pid_epochs[pid][int(epoch)].append({
                    'ts': ts, 'type': f'EndTxn-{action}',
                    'batches': int(batches),
                })
                continue

            m = p_started.match(line)
            if m:
                ts, pid, epoch = m.groups()
                pid = int(pid)
                if filter_pid and pid != filter_pid:
                    continue
                pid_epochs[pid][int(epoch)].append({
                    'ts': ts, 'type': 'started-txn',
                })

    flush_req()

    # Print results
    for pid in sorted(pid_epochs):
        epochs = pid_epochs[pid]
        print(f"\n{'='*70}")
        print(f"PID {pid}")
        print(f"{'='*70}")
        for epoch in sorted(epochs):
            entries = epochs[epoch]
            total_produced = 0
            total_rejected = 0
            n_requests = 0
            for e in entries:
                if e['type'] == 'produce':
                    total_produced += e['total_records']
                    n_requests += 1
            print(f"\n  Epoch {epoch}:")
            for e in entries:
                if e['type'] == 'produce':
                    parts = e['partitions']
                    part_nums = sorted(p for p, _, _ in parts)
                    total = e['total_records']
                    print(f"    {e['ts']} PRODUCE {len(parts)} batches, "
                          f"{total} records, partitions: {part_nums}")
                elif e['type'] == 'INVALID_EPOCH':
                    parts = sorted(e['partitions'])
                    total_rejected = len(parts)
                    print(f"    {e['ts']} INVALID_PRODUCER_EPOCH "
                          f"{len(parts)} partitions (server_epoch={e['server_epoch']}): {parts}")
                elif e['type'] == 'started-txn':
                    print(f"    {e['ts']} started transaction")
                elif e['type'] == 'EndTxn-received':
                    print(f"    {e['ts']} EndTxn received")
                elif e['type'].startswith('EndTxn-'):
                    print(f"    {e['ts']} EndTxn {e['type'][7:]}, "
                          f"batches={e.get('batches', '?')}")
            if n_requests > 0:
                print(f"    -- Summary: {n_requests} produce request(s), "
                      f"{total_produced} records committed")
            if total_rejected:
                print(f"    -- REJECTED: {total_rejected} partition-batches")


def main_lso_state(args):
    """Show LSO vs HWM state per topic and partition from server logs.

    Parses 'txn: commit/abort TOPIC[PART] LSO X -> Y, HWM Z' lines and
    'produce: txnal batch ... TOPIC[PART] offset N records M' lines to
    build a picture of which topics have LSO < HWM (stuck behind open
    transactions).
    """
    with open(args.server) as f:
        server_lines = f.readlines()

    # topic => partition => {lso, hwm, last_ts}
    lso_state = defaultdict(lambda: defaultdict(lambda: {'lso': 0, 'hwm': 0, 'last_ts': ''}))
    # Also track HWM from produce lines
    hwm_state = defaultdict(lambda: defaultdict(int))

    lso_pat = re.compile(
        r'(\d{2}:\d{2}:\d{2}\.\d+).*txn: (?:commit|abort) (\S+)\[(\d+)\] LSO (\d+) -> (\d+), HWM (\d+)'
    )
    # txnal batch produce: "produce: txnal batch pid ... txid ... TOPIC[PART] offset N records M"
    produce_pat = re.compile(
        r'(\d{2}:\d{2}:\d{2}\.\d+).*produce: txnal batch.*?(\S{16,})\[(\d+)\] offset (\d+) records (\d+)'
    )

    for line in server_lines:
        m = lso_pat.search(line)
        if m:
            ts, topic, part, _lso_old, lso_new, hwm = m.group(1), m.group(2), int(m.group(3)), int(m.group(4)), int(m.group(5)), int(m.group(6))
            s = lso_state[topic][part]
            s['lso'] = max(s['lso'], lso_new)
            s['hwm'] = max(s['hwm'], hwm)
            s['last_ts'] = ts
            continue
        m = produce_pat.search(line)
        if m:
            ts, topic, part, offset, nrecs = m.group(1), m.group(2), int(m.group(3)), int(m.group(4)), int(m.group(5))
            end = offset + nrecs
            hwm_state[topic][part] = max(hwm_state[topic][part], end)

    # Merge HWM from produce lines into lso_state
    for topic, parts in hwm_state.items():
        for part, hwm in parts.items():
            s = lso_state[topic][part]
            s['hwm'] = max(s['hwm'], hwm)

    if not lso_state:
        print("No LSO data found in server log.")
        print("Ensure server was run with --server-log debug.")
        return

    topic_filter = args.topic if args.topic else None

    print("=== LSO STATE PER TOPIC ===")
    for topic in sorted(lso_state.keys()):
        if topic_filter and topic_filter not in topic:
            continue
        parts = lso_state[topic]
        total_lso = sum(s['lso'] for s in parts.values())
        total_hwm = sum(s['hwm'] for s in parts.values())
        gap = total_hwm - total_lso
        stuck = " ** STUCK **" if gap > 0 else ""
        print(f"\n  Topic: {topic[:40]}...")
        print(f"    Partitions: {len(parts)}, Total LSO: {total_lso}, Total HWM: {total_hwm}, Gap: {gap}{stuck}")
        if gap > 0 or args.verbose:
            for part in sorted(parts.keys()):
                s = parts[part]
                pgap = s['hwm'] - s['lso']
                if pgap > 0 or args.verbose:
                    print(f"    p={part:>3}  LSO={s['lso']:>8}  HWM={s['hwm']:>8}  gap={pgap:>6}  last={s['last_ts']}")

    print()
    print("=== SUMMARY ===")
    stuck_topics = []
    for topic in sorted(lso_state.keys()):
        if topic_filter and topic_filter not in topic:
            continue
        parts = lso_state[topic]
        total_lso = sum(s['lso'] for s in parts.values())
        total_hwm = sum(s['hwm'] for s in parts.values())
        if total_hwm > total_lso:
            stuck_topics.append((topic, total_lso, total_hwm))
    if stuck_topics:
        print(f"  {len(stuck_topics)} topic(s) with LSO < HWM (data stuck behind open transactions):")
        for topic, lso, hwm in stuck_topics:
            print(f"    {topic[:40]}... LSO={lso} HWM={hwm} gap={hwm-lso}")
    else:
        print("  All topics have LSO == HWM (no stuck transactions).")


def main_commit_timeline(args):
    """Show commit rate timeline and fetch watch timeline for specified groups.

    Reads the server log and shows:
    - OffsetCommit timeline (1-second buckets) with running total
    - Fetch watch create/fire/expire timeline per topic
    - Gaps in commits > 2 seconds
    """
    with open(args.server) as f:
        server_lines = f.readlines()

    groups = [g.strip() for g in args.groups.split(',')]

    for group in groups:
        print(f"=== GROUP {group} ===")
        commits = []  # (timestamp_str, partition, offset)
        for line in server_lines:
            if f'OffsetCommit: group={group}' in line:
                m = re.match(r'(\d+:\d+:\d+\.\d+)', line)
                pm = re.search(r' p=(\d+) ', line)
                om = re.search(r'offset=(\d+)', line)
                if m and pm and om:
                    commits.append((m.group(1), int(pm.group(1)), int(om.group(1))))

        if not commits:
            print(f"  No commits found.")
            print()
            continue

        # Compute final offsets per partition
        final = {}
        for ts, p, o in commits:
            final[p] = o
        total = sum(final.values())
        print(f"  Total commits: {len(commits)}")
        print(f"  Final offsets: {dict(sorted(final.items()))}")
        print(f"  Final total: {total}")

        # Bucket by 1-second intervals and show running total
        running = {}
        running_total = 0
        prev_ts_sec = None
        print(f"\n  Timeline (1s buckets):")
        buckets = []
        for ts, p, o in commits:
            sec = int(parse_ts(ts))
            old = running.get(p, 0)
            if o > old:
                running_total += (o - old)
                running[p] = o
            if sec != prev_ts_sec:
                if prev_ts_sec is not None:
                    buckets.append((prev_ts_sec, running_total))
                prev_ts_sec = sec
        if prev_ts_sec is not None:
            buckets.append((prev_ts_sec, running_total))

        # Show timeline with gap detection
        prev_sec = None
        for sec, total in buckets:
            ts_str = f"{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"
            gap = ""
            if prev_sec is not None and sec - prev_sec > 2:
                gap = f"  *** GAP {sec - prev_sec}s ***"
            print(f"    {ts_str}  total={total}{gap}")
            prev_sec = sec
        print()

    # Also show fetch watch timeline for topics mentioned in commits
    if args.topics:
        topics = [t.strip() for t in args.topics.split(',')]

        # First pass: collect ALL watch events (creates have topics,
        # fire/cleanup do NOT). We'll show creates per topic, and
        # global fire/cleanup totals for context.
        print("=== FETCH WATCH ANALYSIS ===")
        for topic in topics:
            creates = []
            for line in server_lines:
                if 'watch created' in line and topic in line:
                    m = re.match(r'(\d+:\d+:\d+\.\d+)', line)
                    if m:
                        parts_m = re.search(r'\(([^)]*)\)', line)
                        rc = 'readCommitted=true' in line
                        creates.append((m.group(1), parts_m.group(1) if parts_m else '', rc))
            print(f"\n  Topic {topic}: {len(creates)} watch creates")
            if creates:
                print(f"    First: {creates[0][0]}  Last: {creates[-1][0]}")
                # Per-partition breakdown
                pcounts = defaultdict(int)
                for _, parts, _ in creates:
                    for pm in re.finditer(r'\[(\d+)\]', parts):
                        pcounts[int(pm.group(1))] += 1
                print(f"    Per-partition: {dict(sorted(pcounts.items()))}")
                rc_count = sum(1 for _, _, rc in creates if rc)
                print(f"    readCommitted: {rc_count}/{len(creates)}")
                # Show time distribution
                sec_buckets = defaultdict(int)
                for ts, _, _ in creates:
                    sec = int(parse_ts(ts))
                    sec_buckets[sec] += 1
                # Show 5s summary
                fives = defaultdict(int)
                for s, c in sec_buckets.items():
                    fives[s // 5 * 5] += c
                print(f"    5s buckets: ", end="")
                for s in sorted(fives)[:20]:
                    ts_str = f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"
                    print(f"{ts_str}={fives[s]} ", end="")
                if len(fives) > 20:
                    print(f"... +{len(fives)-20} more", end="")
                print()

        # Global fire/cleanup stats (can't be attributed to specific topics)
        global_fires = defaultdict(int)
        global_empty = defaultdict(int)
        global_response_bytes = defaultdict(int)
        for line in server_lines:
            m = re.match(r'(\d+:\d+:\d+\.\d+)', line)
            if not m:
                continue
            sec = int(parse_ts(m.group(1))) // 5 * 5
            if 'watch cleanup' in line:
                if 'fired=true' in line:
                    global_fires[sec] += 1
                else:
                    global_empty[sec] += 1
            elif 'watch response' in line:
                bm = re.search(r'batches=(\d+)', line)
                if bm:
                    global_response_bytes[sec] += int(bm.group(1))

        print(f"\n  Global watch stats (all topics combined):")
        all_secs = sorted(set(list(global_fires.keys()) + list(global_empty.keys())))
        if all_secs:
            print(f"    5s buckets: fires / empty-expires / response-batches")
            for sec in all_secs[:40]:
                ts_str = f"{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"
                f, e, b = global_fires[sec], global_empty[sec], global_response_bytes[sec]
                print(f"      {ts_str}  fires={f:4d}  empty={e:3d}  batches={b:4d}")
            if len(all_secs) > 40:
                print(f"      ... +{len(all_secs)-40} more time buckets")
        print()


def main_848_state(args):
    """Compare 848 group lifecycle with convergence analysis.

    Parses kfake server logs and kgo client logs to show:
    - Member lifecycle: join, leave, fence, session/rebalance timeout
    - Target assignment changes: partition counts per member after each recompute
    - Reconciliation progress: reconciled vs target per member, blocked partitions
    - Convergence timeline: number of converged members over time
    - Epoch map size over time
    """
    with open(args.client) as f:
        client_lines = f.readlines()
    with open(args.server) as f:
        server_lines = f.readlines()

    groups = [g.strip() for g in args.groups.split(',')]
    after_ts = parse_ts(args.after) if args.after else 0

    # Auto-discover 848 groups when "all" is requested.
    if groups == ['all']:
        seen = set()
        p_grp = re.compile(r'consumerJoin: group=(\S+)')
        for line in server_lines:
            m = p_grp.search(line)
            if m:
                seen.add(m.group(1))
        groups = sorted(seen)
        if not groups:
            print("No 848 groups found in server log.")
            return
        print(f"Discovered {len(groups)} group(s): {', '.join(groups)}")
        print()

    for group in groups:
        print(f"=== GROUP {group} ===")

        # Find member IDs from both client and server logs.
        members = set()
        for line in client_lines:
            if f'group: {group}' not in line and f'group={group}' not in line:
                continue
            mm = re.search(r'member_id: (\S+)', line)
            if mm:
                members.add(mm.group(1).rstrip(','))
        for line in server_lines:
            if f'group={group}' not in line:
                continue
            mm = re.search(r'member=(\S+)', line)
            if mm:
                members.add(mm.group(1))
        print(f"  Members ({len(members)}):")
        for mid in sorted(members):
            print(f"    {mid}")

        # --- Server-side structured event timeline ---
        # Parse the structured logs from groups.go into a unified timeline.
        print(f"\n  Server timeline:")
        event_count = 0

        # Patterns for new structured logs
        p_join = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* consumerJoin: group=' + re.escape(group)
            + r'\S*\s+member=(\S+)\s+epoch=(\d+)\s+sent=(\d+)\s+epochMap=(\d+)\s+members=(\d+)')
        p_leave = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* consumerLeave: group=' + re.escape(group)
            + r'\S*\s+member=(\S+)\s+epoch=(\d+)\s+sent=(\d+)\s+pending=(\d+)\s+remaining=(\d+)')
        p_static_leave = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* consumerStaticLeave: group=' + re.escape(group)
            + r'\S*\s+member=(\S+)\s+instance=(\S+)\s+epoch=(\d+)\s+sent=(\d+)\s+remaining=(\d+)')
        p_session_timeout = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* consumerSessionTimeout: group=' + re.escape(group)
            + r'\S*\s+member=(\S+)\s+epoch=(\d+)\s+remaining=(\d+)')
        p_rebalance_timeout = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* consumerRebalanceTimeout: group=' + re.escape(group)
            + r'\S*\s+member=(\S+)\s+epoch=(\d+)\s+remaining=(\d+)')
        p_target = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* computeTarget: group=' + re.escape(group)
            + r'\S*\s+member=(\S+)\s+target=(\d+)\s+current=(\d+)\s+pending=(\d+)\s+memberEpoch=(\d+)\s+gen=(\d+)')
        p_fence = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* fenceConsumer: group=' + re.escape(group)
            + r'\S*\s+member=(\S+)\s+freed=(\d+)\s+\(sent=(\d+)\s+pending=(\d+)\)\s+epochMap=(\d+)')
        p_reconcile_send = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* RECONCILE SEND: member=(\S+)\s+epoch=(\d+)\s+partitions=(\d+)')
        p_reconcile_summary = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* RECONCILE SUMMARY: member=(\S+)\s+reconciled=(\d+)\s+target=(\d+)\s+allowed=(\d+)\s+blocked=(\d+)')
        p_reconcile_revoke = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* RECONCILE REVOKE-FIRST: member=(\S+)\s+reconciled=(\d+)\s+sent=(\d+)\s+target=(\d+)\s+pending=(\d+)')
        p_pending_cleared = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* pendingRevoke cleared: member=(\S+)\s+epoch=(\d+)\s+cleared=(\d+)\s+kept=(\d+)')
        p_sent_detail = re.compile(
            r'(\d+:\d+:\d+\.\d+) .*  SENT: (\S+) \[([^\]]*)\]')
        p_target_detail = re.compile(
            r'(\d+:\d+:\d+\.\d+) .*  TARGET: (\S+) \[([^\]]*)\]')
        p_hb_late = re.compile(
            r'(\d+:\d+:\d+\.\d+) .* consumerHeartbeatLate: group=' + re.escape(group)
            + r'\S*\s+member=(\S+)\s+gap=(\d+)ms\s+interval=(\d+)ms')

        # Collect all events for this group.
        events = []  # (ts_float, ts_str, event_type, details_str)
        last_revoke_member = None  # track REVOKE-FIRST owner for SENT/TARGET detail lines

        for line in server_lines:
            m = p_join.search(line)
            if m:
                ts, mid, epoch, sent, emap, nmem = m.groups()
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'JOIN',
                    f'member={mid} epoch={epoch} sent={sent} epochMap={emap} members={nmem}'))
                continue

            m = p_leave.search(line)
            if m:
                ts, mid, epoch, sent, pend, remaining = m.groups()
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'LEAVE',
                    f'member={mid} epoch={epoch} sent={sent} pending={pend} remaining={remaining}'))
                continue

            m = p_static_leave.search(line)
            if m:
                ts, mid, inst, epoch, sent, remaining = m.groups()
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'STATIC-LEAVE',
                    f'member={mid} instance={inst} epoch={epoch} sent={sent} remaining={remaining}'))
                continue

            m = p_session_timeout.search(line)
            if m:
                ts, mid, epoch, remaining = m.groups()
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'SESSION-TIMEOUT',
                    f'member={mid} epoch={epoch} remaining={remaining}'))
                continue

            m = p_rebalance_timeout.search(line)
            if m:
                ts, mid, epoch, remaining = m.groups()
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'REBAL-TIMEOUT',
                    f'member={mid} epoch={epoch} remaining={remaining}'))
                continue

            m = p_hb_late.search(line)
            if m:
                ts, mid, gap, interval = m.groups()
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'HB-LATE',
                    f'member={mid} gap={gap}ms interval={interval}ms'))
                continue

            m = p_fence.search(line)
            if m:
                ts, mid, freed, sent, pend, emap = m.groups()
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'FENCE',
                    f'member={mid} freed={freed} (sent={sent} pending={pend}) epochMap={emap}'))
                continue

            m = p_pending_cleared.search(line)
            if m:
                ts, mid, epoch, cleared, kept = m.groups()
                if mid not in members:
                    continue
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'REVOKE-CONFIRMED',
                    f'member={mid} epoch={epoch} cleared={cleared} kept={kept}'))
                continue

            # computeTarget lines - only show if member is in this group.
            m = p_target.search(line)
            if m:
                ts, mid, target, current, pending, mepoch, gen = m.groups()
                if mid not in members:
                    continue
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'TARGET',
                    f'member={mid} target={target} current={current} pending={pending} epoch={mepoch} gen={gen}'))
                continue

            # RECONCILE events - only for this group's members.
            m = p_reconcile_summary.search(line)
            if m:
                ts, mid, reconciled, target, allowed, blocked = m.groups()
                if mid not in members:
                    continue
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'RECONCILE',
                    f'member={mid} reconciled={reconciled} target={target} +{allowed} blocked={blocked}'))
                continue

            m = p_reconcile_revoke.search(line)
            if m:
                ts, mid, reconciled, sent, target, pending = m.groups()
                if mid not in members:
                    last_revoke_member = None
                    continue
                last_revoke_member = mid
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'REVOKE-FIRST',
                    f'member={mid} reconciled={reconciled} sent={sent} target={target} pending={pending}'))
                continue

            m = p_reconcile_send.search(line)
            if m:
                ts, mid, epoch, parts = m.groups()
                if mid not in members:
                    continue
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'SEND',
                    f'member={mid} epoch={epoch} partitions={parts}'))
                continue

            m = p_sent_detail.search(line)
            if m:
                if last_revoke_member is None:
                    continue
                ts, topic_id, parts = m.groups()
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'SENT-DETAIL',
                    f'{topic_id} [{parts}]'))
                continue

            m = p_target_detail.search(line)
            if m:
                if last_revoke_member is None:
                    continue
                ts, topic_id, parts = m.groups()
                t = parse_ts(ts)
                if after_ts and t < after_ts:
                    continue
                events.append((t, ts, 'TARGET-DETAIL',
                    f'{topic_id} [{parts}]'))
                continue

        events.sort(key=lambda e: e[0])

        # Print the timeline (compact: group consecutive TARGET lines).
        i = 0
        while i < len(events) and event_count < 200:
            t, ts, etype, details = events[i]
            if etype == 'TARGET':
                # Group consecutive TARGET events into one block.
                target_lines = [(ts, details)]
                j = i + 1
                while j < len(events) and events[j][2] == 'TARGET':
                    target_lines.append((events[j][1], events[j][3]))
                    j += 1
                # Print as compact table.
                print(f"    {ts} TARGET ({len(target_lines)} members):")
                for tts, td in target_lines:
                    print(f"      {td}")
                event_count += 1
                i = j
            elif etype in ('SENT-DETAIL', 'TARGET-DETAIL'):
                # Indent detail lines under their parent REVOKE-FIRST.
                print(f"                               {etype}: {details}")
                i += 1
                continue
            elif etype == 'ASSIGN-KEEP':
                # Group consecutive ASSIGN-KEEP lines.
                keep_lines = [(ts, details)]
                j = i + 1
                while j < len(events) and events[j][2] == 'ASSIGN-KEEP':
                    keep_lines.append((events[j][1], events[j][3]))
                    j += 1
                print(f"    {ts} ASSIGN-KEEP ({len(keep_lines)} members):")
                for _, kd in keep_lines:
                    print(f"      {kd}")
                event_count += 1
                i = j
                continue
            else:
                print(f"    {ts} {etype:<18} {details}")
                event_count += 1
                i += 1
        if event_count >= 200:
            print(f"    ... truncated at 200 events ({len(events)} total)")

        # --- Client-side events timeline ---
        print(f"\n  Client timeline:")
        client_count = 0
        for line in client_lines:
            if group not in line:
                continue
            m = re.match(r'\[(\d+:\d+:\d+\.\d+)', line)
            if not m:
                continue
            ts = m.group(1)
            if after_ts and parse_ts(ts) < after_ts:
                continue
            if 'now_assigned' in line:
                am = re.search(r'now_assigned: (map\[[^\]]*\])', line)
                assigned = am.group(1) if am else '?'
                # Count total partitions assigned.
                part_count = len(re.findall(r'\d+', am.group(1))) if am else 0
                mid = re.search(r'member_id: (\S+)', line)
                member = mid.group(1)[:20] if mid else '?'
                print(f"    {ts} ASSIGN member={member} parts~{part_count} {assigned[:80]}")
                client_count += 1
            elif 'new group session' in line:
                am = re.search(r'added: ([^,]*), lost: (.*)', line)
                if am:
                    added = am.group(1).strip()
                    lost = am.group(2).strip()
                    # Count partitions in added/lost.
                    add_count = len(re.findall(r'\d+', added)) if added else 0
                    lost_count = len(re.findall(r'\d+', lost)) if lost else 0
                    print(f"    {ts} SESSION +{add_count} -{lost_count}")
                    client_count += 1
            elif 'heartbeat errored' in line:
                em = re.search(r'err: (.+)', line)
                err = em.group(1) if em else '?'
                print(f"    {ts} HB-ERROR: {err[:80]}")
                client_count += 1
            elif 'leaving next-gen group' in line:
                mid = re.search(r'member_id: (\S+)', line)
                member = mid.group(1)[:20] if mid else '?'
                ep = re.search(r'member_epoch: (\S+)', line)
                epoch = ep.group(1) if ep else '?'
                print(f"    {ts} LEAVE member={member} epoch={epoch}")
                client_count += 1
            if client_count > 80:
                print(f"    ... truncated (>80 events)")
                break

        # --- Convergence summary ---
        # Count joins, leaves, fences, total target recomputes,
        # and estimate convergence window.
        n_joins = sum(1 for e in events if e[2] == 'JOIN')
        n_leaves = sum(1 for e in events if e[2] in ('LEAVE', 'STATIC-LEAVE'))
        n_fences = sum(1 for e in events if e[2] == 'FENCE')
        n_timeouts = sum(1 for e in events if e[2] in ('SESSION-TIMEOUT', 'REBAL-TIMEOUT'))
        n_targets = sum(1 for e in events if e[2] == 'TARGET')
        n_revoke_first = sum(1 for e in events if e[2] == 'REVOKE-FIRST')
        n_blocked = 0
        for e in events:
            if e[2] == 'RECONCILE':
                bm = re.search(r'blocked=(\d+)', e[3])
                if bm:
                    n_blocked += int(bm.group(1))

        # Detect generation spiral: generation incrementing with same
        # target (needRecompute firing when nothing changed).
        target_events = [e for e in events if e[2] == 'TARGET']
        gen_spiral_count = 0
        prev_target_block = None
        for e in target_events:
            # Extract per-member target assignments from the detail lines.
            target_block = e[3]
            if prev_target_block is not None:
                # Compare target partitions (strip epoch/gen which change).
                def strip_epoch(s):
                    return re.sub(r'epoch=\d+ gen=\d+', '', s)
                if strip_epoch(target_block) == strip_epoch(prev_target_block):
                    gen_spiral_count += 1
            prev_target_block = target_block

        print(f"\n  Summary:")
        print(f"    Joins: {n_joins}")
        print(f"    Leaves: {n_leaves}")
        print(f"    Fences: {n_fences}")
        print(f"    Session/rebalance timeouts: {n_timeouts}")
        print(f"    Target recomputes: {n_targets // max(len(members), 1)} (as {n_targets} member-level entries)")
        print(f"    Revoke-first heartbeats: {n_revoke_first}")
        print(f"    Total blocked partition adds: {n_blocked}")
        if gen_spiral_count > 0:
            print(f"    WARNING: generation spiral detected - {gen_spiral_count} consecutive target recomputes with identical assignments")
            print(f"             Server is bumping generation without actual subscription changes (regex needRecompute bug?)")

        # Find the last event where all members are converged.
        # Look for the last TARGET block where all members have
        # target == current and epoch == gen.
        last_stable_ts = None
        for e in reversed(events):
            if e[2] == 'TARGET':
                tm = re.search(r'target=(\d+).*current=(\d+).*epoch=(\d+).*gen=(\d+)', e[3])
                if tm and tm.group(1) == tm.group(2) and tm.group(3) == tm.group(4):
                    last_stable_ts = e[1]
                else:
                    break
        if last_stable_ts:
            print(f"    Last stable (all converged): {last_stable_ts}")
        else:
            print(f"    Never fully converged in the log window")

        print()



def main_848_convergence(args):
    """Analyze 848 group revocation timing and convergence bottlenecks.

    For each 848 group, traces the complete convergence lifecycle:
    1. First member joins and gets all partitions
    2. Subsequent members join, triggering revocation from first member
    3. Client-side prerevoking phase (keepalive heartbeats block server progress)
    4. Revocation confirmed, blocked members finally get partitions
    5. All members converged and consuming

    Identifies the common bottleneck: the first member's cooperative revocation
    takes multiple heartbeat intervals (prerevoking sends keepalives, server
    conservatively keeps pending revocations alive, all other members are blocked).

    Output: per-group convergence timeline with timing annotations.
    """
    with open(args.client) as f:
        client_lines = f.readlines()
    with open(args.server) as f:
        server_lines = f.readlines()

    # Auto-discover 848 groups.
    if args.groups:
        group_prefixes = [g.strip() for g in args.groups.split(',')]
    else:
        group_prefixes = []

    # Find all 848 groups from consumerJoin lines.
    p_join = re.compile(r'(\d+:\d+:\d+\.\d+) .* consumerJoin: group=(\S+)\s+member=(\S+)\s+epoch=(\d+)\s+sent=(\d+)\s+epochMap=(\d+)\s+members=(\d+)')
    p_leave = re.compile(r'(\d+:\d+:\d+\.\d+) .* consumer(?:Static)?Leave: group=(\S+)\s+member=(\S+)')
    p_target = re.compile(r'(\d+:\d+:\d+\.\d+) .* computeTarget: group=(\S+)\s+member=(\S+)\s+target=(\d+)\s+current=(\d+)\s+pending=(\d+)\s+memberEpoch=(\d+)\s+gen=(\d+)')
    p_revoke_first = re.compile(r'(\d+:\d+:\d+\.\d+) .* RECONCILE REVOKE-FIRST: member=(\S+)\s+reconciled=(\d+)\s+sent=(\d+)\s+target=(\d+)\s+pending=(\d+)')
    p_pending_cleared = re.compile(r'(\d+:\d+:\d+\.\d+) .* pendingRevoke cleared: member=(\S+)\s+epoch=(\d+)\s+cleared=(\d+)\s+kept=(\d+)')
    p_send = re.compile(r'(\d+:\d+:\d+\.\d+) .* RECONCILE SEND: member=(\S+)\s+epoch=(\d+)\s+partitions=(\d+)')
    p_blocked = re.compile(r'(\d+:\d+:\d+\.\d+) .* RECONCILE SUMMARY: member=(\S+)\s+reconciled=(\d+)\s+target=(\d+)\s+allowed=(\d+)\s+blocked=(\d+)')
    p_skip = re.compile(r'(\d+:\d+:\d+\.\d+) .* RECONCILE SKIP-UNREVOKED: member=(\S+)\s+epoch=(\d+)\s+pending=(\d+)')
    p_topics_changed = re.compile(r'(\d+:\d+:\d+\.\d+) .* TOPICS CHANGED: member=(\S+)\s+epoch=(\d+)\s+old=(\d+)\s+new=(\d+)')

    # Client-side patterns.
    p_client_prerevoke_start = re.compile(r'\[(\d+:\d+:\d+\.\d+) (\d+)\].*prerevoke: starting cooperative revoke.*group: (\S+),.*lostPartitions: (\d+)')
    p_client_prerevoke_done = re.compile(r'\[(\d+:\d+:\d+\.\d+) (\d+)\].*prerevoke: cooperative revoke complete.*group: (\S+),.*lostPartitions: (\d+)')
    p_client_keepalive = re.compile(r'\[(\d+:\d+:\d+\.\d+) (\d+)\].*848 heartbeat: sending keepalive.*group: (\S+),.*prerevoking: (\w+)')
    p_client_full = re.compile(r'\[(\d+:\d+:\d+\.\d+) (\d+)\].*848 heartbeat: sending full Topics.*group: (\S+),.*topicPartitions: (\d+)')

    # Build per-group data.
    all_groups = {}  # group_id => { members, first_join_ts, ... }

    # Scan server log for group membership.
    for line in server_lines:
        m = p_join.search(line)
        if m:
            ts, gid, mid, epoch, sent, emap, nmem = m.groups()
            if group_prefixes and not any(gid.startswith(p) for p in group_prefixes):
                continue
            if gid not in all_groups:
                all_groups[gid] = {
                    'members': set(),
                    'first_join_ts': ts,
                    'first_join_t': parse_ts(ts),
                    'first_member': mid,
                    'first_member_sent': int(sent),
                    'revoke_events': [],  # (ts, member, reconciled, sent, target)
                    'pending_cleared': [],  # (ts, member, cleared, kept)
                    'blocked_events': [],  # (ts, member, blocked)
                    'skip_events': [],  # (ts, member, pending)
                    'topics_changed': [],  # (ts, member, old, new)
                    'sends': [],  # (ts, member, epoch, partitions)
                    'all_members_assigned_ts': None,
                    'all_members_assigned_t': None,
                    'member_first_real_send': {},  # member => ts
                    'leaves': [],  # (ts, member)
                    'client_prerevoke_start': [],  # (ts, group, lost)
                    'client_prerevoke_done': [],  # (ts, group, lost)
                    'client_keepalives': [],  # (ts, group, prerevoking)
                    'client_full_topics': [],  # (ts, group, count)
                    'max_members': 0,
                }
            g = all_groups[gid]
            g['members'].add(mid)
            g['max_members'] = max(g['max_members'], int(nmem))
            continue

    if not all_groups:
        print('No 848 groups found in server log.')
        return

    # Map member => group for member-only log lines.
    member_to_group = {}
    for gid, g in all_groups.items():
        for mid in g['members']:
            member_to_group[mid] = gid

    # Second pass: collect events.
    for line in server_lines:
        m = p_revoke_first.search(line)
        if m:
            ts, mid, reconciled, sent, target, pending = m.groups()
            gid = member_to_group.get(mid)
            if gid:
                all_groups[gid]['revoke_events'].append((ts, mid, int(reconciled), int(sent), int(target)))
            continue

        m = p_pending_cleared.search(line)
        if m:
            ts, mid, epoch, cleared, kept = m.groups()
            gid = member_to_group.get(mid)
            if gid:
                all_groups[gid]['pending_cleared'].append((ts, mid, int(cleared), int(kept)))
            continue

        m = p_blocked.search(line)
        if m:
            ts, mid, reconciled, target, allowed, blocked = m.groups()
            gid = member_to_group.get(mid)
            if gid and int(blocked) > 0:
                all_groups[gid]['blocked_events'].append((ts, mid, int(blocked)))
            continue

        m = p_skip.search(line)
        if m:
            ts, mid, epoch, pending = m.groups()
            gid = member_to_group.get(mid)
            if gid:
                all_groups[gid]['skip_events'].append((ts, mid, int(pending)))
            continue

        m = p_topics_changed.search(line)
        if m:
            ts, mid, epoch, old, new = m.groups()
            gid = member_to_group.get(mid)
            if gid:
                all_groups[gid]['topics_changed'].append((ts, mid, int(old), int(new)))
            continue

        m = p_send.search(line)
        if m:
            ts, mid, epoch, parts = m.groups()
            gid = member_to_group.get(mid)
            if gid and int(parts) > 0:
                g = all_groups[gid]
                g['sends'].append((ts, mid, int(epoch), int(parts)))
                if mid not in g['member_first_real_send']:
                    g['member_first_real_send'][mid] = ts
            continue

        m = p_leave.search(line)
        if m:
            ts, gid_found, mid = m.groups()
            if gid_found in all_groups:
                all_groups[gid_found]['leaves'].append((ts, mid))
            continue

    # Third pass: client-side events.
    for line in client_lines:
        m = p_client_prerevoke_start.search(line)
        if m:
            ts, cid, grp, lost = m.groups()
            for gid in all_groups:
                if gid in grp:
                    all_groups[gid]['client_prerevoke_start'].append((ts, cid, int(lost)))
                    break
            continue

        m = p_client_prerevoke_done.search(line)
        if m:
            ts, cid, grp, lost = m.groups()
            for gid in all_groups:
                if gid in grp:
                    all_groups[gid]['client_prerevoke_done'].append((ts, cid, int(lost)))
                    break
            continue

        m = p_client_keepalive.search(line)
        if m:
            ts, cid, grp, prerevoking = m.groups()
            for gid in all_groups:
                if gid in grp:
                    all_groups[gid]['client_keepalives'].append((ts, cid, prerevoking))
                    break
            continue

        m = p_client_full.search(line)
        if m:
            ts, cid, grp, count = m.groups()
            for gid in all_groups:
                if gid in grp:
                    all_groups[gid]['client_full_topics'].append((ts, cid, int(count)))
                    break
            continue

    # Compute convergence timing per group.
    for gid, g in all_groups.items():
        if g['member_first_real_send']:
            last_ts = max(g['member_first_real_send'].values(), key=parse_ts)
            g['all_members_assigned_ts'] = last_ts
            g['all_members_assigned_t'] = parse_ts(last_ts)

    # Print analysis.
    print('=== 848 CONVERGENCE ANALYSIS ===')
    print()

    # Sort groups by first join time.
    sorted_groups = sorted(all_groups.items(), key=lambda x: x[1]['first_join_t'])

    for gid, g in sorted_groups:
        print(f'--- Group {gid} ({len(g["members"])} members, max concurrent {g["max_members"]}) ---')
        print(f'  First join:        {g["first_join_ts"]} (member={g["first_member"][:16]}..., got {g["first_member_sent"]} partitions)')

        # Find the first REVOKE-FIRST for this group (initial member losing partitions).
        initial_revokes = [r for r in g['revoke_events'] if r[3] == g['first_member_sent']]
        if initial_revokes:
            ts, mid, reconciled, sent, target = initial_revokes[0]
            revoke_t = parse_ts(ts)
            print(f'  First revoke:      {ts} (member={mid[:16]}..., sent={sent} => reconciled={reconciled}, needs to revoke {sent - reconciled})')
            print(f'    Time to trigger: {revoke_t - g["first_join_t"]:.1f}s after first join')
        else:
            revoke_t = None

        # Find when the initial member's pending revocations cleared.
        first_member_clears = [c for c in g['pending_cleared'] if c[1] == g['first_member']]
        if first_member_clears:
            first_clear = first_member_clears[0]
            clear_t = parse_ts(first_clear[0])
            print(f'  Revoke confirmed:  {first_clear[0]} (member={first_clear[1][:16]}..., cleared={first_clear[2]}, kept={first_clear[3]})')
            if revoke_t:
                print(f'    Revoke duration: {clear_t - revoke_t:.1f}s (client prerevoking phase)')

        # Show skip-unrevoked events (server waiting for client).
        if g['skip_events']:
            print(f'  Skip-unrevoked:    {len(g["skip_events"])} heartbeats where server waited for client revocation')
            for ts, mid, pending in g['skip_events']:
                print(f'    {ts} member={mid[:16]}... pending={pending}')

        # Show blocked events count.
        if g['blocked_events']:
            unique_blocked_members = set(mid for _, mid, _ in g['blocked_events'])
            print(f'  Blocked members:   {len(unique_blocked_members)} members blocked across {len(g["blocked_events"])} heartbeats')
            # Show first and last blocked event.
            first_blocked = g['blocked_events'][0]
            last_blocked = g['blocked_events'][-1]
            print(f'    First: {first_blocked[0]} member={first_blocked[1][:16]}... blocked={first_blocked[2]}')
            print(f'    Last:  {last_blocked[0]} member={last_blocked[1][:16]}... blocked={last_blocked[2]}')
            blocked_duration = parse_ts(last_blocked[0]) - parse_ts(first_blocked[0])
            print(f'    Duration: {blocked_duration:.1f}s of members unable to receive partitions')

        # Show client-side prerevoking timeline.
        if g['client_prerevoke_start']:
            pr_start = g['client_prerevoke_start'][0]
            print(f'  Client prerevoke:  started at {pr_start[0]} (client {pr_start[1]}, lost={pr_start[2]} partitions)')
        keepalive_prerevoking = [k for k in g['client_keepalives'] if k[2] == 'true']
        if keepalive_prerevoking:
            print(f'    Keepalives while prerevoking: {len(keepalive_prerevoking)}')
            for ts, cid, _ in keepalive_prerevoking:
                print(f'      {ts} (client {cid})')
        if g['client_prerevoke_done']:
            pr_done = g['client_prerevoke_done'][0]
            print(f'    Prerevoke done:  {pr_done[0]} (client {pr_done[1]}, lost={pr_done[2]})')
            if g['client_prerevoke_start']:
                pr_duration = parse_ts(pr_done[0]) - parse_ts(g['client_prerevoke_start'][0][0])
                print(f'    Client-side prerevoke duration: {pr_duration:.1f}s')
        # First full Topics after prerevoke.
        if g['client_prerevoke_done'] and g['client_full_topics']:
            done_t = parse_ts(g['client_prerevoke_done'][0][0])
            post_prerevoke_fulls = [(ts, cid, cnt) for ts, cid, cnt in g['client_full_topics'] if parse_ts(ts) > done_t]
            if post_prerevoke_fulls:
                first_full = post_prerevoke_fulls[0]
                full_delay = parse_ts(first_full[0]) - done_t
                print(f'    First full Topics after prerevoke: {first_full[0]} (+{full_delay:.1f}s, {first_full[2]} partitions)')

        # Convergence: when all members got partitions.
        if g['all_members_assigned_ts']:
            print(f'  All assigned:      {g["all_members_assigned_ts"]}')
            convergence_time = g['all_members_assigned_t'] - g['first_join_t']
            print(f'    Total convergence: {convergence_time:.1f}s from first join')
        else:
            print(f'  All assigned:      NEVER - group did not fully converge')
            # Show which members never got real partitions.
            unassigned = g['members'] - set(g['member_first_real_send'].keys())
            if unassigned:
                print(f'    Unassigned members ({len(unassigned)}):')
                for mid in sorted(unassigned):
                    print(f'      {mid}')

        # Show leaves if any.
        if g['leaves']:
            print(f'  Leaves:            {len(g["leaves"])}')

        print()

    # Summary table.
    print('=== SUMMARY TABLE ===')
    print(f'{"GROUP":<20} {"MEM":>3} {"JOIN":>8} {"1st-REVOKE":>12} {"REVOKE-DUR":>10} {"BLOCKED":>7} {"CONVERGED":>10} {"TOTAL":>8}  STATUS')
    print('-' * 110)

    for gid, g in sorted_groups:
        join_ts = g['first_join_ts'][:12]
        n_mem = len(g['members'])

        # First revoke time.
        initial_revokes = [r for r in g['revoke_events'] if r[3] == g['first_member_sent']]
        revoke_ts = initial_revokes[0][0][:12] if initial_revokes else '-'
        revoke_t = parse_ts(initial_revokes[0][0]) if initial_revokes else None

        # Revoke duration.
        first_member_clears = [c for c in g['pending_cleared'] if c[1] == g['first_member']]
        if first_member_clears and revoke_t:
            clear_t = parse_ts(first_member_clears[0][0])
            revoke_dur = f'{clear_t - revoke_t:.1f}s'
        else:
            revoke_dur = '-'

        n_blocked = len(g['blocked_events'])
        conv_ts = g['all_members_assigned_ts'][:12] if g['all_members_assigned_ts'] else 'NEVER'
        total = f'{g["all_members_assigned_t"] - g["first_join_t"]:.1f}s' if g['all_members_assigned_t'] else 'STUCK'

        status = 'OK' if g['all_members_assigned_ts'] else 'STUCK'
        if g['all_members_assigned_t'] and g['all_members_assigned_t'] - g['first_join_t'] > 15:
            status = 'SLOW'

        print(f'{gid[:20]:<20} {n_mem:>3} {join_ts:>8} {revoke_ts:>12} {revoke_dur:>10} {n_blocked:>7} {conv_ts:>10} {total:>8}  {status}')

def main_848_summary(args):
    """Quick overview of all 848 groups - which are stuck and why.

    Scans server log for all 848 groups and shows a one-line summary per group:
    members, joins, leaves, session timeouts, max generation, and convergence status.
    """
    with open(args.server) as f:
        server_lines = f.readlines()

    p_join = re.compile(r'consumerJoin: group=(\S+)\s+member=(\S+)\s+epoch=(\d+)')
    p_leave = re.compile(r'consumerLeave: group=(\S+)\s+member=(\S+)')
    p_static_leave = re.compile(r'consumerStaticLeave: group=(\S+)\s+member=(\S+)')
    p_timeout = re.compile(r'consumerSessionTimeout: group=(\S+)\s+member=(\S+)\s+epoch=(\d+)\s+remaining=(\d+)')
    p_rebal_timeout = re.compile(r'consumerRebalanceTimeout: group=(\S+)\s+member=(\S+)')
    p_target = re.compile(r'computeTarget: group=(\S+)\s+member=(\S+)\s+target=(\d+)\s+current=(\d+)\s+pending=(\d+)\s+memberEpoch=(\d+)\s+gen=(\d+)')
    p_fence = re.compile(r'fenceConsumer: group=(\S+)\s+member=(\S+)\s+freed=(\d+)')
    p_revoke = re.compile(r'RECONCILE REVOKE-FIRST: member=(\S+)\s+reconciled=(\d+)\s+sent=(\d+)\s+target=(\d+)')
    p_blocked = re.compile(r'RECONCILE SUMMARY: member=(\S+)\s+reconciled=(\d+)\s+target=(\d+)\s+allowed=(\d+)\s+blocked=(\d+)')

    # Collect per-group stats.
    groups = {}  # group_id => stats dict

    def g(gid):
        if gid not in groups:
            groups[gid] = {
                'members': set(), 'joins': 0, 'leaves': 0,
                'timeouts': 0, 'fences': 0, 'max_gen': 0,
                'revoke_first': 0, 'total_blocked': 0,
                'last_target': {},  # member => (target, current, pending, epoch, gen)
                'alive_members': set(),
            }
        return groups[gid]

    for line in server_lines:
        m = p_join.search(line)
        if m:
            gid, mid, epoch = m.group(1), m.group(2), int(m.group(3))
            s = g(gid)
            s['members'].add(mid)
            s['alive_members'].add(mid)
            s['joins'] += 1
            continue

        m = p_leave.search(line)
        if m:
            gid, mid = m.group(1), m.group(2)
            s = g(gid)
            s['leaves'] += 1
            s['alive_members'].discard(mid)
            continue

        m = p_static_leave.search(line)
        if m:
            gid, mid = m.group(1), m.group(2)
            s = g(gid)
            s['leaves'] += 1
            s['alive_members'].discard(mid)
            continue

        m = p_timeout.search(line)
        if m:
            gid, mid = m.group(1), m.group(2)
            s = g(gid)
            s['timeouts'] += 1
            continue

        m = p_rebal_timeout.search(line)
        if m:
            gid, mid = m.group(1), m.group(2)
            s = g(gid)
            s['timeouts'] += 1
            continue

        m = p_fence.search(line)
        if m:
            gid, mid = m.group(1), m.group(2)
            s = g(gid)
            s['fences'] += 1
            s['alive_members'].discard(mid)
            continue

        m = p_target.search(line)
        if m:
            gid = m.group(1)
            mid, target, current, pending, mepoch, gen = m.group(2), int(m.group(3)), int(m.group(4)), int(m.group(5)), int(m.group(6)), int(m.group(7))
            s = g(gid)
            s['max_gen'] = max(s['max_gen'], gen)
            s['last_target'][mid] = (target, current, pending, mepoch, gen)
            continue

        m = p_blocked.search(line)
        if m:
            blocked = int(m.group(5))
            if blocked > 0:
                # Find group by member - check all groups.
                mid = m.group(1)
                for gid, s in groups.items():
                    if mid in s['members']:
                        s['total_blocked'] += blocked
                        break
            continue

        m = p_revoke.search(line)
        if m:
            mid = m.group(1)
            for gid, s in groups.items():
                if mid in s['members']:
                    s['revoke_first'] += 1
                    break
            continue

    # Print summary table.
    print(f"{'GROUP':<20} {'MEM':>3} {'ALIVE':>5} {'JOIN':>4} {'LEAVE':>5} {'TMOUT':>5} {'FENCE':>5} {'GEN':>4} {'REVOKE':>6} {'BLOCKED':>7}  STATUS")
    print('-' * 110)

    for gid in sorted(groups.keys()):
        s = groups[gid]
        # Determine status from last target state.
        status = ''
        converged = True
        unconverged_members = []
        for mid, (target, current, pending, mepoch, gen) in s['last_target'].items():
            if mid not in s['alive_members']:
                continue
            if target != current or mepoch != gen:
                converged = False
                unconverged_members.append(f"{mid[:8]}(t={target},c={current},p={pending})")
        if not s['alive_members']:
            status = 'EMPTY'
        elif s['timeouts'] > 0 and not converged:
            status = 'TIMEOUT-CASCADE: ' + ' '.join(unconverged_members[:3])
        elif not converged:
            status = 'UNCONVERGED: ' + ' '.join(unconverged_members[:3])
        else:
            status = 'OK'

        print(f"{gid:<20} {len(s['members']):>3} {len(s['alive_members']):>5} {s['joins']:>4} {s['leaves']:>5} {s['timeouts']:>5} {s['fences']:>5} {s['max_gen']:>4} {s['revoke_first']:>6} {s['total_blocked']:>7}  {status}")


def main_848_assign_trace(args):
    """Trace 848 assignment lifecycle for a specific group from client + server logs.

    Shows chronological events: handleResp comparisons, assignment changes,
    setupAssignedAndHeartbeat returns, keepalive/full decisions, and errors.
    Highlights where partition counts drop or get stuck.
    """
    group_prefix = args.group
    after_ts = parse_ts(args.after) if args.after else 0
    p_ts = re.compile(r'(\d+:\d+:\d+\.\d+)')

    events = []  # (ts_sec, ts_str, source, category, detail)

    # Client log patterns
    client_patterns = [
        ('HANDLERESP', re.compile(r'848 handleResp: comparison.*hasAssignment: (\w+).*currentCount: (\d+), newCount: (\d+), equal: (\w+), wasKeepalive: (\w+), epoch: (\d+)')),
        ('HANDLERESP_UNRESOLVED', re.compile(r'848 handleResp: early nil return')),
        ('HANDLERESP_UNRESOLVED_TOPIC', re.compile(r'848 handleResp: unresolved topic ID')),
        ('ASSIGN_CHANGED', re.compile(r'848 heartbeat: assignment changed.*newPartitions: (\d+)')),
        ('SETUP_RETURNED', re.compile(r'848 setupAssignedAndHeartbeat returned.*err: ([^,]+), hasNewAssignment: (\w+), newAssignmentParts: (\d+)')),
        ('SESSION_BEGUN', re.compile(r'new group session begun.*nowAssignedPartitions: (\d+)')),
        ('KEEPALIVE', re.compile(r'848 heartbeat: sending keepalive.*topicPartitions: (\d+)')),
        ('FULL_REQ', re.compile(r'848 heartbeat: sending full Topics.*topicPartitions: (\d+)')),
        ('RESP_ERROR', re.compile(r'848 heartbeat: response error code.*err: ([^,]+), epoch: (\d+)')),
        ('REQ_ERROR', re.compile(r'848 heartbeat: request error.*err: (.+)')),
        ('STORING_EPOCH', re.compile(r'storing (?:member and )?epoch.*epoch: (\d+)')),
    ]

    with open(args.client) as f:
        for line in f:
            if group_prefix not in line:
                continue
            m = p_ts.search(line)
            if not m:
                continue
            ts_str = m.group(1)
            ts_sec = parse_ts(ts_str)
            if ts_sec < after_ts:
                continue
            for cat, pat in client_patterns:
                pm = pat.search(line)
                if pm:
                    events.append((ts_sec, ts_str, 'C', cat, pm.groups()))
                    break

    # Server log patterns
    server_patterns = [
        ('SRV_HB', re.compile(r'consumerHeartbeat: group=\S+ member=(\S+) reqEpoch=(\d+) memberEpoch=(\d+) prevEpoch=(\d+) full=(\w+) gen=(\d+)')),
        ('SRV_FENCED', re.compile(r'consumerHeartbeat FENCED: group=\S+ member=(\S+) reqEpoch=(\d+) memberEpoch=(\d+).*matchesPrev=(\w+) topicsNil=(\w+) isSubset=(\w+)')),
        ('SRV_RECONCILE', re.compile(r'consumerHeartbeat: reconcile group=\S+ member=(\S+) full=(\w+) changed=(\w+) sendAssignment=(\w+) reconciledParts=(\d+) lastSentParts=(\d+) epoch=(\d+)')),
        ('SRV_EPOCH_BUMP', re.compile(r'consumerHeartbeat: epoch bump group=\S+ member=(\S+) (\d+)->(\d+) gen=(\d+)')),
        ('SRV_SKIP', re.compile(r'consumerHeartbeat: skip reconcile group=\S+ member=(\S+) pendingRevoke=(\d+)')),
        ('SRV_SEND', re.compile(r'RECONCILE SEND: member=(\S+) epoch=(\d+) partitions=(\d+)')),
        ('SRV_COMMIT_STALE', re.compile(r'OffsetCommit STALE: group=\S+ member=(\S+) reqEpoch=(\d+) memberEpoch=(\d+)')),
    ]

    with open(args.server) as f:
        for line in f:
            if group_prefix[:16] not in line:
                continue
            m = p_ts.search(line)
            if not m:
                continue
            ts_str = m.group(1)
            ts_sec = parse_ts(ts_str)
            if ts_sec < after_ts:
                continue
            for cat, pat in server_patterns:
                pm = pat.search(line)
                if pm:
                    events.append((ts_sec, ts_str, 'S', cat, pm.groups()))
                    break

    events.sort(key=lambda e: e[0])

    if not events:
        print(f"No events found for group prefix '{group_prefix}'")
        return

    print(f"=== 848 ASSIGN TRACE: {group_prefix[:16]}... ===")
    print(f"  Events: {len(events)}")
    print(f"  Time range: {events[0][1]} - {events[-1][1]}")
    print()

    # Count events by category
    counts = defaultdict(int)
    for _, _, _, cat, _ in events:
        counts[cat] += 1
    print("  Event counts:")
    for cat in sorted(counts.keys()):
        print(f"    {cat}: {counts[cat]}")
    print()

    # Find where partition count gets stuck: track keepalive partition
    # counts and find transitions and long runs of the same count.
    keepalive_runs = []  # (start_ts, end_ts, count, n_keepalives)
    cur_count = None
    cur_start = None
    cur_n = 0
    for ts_sec, ts_str, src, cat, groups in events:
        if cat == 'KEEPALIVE':
            n = int(groups[0])
            if n != cur_count:
                if cur_count is not None:
                    keepalive_runs.append((cur_start, ts_str, cur_count, cur_n))
                cur_count = n
                cur_start = ts_str
                cur_n = 1
            else:
                cur_n += 1
    if cur_count is not None:
        keepalive_runs.append((cur_start, events[-1][1], cur_count, cur_n))

    print("  === KEEPALIVE PARTITION COUNT RUNS ===")
    for start, end, count, n in keepalive_runs:
        dur = parse_ts(end) - parse_ts(start)
        flag = " <<<" if dur > 5 and n > 10 else ""
        print(f"    {start} - {end} ({dur:.1f}s, {n} keepalives): topicPartitions={count}{flag}")
    print()

    # Show non-keepalive events (the interesting ones) plus keepalives
    # at transitions.
    print("  === KEY EVENTS ===")
    last_keepalive_count = None
    for ts_sec, ts_str, src, cat, groups in events:
        if cat == 'KEEPALIVE':
            n = int(groups[0])
            if n != last_keepalive_count:
                print(f"    {ts_str} [{src}] {cat}: topicPartitions={n}")
                last_keepalive_count = n
            continue
        last_keepalive_count = None

        if cat == 'HANDLERESP':
            hasAssign, curN, newN, eq, keepalive, epoch = groups
            marker = ""
            if eq == 'true' and int(curN) != int(newN):
                marker = " !!! EQUAL BUT DIFFERENT COUNTS"
            print(f"    {ts_str} [{src}] {cat}: cur={curN} new={newN} eq={eq} keepalive={keepalive} epoch={epoch} hasAssign={hasAssign}{marker}")
        elif cat == 'ASSIGN_CHANGED':
            print(f"    {ts_str} [{src}] {cat}: newPartitions={groups[0]}")
        elif cat == 'SETUP_RETURNED':
            err, hasNew, parts = groups
            print(f"    {ts_str} [{src}] {cat}: err={err} hasNew={hasNew} parts={parts}")
        elif cat == 'SESSION_BEGUN':
            print(f"    {ts_str} [{src}] {cat}: nowAssigned={groups[0]}")
        elif cat == 'FULL_REQ':
            print(f"    {ts_str} [{src}] {cat}: topicPartitions={groups[0]}")
        elif cat == 'RESP_ERROR':
            print(f"    {ts_str} [{src}] {cat}: err={groups[0]} epoch={groups[1]}")
        elif cat == 'REQ_ERROR':
            print(f"    {ts_str} [{src}] {cat}: err={groups[0]}")
        elif cat == 'STORING_EPOCH':
            pass  # too noisy, skip
        elif cat == 'HANDLERESP_UNRESOLVED':
            print(f"    {ts_str} [{src}] {cat}")
        elif cat == 'HANDLERESP_UNRESOLVED_TOPIC':
            print(f"    {ts_str} [{src}] {cat}")
        elif cat == 'SRV_FENCED':
            member, reqE, memE, matchPrev, topicsNil, isSubset = groups
            print(f"    {ts_str} [{src}] {cat}: member={member[:12]} reqEpoch={reqE} memberEpoch={memE} matchesPrev={matchPrev} topicsNil={topicsNil} isSubset={isSubset}")
        elif cat == 'SRV_RECONCILE':
            member, full, changed, send, rParts, sParts, epoch = groups
            if changed == 'true' or send == 'true':
                print(f"    {ts_str} [{src}] {cat}: member={member[:12]} full={full} changed={changed} send={send} reconciled={rParts} lastSent={sParts} epoch={epoch}")
        elif cat == 'SRV_EPOCH_BUMP':
            member, old, new, gen = groups
            print(f"    {ts_str} [{src}] {cat}: member={member[:12]} {old}->{new} gen={gen}")
        elif cat == 'SRV_SEND':
            member, epoch, parts = groups
            print(f"    {ts_str} [{src}] {cat}: member={member[:12]} epoch={epoch} partitions={parts}")
        elif cat == 'SRV_SKIP':
            member, pending = groups
            print(f"    {ts_str} [{src}] {cat}: member={member[:12]} pendingRevoke={pending}")
        elif cat == 'SRV_COMMIT_STALE':
            member, reqE, memE = groups
            print(f"    {ts_str} [{src}] {cat}: member={member[:12]} reqEpoch={reqE} memberEpoch={memE}")
        elif cat == 'SRV_HB':
            # Only log when full or epoch mismatch
            member, reqE, memE, prevE, full, gen = groups
            if full == 'true' or reqE != memE:
                print(f"    {ts_str} [{src}] {cat}: member={member[:12]} reqEpoch={reqE} memberEpoch={memE} full={full} gen={gen}")


def main_dropped_response(args):
    """Find requests that were hijacked but never got a response.

    Parses dispatch: hijacked (request sent to group goroutine),
    dispatch: responding (response sent by cluster run loop),
    and group: reply (response sent by group goroutine).

    A hijacked request without a matching reply is a dropped response
    that will deadlock the client connection.
    """
    p_hijacked = re.compile(
        r'(\d+:\d+:\d+\.\d+) \[DBG\] dispatch: hijacked (\S+) node=(\d+) seq=(\d+)')
    p_responding = re.compile(
        r'(\d+:\d+:\d+\.\d+) \[DBG\] dispatch: responding (\S+) node=(\d+) seq=(\d+)')
    p_reply = re.compile(
        r'(\d+:\d+:\d+\.\d+) \[DBG\] group: reply (\S+) node=(\d+) seq=(\d+)')

    hijacked = {}   # (node, seq) => (ts, reqtype)
    responded = set()  # (node, seq)
    replied = set()    # (node, seq)

    hijack_counts = defaultdict(int)
    reply_counts = defaultdict(int)
    respond_counts = defaultdict(int)

    with open(args.server) as f:
        for line in f:
            m = p_hijacked.search(line)
            if m:
                ts, reqtype, node, seq = m.group(1), m.group(2), m.group(3), m.group(4)
                key = (node, seq)
                hijacked[key] = (ts, reqtype)
                hijack_counts[reqtype] += 1
                continue
            m = p_responding.search(line)
            if m:
                ts, reqtype, node, seq = m.group(1), m.group(2), m.group(3), m.group(4)
                responded.add((node, seq))
                respond_counts[reqtype] += 1
                continue
            m = p_reply.search(line)
            if m:
                ts, reqtype, node, seq = m.group(1), m.group(2), m.group(3), m.group(4)
                replied.add((node, seq))
                reply_counts[reqtype] += 1
                continue

    print("=== DISPATCH SUMMARY ===")
    print(f"  Hijacked: {len(hijacked)}")
    print(f"  Responded (cluster): {len(responded)}")
    print(f"  Replied (group): {len(replied)}")
    print()

    print("  Hijacked by type:")
    for rt in sorted(hijack_counts, key=lambda x: -hijack_counts[x]):
        print(f"    {rt}: {hijack_counts[rt]}")
    print()

    # Find dropped: hijacked but no reply
    dropped = []
    for key, (ts, reqtype) in hijacked.items():
        if key not in replied:
            dropped.append((ts, reqtype, key[0], key[1]))
    dropped.sort()

    print(f"  === DROPPED RESPONSES ({len(dropped)}) ===")
    if dropped:
        for ts, reqtype, node, seq in dropped[:50]:
            print(f"    {ts} {reqtype} node={node} seq={seq}")
        if len(dropped) > 50:
            print(f"    ... ({len(dropped) - 50} more)")
    else:
        print("    (none - all hijacked requests got replies)")
    print()


def main_fetch_trace(args):
    """Trace fetch activity for a specific topic from server logs.

    Shows fetch entries per minute, watch events, session filtering,
    commits, and detects stalls where fetch activity drops. Also shows
    per-node noWatch events with byte counts near the stall transition.
    """
    topic_prefix = args.topic
    p_ts = re.compile(r'^(\d+:\d+:\d+\.\d+)')

    entries_per_min = defaultdict(int)
    watches_per_min = defaultdict(int)
    watch_resp_per_min = defaultdict(int)
    session_filt_per_min = defaultdict(int)
    commits_per_min = defaultdict(int)
    entries_per_sec = defaultdict(int)

    # For noWatch, we can't filter by topic (noWatch log doesn't contain topic).
    # Instead, track noWatch globally per minute.
    nowatch_global_per_min = defaultdict(int)

    # Track fetch entry nodes+partitions for stall analysis
    entry_details = []  # (ts, node, nparts)

    with open(args.server) as f:
        for line in f:
            m = p_ts.match(line)
            if not m:
                continue
            ts = m.group(1)
            minute = ts[:5]
            sec = ts[:8]

            # Global noWatch tracking (not topic-filtered)
            if 'fetch: noWatch' in line:
                nowatch_global_per_min[minute] += 1
                continue

            if topic_prefix not in line:
                continue

            if 'fetch: entry' in line:
                entries_per_min[minute] += 1
                entries_per_sec[sec] += 1
                nm = re.search(r'node=(\d+).*' + re.escape(topic_prefix[:12]) + r':(\d+)', line)
                if nm:
                    entry_details.append((ts, int(nm.group(1)), int(nm.group(2))))
            elif 'watch created' in line:
                watches_per_min[minute] += 1
            elif 'watch response' in line:
                watch_resp_per_min[minute] += 1
            elif 'session filtered' in line:
                session_filt_per_min[minute] += 1
            elif 'OffsetCommit:' in line and 'STALE' not in line:
                commits_per_min[minute] += 1

    if not entries_per_min:
        print(f"No fetch entries for topic prefix '{topic_prefix}'")
        return

    print(f"=== FETCH TRACE: {topic_prefix} ===")
    print()
    print(f"  {'MINUTE':>8} {'ENTRIES':>7} {'WATCHES':>7} {'W-RESP':>7} {'S-FILT':>7} {'COMMIT':>6} {'NOWATCH*':>8}  STATUS")
    print(f"  {'-'*72}")

    all_minutes = sorted(set(list(entries_per_min.keys()) + list(nowatch_global_per_min.keys())))
    prev_entries = None
    for minute in all_minutes:
        e = entries_per_min.get(minute, 0)
        w = watches_per_min.get(minute, 0)
        wr = watch_resp_per_min.get(minute, 0)
        sf = session_filt_per_min.get(minute, 0)
        cm = commits_per_min.get(minute, 0)
        nwg = nowatch_global_per_min.get(minute, 0)
        status = ''
        if prev_entries is not None and prev_entries > 500 and e < 200:
            status = '<<< STALL'
        elif e < 10 and e > 0:
            status = 'idle'
        prev_entries = e
        print(f"  {minute:>8} {e:>7} {w:>7} {wr:>7} {sf:>7} {cm:>6} {nwg:>8}  {status}")

    # Find stall transition
    print()
    all_secs = sorted(entries_per_sec.keys())
    drop_at = None
    if len(all_secs) > 10:
        max_drop = 0
        for i in range(1, len(all_secs)):
            prev = entries_per_sec[all_secs[i-1]]
            cur = entries_per_sec[all_secs[i]]
            drop = prev - cur
            if drop > max_drop and prev > 10:
                max_drop = drop
                drop_at = i
        if drop_at and max_drop > 10:
            start = max(0, drop_at - 3)
            end = min(len(all_secs), drop_at + 5)
            print(f"  === STALL TRANSITION (biggest drop: {max_drop} entries/sec) ===")
            for i in range(start, end):
                s = all_secs[i]
                marker = " <<<" if i == drop_at else ""
                print(f"    {s}: {entries_per_sec[s]} entries/sec{marker}")
    print()

    # After the stall, analyze noWatch events to find wrongLeader interference.
    # The fetch request might include our topic, but a session partition from
    # a DIFFERENT topic on the wrong broker causes returnEarly. The noWatch
    # log mentions the wrong-leader topic, not ours.
    if drop_at:
        stall_ts = all_secs[drop_at]
        stall_sec = parse_ts(stall_ts)
        print(f"  === WRONG-LEADER ANALYSIS (after {stall_ts}) ===")

        p_nowatch = re.compile(
            r'(\d+:\d+:\d+\.\d+) \[DBG\] fetch: noWatch node=(\d+) '
            r'returnEarly=(\w+)\(([^)]*)\) nbytes=(\d+) MinBytes=(\d+) '
            r'pastDeadline=(\w+) partitions=(\d+)')
        wrong_leader_reasons = defaultdict(int)
        total_nowatch_after = 0
        with open(args.server) as f:
            for line in f:
                m = p_nowatch.search(line)
                if not m:
                    continue
                ts = m.group(1)
                if parse_ts(ts) < stall_sec:
                    continue
                total_nowatch_after += 1
                return_early = m.group(3)
                reason = m.group(4)
                if return_early == 'true' and reason:
                    wrong_leader_reasons[reason] += 1

        if wrong_leader_reasons:
            print(f"    Total noWatch after stall: {total_nowatch_after}")
            print(f"    returnEarly reasons:")
            for reason, count in sorted(wrong_leader_reasons.items(), key=lambda x: -x[1]):
                print(f"      {reason}: {count}")
        else:
            print(f"    Total noWatch after stall: {total_nowatch_after}")
            print(f"    No wrongLeader returnEarly events found")

        # Also check: are there watch-created events that DON'T contain our topic?
        watches_without_topic = 0
        watches_with_topic = 0
        with open(args.server) as f:
            for line in f:
                if 'watch created' not in line:
                    continue
                m = p_ts.match(line)
                if not m or parse_ts(m.group(1)) < stall_sec:
                    continue
                if topic_prefix[:8] in line:
                    watches_with_topic += 1
                else:
                    watches_without_topic += 1

        print(f"    Watches with our topic after stall: {watches_with_topic}")
        print(f"    Watches without our topic after stall: {watches_without_topic}")

        # Check for atEnd events for our topic after the stall
        at_end_count = 0
        at_end_samples = []
        data_lost_count = 0
        with open(args.server) as f:
            for line in f:
                m = p_ts.match(line)
                if not m or parse_ts(m.group(1)) < stall_sec:
                    continue
                if 'firstPass atEnd' in line and topic_prefix[:12] in line:
                    at_end_count += 1
                    if len(at_end_samples) < 5:
                        at_end_samples.append(line.strip())
                if 'secondPass atEnd' in line and topic_prefix[:12] in line:
                    at_end_count += 1
                if 'DATA LOST IN FILTER' in line:
                    data_lost_count += 1

        print(f"    atEnd events for our topic after stall: {at_end_count}")
        for s in at_end_samples:
            print(f"      {s}")
        print(f"    DATA LOST IN FILTER events: {data_lost_count}")
        print()

    # Show per-node partition counts at and after the stall
    if entry_details:
        # Group entries by second, show node distribution
        by_sec = defaultdict(lambda: defaultdict(list))
        for ts, node, nparts in entry_details:
            by_sec[ts[:8]][node].append(nparts)

        # Find the stall second
        stall_sec = None
        if drop_at:
            stall_sec = all_secs[drop_at]

        if stall_sec:
            print(f"  === PER-NODE ENTRIES AROUND STALL ===")
            secs = sorted(by_sec.keys())
            stall_idx = secs.index(stall_sec) if stall_sec in secs else -1
            if stall_idx >= 0:
                show_start = max(0, stall_idx - 2)
                show_end = min(len(secs), stall_idx + 8)
                for i in range(show_start, show_end):
                    s = secs[i]
                    parts = []
                    for node in sorted(by_sec[s].keys()):
                        counts = by_sec[s][node]
                        parts.append(f"node{node}={len(counts)}x(parts={counts})")
                    marker = " <<<" if s == stall_sec else ""
                    print(f"    {s}: {' '.join(parts)}{marker}")
            print()

    print("  * NOWATCH column is global (all topics), not filtered by topic")
    print()


def main_classic_summary(args):
    """Quick overview of all classic consumer groups.

    Scans server log for JoinGroup/SyncGroup/Heartbeat/OffsetCommit events
    to show group state: members, rebalances, commits, last activity time.
    """
    with open(args.server) as f:
        server_lines = f.readlines()

    p_ts = re.compile(r'^(\d+:\d+:\d+\.\d+)')
    p_join = re.compile(r'group (\S+): handleJoin memberID="([^"]*)", state=(\w+), members=(\d+)')
    p_sync = re.compile(r'group (\S+): handleSync memberID=(\S+), generation=(\d+), state=(\w+), isLeader=(\w+)')
    p_hb = re.compile(r'group (\S+): handleHeartbeat memberID=(\S+)')
    p_commit = re.compile(r'OffsetCommit: group=(\S+)\s+member=(\S+)')
    p_commit_stale = re.compile(r'OffsetCommit STALE: group=(\S+)')

    groups = {}

    def g(gid):
        if gid not in groups:
            groups[gid] = {
                'members': set(), 'joins': 0, 'syncs': 0,
                'heartbeats': 0, 'commits': 0, 'stale_commits': 0,
                'max_gen': 0, 'last_state': '?',
                'first_ts': None, 'last_ts': None,
                'last_members': 0,
            }
        return groups[gid]

    for line in server_lines:
        ts_m = p_ts.match(line)
        ts = ts_m.group(1) if ts_m else None

        m = p_join.search(line)
        if m:
            gid, mid, state, nmembers = m.group(1), m.group(2), m.group(3), int(m.group(4))
            s = g(gid[:16])
            if mid:
                s['members'].add(mid)
            s['joins'] += 1
            s['last_state'] = state
            s['last_members'] = nmembers
            if ts and not s['first_ts']:
                s['first_ts'] = ts
            if ts:
                s['last_ts'] = ts
            continue

        m = p_sync.search(line)
        if m:
            gid, mid, gen = m.group(1), m.group(2), int(m.group(3))
            s = g(gid[:16])
            s['members'].add(mid)
            s['syncs'] += 1
            s['max_gen'] = max(s['max_gen'], gen)
            if ts:
                s['last_ts'] = ts
            continue

        m = p_commit.search(line)
        if m:
            gid = m.group(1)
            s = g(gid[:16])
            s['commits'] += 1
            if ts:
                s['last_ts'] = ts
            continue

        m = p_commit_stale.search(line)
        if m:
            gid = m.group(1)
            s = g(gid[:16])
            s['stale_commits'] += 1
            continue

    print(f"{'GROUP':<18} {'MEM':>3} {'JOIN':>4} {'SYNC':>4} {'GEN':>3} {'COMMIT':>6} {'STALE':>5} {'FIRST':>15} {'LAST':>15}  STATE")
    print('-' * 110)

    for gid in sorted(groups.keys()):
        s = groups[gid]
        print(f"{gid:<18} {len(s['members']):>3} {s['joins']:>4} {s['syncs']:>4} {s['max_gen']:>3} {s['commits']:>6} {s['stale_commits']:>5} {s['first_ts'] or '?':>15} {s['last_ts'] or '?':>15}  {s['last_state']}")


def main_group_timeline(args):
    """Show chronological activity timeline for a specific group.

    Parses server log for all events (join, leave, heartbeat, OffsetCommit,
    TxnOffsetCommit, reconciliation, session/rebalance timeout, fence) for the
    specified group and presents them chronologically with gap detection.

    Use --after / --before to narrow the window.
    """
    group_prefix = args.group
    gap_threshold = args.gap
    after_ts = parse_ts(args.after) if args.after else 0
    before_ts = parse_ts(args.before) if args.before else 999999

    events = []  # (ts_str, ts_sec, category, detail)

    p_ts = re.compile(r'^(\d+:\d+:\d+\.\d+)')

    # Patterns to look for
    patterns = [
        ('JOIN', re.compile(r'consumerJoin: group=(\S+)\s+member=(\S+)\s+epoch=(\d+)\s+sent=(\d+)\s+epochMap=(\d+)\s+members=(\d+)')),
        ('LEAVE', re.compile(r'consumerLeave: group=(\S+)\s+member=(\S+)\s+epoch=(\d+)\s+sent=(\d+)\s+pending=(\d+)\s+remaining=(\d+)')),
        ('STATIC_LEAVE', re.compile(r'consumerStaticLeave: group=(\S+)\s+member=(\S+)\s+instance=(\S+)')),
        ('SESSION_TIMEOUT', re.compile(r'consumerSessionTimeout: group=(\S+)\s+member=(\S+)\s+epoch=(\d+)\s+remaining=(\d+)')),
        ('REBAL_TIMEOUT', re.compile(r'consumerRebalanceTimeout: group=(\S+)\s+member=(\S+)\s+epoch=(\d+)\s+remaining=(\d+)')),
        ('FENCE', re.compile(r'fenceConsumer: group=(\S+)\s+member=(\S+)\s+freed=(\d+)\s+\(sent=(\d+)\s+pending=(\d+)\)\s+epochMap=(\d+)')),
        ('TARGET', re.compile(r'computeTarget: group=(\S+)\s+member=(\S+)\s+target=(\d+)\s+current=(\d+)\s+pending=(\d+)\s+memberEpoch=(\d+)\s+gen=(\d+)')),
        ('RECONCILE', re.compile(r'RECONCILE SUMMARY: member=(\S+)\s+reconciled=(\d+)\s+target=(\d+)\s+allowed=(\d+)\s+blocked=(\d+)')),
        ('REVOKE', re.compile(r'RECONCILE REVOKE-FIRST: member=(\S+)\s+reconciled=(\d+)\s+target=(\d+)\s+pending=(\d+)')),
        ('SEND', re.compile(r'RECONCILE SEND: member=(\S+)\s+epoch=(\d+)\s+partitions=(\d+)')),
        ('PENDING_CLR', re.compile(r'pendingRevoke cleared: member=(\S+)\s+epoch=(\d+)\s+cleared=(\d+)\s+kept=(\d+)')),
        ('COMMIT', re.compile(r'OffsetCommit: group=(\S+)\s+member=(\S+)\s+topic=(\S+)\s+p=(\d+)\s+offset=(\d+)')),
        ('COMMIT_STALE', re.compile(r'OffsetCommit STALE: group=(\S+)\s+member=(\S+)\s+reqEpoch=(\d+)\s+memberEpoch=(\d+)\s+assignmentEpoch=(\d+)')),
        ('TXN_COMMIT', re.compile(r'TxnOffsetCommit: group=(\S+)\s+topic=(\S+)\s+p=(\d+)\s+offset=(\d+)\s+pid=(\d+)\s+epoch=(\d+)')),
        ('TXN_APPLY', re.compile(r'TxnOffsetApply: group=(\S+)\s+topic=(\S+)\s+p=(\d+)\s+offset=(\d+)\s+pid=(\d+)\s+epoch=(\d+)')),
        ('TXN_DISCARD', re.compile(r'TxnOffsetDiscard: group=(\S+)')),
        # Classic protocol events
        ('C_JOIN', re.compile(r'group (\S+): handleJoin memberID="([^"]*)", state=(\w+), members=(\d+)')),
        ('C_SYNC', re.compile(r'group (\S+): handleSync memberID=(\S+), generation=(\d+), state=(\w+), isLeader=(\w+)')),
        ('C_HEARTBEAT', re.compile(r'group (\S+): handleHeartbeat memberID=(\S+)')),
        ('C_LEAVE', re.compile(r'group (\S+): handleLeave, state=(\w+), members=(\d+), leaving=(\d+)')),
        ('C_REBALANCE', re.compile(r'group (\S+): rebalance triggered, current state (\w+), members (\d+)')),
        ('C_COMPLETE', re.compile(r'group (\S+): completeRebalance called, members (\d+)')),
        ('C_STATIC_REPLACE', re.compile(r'group (\S+): replaceStaticMember')),
        ('C_WAIT_REBALANCE', re.compile(r'group (\S+): rebalance timeout started')),
        ('C_REBAL_TIMEOUT', re.compile(r'group (\S+): rebalance timed out')),
        ('C_SESSION_TIMEOUT', re.compile(r'group (\S+): session timeout for member (\S+)')),
    ]

    with open(args.server) as f:
        for line in f:
            ts_m = p_ts.match(line)
            if not ts_m:
                continue
            ts_str = ts_m.group(1)
            ts_sec = parse_ts(ts_str)
            if ts_sec < after_ts or ts_sec > before_ts:
                continue

            for category, pat in patterns:
                m = pat.search(line)
                if not m:
                    continue
                # Check group match - group is always group(1) for most patterns
                # except RECONCILE/REVOKE/SEND/PENDING_CLR which have member only
                if category in ('RECONCILE', 'REVOKE', 'SEND', 'PENDING_CLR'):
                    # These don't have group= in the log, we'd need to look up by member.
                    # For now, skip these and only match group-prefixed patterns.
                    # Actually they DO have the group context in the line sometimes.
                    # Let's just check if group_prefix appears in the line.
                    if group_prefix not in line:
                        continue
                    detail = m.group(0)
                else:
                    gid = m.group(1)
                    if not gid.startswith(group_prefix):
                        continue
                    detail = m.group(0)

                events.append((ts_str, ts_sec, category, detail))
                break

    if not events:
        print(f"No events found for group prefix '{group_prefix}'")
        return

    # Print timeline with gap detection
    print(f"=== GROUP TIMELINE: {group_prefix}... ===")
    print(f"  Events: {len(events)}")
    print(f"  Time range: {events[0][0]} - {events[-1][0]}")
    print()

    # Summary counts
    counts = defaultdict(int)
    for _, _, cat, _ in events:
        counts[cat] += 1
    print("  Event counts:")
    for cat in sorted(counts.keys()):
        print(f"    {cat}: {counts[cat]}")
    print()

    # Members activity tracking
    member_last_seen = {}  # member => last timestamp
    member_commits = defaultdict(int)  # member => total commits
    stale_events = []
    gaps = []

    prev_ts = None
    for ts_str, ts_sec, category, detail in events:
        if prev_ts is not None:
            gap = ts_sec - prev_ts
            if gap >= gap_threshold:
                gaps.append((ts_str, gap, category, detail))
        prev_ts = ts_sec

        # Track member activity
        m = re.search(r'member=(\S+)', detail)
        if m:
            member_last_seen[m.group(1)] = ts_str

        if category == 'COMMIT':
            m = re.search(r'member=(\S+)', detail)
            if m:
                member_commits[m.group(1)] += 1

        if category == 'COMMIT_STALE':
            stale_events.append((ts_str, detail))

    if gaps:
        print(f"  === GAPS > {gap_threshold}s ({len(gaps)}) ===")
        for ts_str, gap, category, detail in gaps:
            print(f"    {ts_str} [{gap:.1f}s gap] {category}: {detail}")
        print()

    if stale_events:
        print(f"  === OFFSET COMMIT STALE ({len(stale_events)}) ===")
        for ts_str, detail in stale_events:
            print(f"    {ts_str} {detail}")
        print()

    if member_commits:
        print(f"  === PER-MEMBER COMMIT COUNTS ===")
        for mid, cnt in sorted(member_commits.items(), key=lambda x: -x[1]):
            last = member_last_seen.get(mid, '?')
            print(f"    {mid[:20]}  commits={cnt}  last_seen={last}")
        print()

    # Show last N events and first N events (for context)
    show = args.show
    if show > 0:
        if len(events) <= show * 2:
            print(f"  === ALL EVENTS ({len(events)}) ===")
            for ts_str, _, category, detail in events:
                print(f"    {ts_str} {category}: {detail}")
        else:
            print(f"  === FIRST {show} EVENTS ===")
            for ts_str, _, category, detail in events[:show]:
                print(f"    {ts_str} {category}: {detail}")
            print(f"  ... ({len(events) - show * 2} events omitted) ...")
            print(f"  === LAST {show} EVENTS ===")
            for ts_str, _, category, detail in events[-show:]:
                print(f"    {ts_str} {category}: {detail}")
    print()

    # Also scan client log for OffsetCommit response errors for this group
    if args.client:
        print("  === CLIENT-SIDE OFFSET COMMIT ERRORS ===")
        p_commit_resp = re.compile(
            r'(\d+:\d+:\d+\.\d+) .*(?:OffsetCommit|offset commit|autocommit).*'
            + re.escape(group_prefix[:16])
        )
        p_stale_err = re.compile(r'STALE_MEMBER_EPOCH|stale.member.epoch|err.*49')
        found = 0
        with open(args.client) as f:
            for line in f:
                if group_prefix[:16] not in line:
                    continue
                if 'STALE_MEMBER_EPOCH' in line or 'stale member epoch' in line.lower():
                    ts_m = p_ts.match(line)
                    ts_str = ts_m.group(1) if ts_m else '?'
                    print(f"    {ts_str} {line.strip()[:200]}")
                    found += 1
                    if found >= 50:
                        print("    ... (truncated)")
                        break
        if found == 0:
            print("    (none found)")
        print()



def main_chain_stuck(args):
    """Investigate why specific consumer group chains had zero offset commits.

    Takes group ID prefixes (short hex prefixes from test-overview or
    etl-stuck output) and investigates each group's server-side lifecycle:

    For 848 groups: consumerJoin, consumerLeave, session/rebalance timeouts,
    reconciliation events, computeTarget, RECONCILE SEND/SUMMARY.

    For classic groups: handleJoin, handleSync, handleHeartbeat, handleLeave,
    rebalance triggered, completeRebalance, session timeout.

    Also checks: OffsetCommit attempts (including STALE), fetch activity,
    produce activity for upstream topics, and client-side errors.

    Usage:
      analyze_logs.py chain-stuck --groups 7355,6174,81e9,593c,72cb,3501,23ad,f5b1,d00b
      analyze_logs.py chain-stuck --groups 7355 -v
    """
    import os

    group_prefixes = [g.strip() for g in args.groups.split(',') if g.strip()]
    if not group_prefixes:
        print("ERROR: --groups is required (comma-separated group ID prefixes)")
        return

    server_path = args.server
    client_path = args.client

    server_lines = []
    client_lines = []
    if os.path.exists(server_path):
        with open(server_path) as f:
            server_lines = f.readlines()
    if os.path.exists(client_path):
        with open(client_path) as f:
            client_lines = f.readlines()

    if not server_lines:
        print("ERROR: no server log found at", server_path)
        return

    p_ts = re.compile(r'^(\d+:\d+:\d+\.\d+)')

    def extract_ts(line):
        m = p_ts.match(line)
        return m.group(1) if m else None

    # --- Step 1: find which groups match each prefix ---
    p_group_eq = re.compile(r'group=(\S+)')
    p_group_colon = re.compile(r'group (\S+):')

    all_group_ids = set()
    for line in server_lines:
        m = p_group_eq.search(line)
        if m:
            gid = m.group(1).rstrip(',')
            if len(gid) > 4:
                all_group_ids.add(gid)
        m = p_group_colon.search(line)
        if m:
            gid = m.group(1).rstrip(':')
            if len(gid) > 4:
                all_group_ids.add(gid)

    # Map each prefix to matching full group IDs
    prefix_to_groups = {}
    for prefix in group_prefixes:
        matches = sorted(gid for gid in all_group_ids if gid.startswith(prefix))
        prefix_to_groups[prefix] = matches

    print("=== GROUP ID RESOLUTION ===")
    for prefix in group_prefixes:
        matches = prefix_to_groups[prefix]
        if not matches:
            print(f"  {prefix}: NO MATCH in server log")
        elif len(matches) == 1:
            print(f"  {prefix}: {matches[0]}")
        else:
            print(f"  {prefix}: {len(matches)} matches")
            for m in matches[:5]:
                print(f"    {m}")
    print()

    # Flatten all matched group IDs for scanning
    target_groups = {}  # full_gid => prefix
    for prefix, matches in prefix_to_groups.items():
        for gid in matches:
            target_groups[gid] = prefix

    if not target_groups:
        print("ERROR: no group IDs matched any prefix.")
        print("  Known group IDs (sample):")
        for gid in sorted(all_group_ids)[:20]:
            print(f"    {gid}")
        return

    # --- Step 2: classify each group as classic or 848 ---
    group_type = {}  # full_gid => 'classic' or '848'
    for gid in target_groups:
        group_type[gid] = 'unknown'

    p_848_join = re.compile(r'consumerJoin: group=(\S+)')
    p_classic_join = re.compile(r'group (\S+): handleJoin')

    for line in server_lines:
        m = p_848_join.search(line)
        if m:
            gid = m.group(1)
            if gid in target_groups:
                group_type[gid] = '848'
            continue
        m = p_classic_join.search(line)
        if m:
            gid = m.group(1)
            if gid in target_groups:
                group_type[gid] = 'classic'

    print("=== GROUP PROTOCOL TYPE ===")
    for gid in sorted(target_groups.keys()):
        prefix = target_groups[gid]
        print(f"  [{prefix}] {gid[:20]}... => {group_type[gid]}")
    print()

    # --- Step 3: per-group detailed event scan ---
    group_events = defaultdict(list)  # gid => [(ts, category, detail)]

    for line in server_lines:
        ts = extract_ts(line)
        if not ts:
            continue

        # Find which target group this line belongs to
        matched_gid = None
        for gid in target_groups:
            if gid in line:
                matched_gid = gid
                break
        if not matched_gid:
            for gid in target_groups:
                if gid[:16] in line:
                    matched_gid = gid
                    break
        if not matched_gid:
            continue

        # Classify the event
        category = None
        detail = line.strip()[:200]

        # 848 events
        if 'consumerJoin:' in line:
            category = '848_JOIN'
        elif 'consumerLeave:' in line:
            category = '848_LEAVE'
        elif 'consumerStaticLeave:' in line:
            category = '848_STATIC_LEAVE'
        elif 'consumerSessionTimeout:' in line:
            category = '848_SESSION_TIMEOUT'
        elif 'consumerRebalanceTimeout:' in line:
            category = '848_REBAL_TIMEOUT'
        elif 'fenceConsumer:' in line:
            category = '848_FENCE'
        elif 'computeTarget:' in line:
            category = '848_TARGET'
        elif 'RECONCILE SUMMARY:' in line:
            category = '848_RECONCILE'
        elif 'RECONCILE REVOKE-FIRST:' in line:
            category = '848_REVOKE'
        elif 'RECONCILE SEND:' in line:
            category = '848_SEND'
        elif 'pendingRevoke cleared:' in line:
            category = '848_PENDING_CLR'
        elif 'consumerHeartbeatLate:' in line:
            category = '848_LATE_HB'
        # Classic events
        elif 'handleJoin' in line:
            category = 'CLASSIC_JOIN'
        elif 'handleSync' in line:
            category = 'CLASSIC_SYNC'
        elif 'handleHeartbeat' in line:
            category = 'CLASSIC_HEARTBEAT'
        elif 'handleLeave' in line:
            category = 'CLASSIC_LEAVE'
        elif 'rebalance triggered' in line:
            category = 'CLASSIC_REBALANCE'
        elif 'completeRebalance' in line:
            category = 'CLASSIC_COMPLETE'
        elif 'session timeout' in line:
            category = 'CLASSIC_SESSION_TIMEOUT'
        elif 'rebalance timed out' in line:
            category = 'CLASSIC_REBAL_TIMEOUT'
        elif 'replaceStaticMember' in line:
            category = 'CLASSIC_STATIC_REPLACE'
        # Common events
        elif 'OffsetCommit' in line and 'STALE' in line:
            category = 'COMMIT_STALE'
        elif 'OffsetCommit' in line:
            category = 'COMMIT'
        elif 'TxnOffsetCommit' in line:
            category = 'TXN_COMMIT'
        elif 'TxnOffsetApply' in line:
            category = 'TXN_APPLY'
        elif 'TxnOffsetDiscard' in line:
            category = 'TXN_DISCARD'
        elif 'fetch:' in line:
            category = 'FETCH'
        elif 'produce:' in line:
            category = 'PRODUCE'
        else:
            category = 'OTHER'

        group_events[matched_gid].append((ts, category, detail))

    # --- Step 4: per-group summary ---
    for gid in sorted(target_groups.keys()):
        prefix = target_groups[gid]
        events = group_events[gid]
        gtype = group_type[gid]

        print(f"{'=' * 80}")
        print(f"GROUP: [{prefix}] {gid}")
        print(f"  Type: {gtype}")
        print(f"  Total events: {len(events)}")

        if not events:
            print("  NO SERVER-SIDE EVENTS FOUND")
            print()
            continue

        print(f"  Time range: {events[0][0]} - {events[-1][0]}")

        # Count by category
        counts = defaultdict(int)
        for _, cat, _ in events:
            counts[cat] += 1

        print(f"\n  Event counts:")
        for cat in sorted(counts.keys()):
            print(f"    {cat}: {counts[cat]}")

        # Show key lifecycle events (non-heartbeat, non-fetch, non-produce)
        lifecycle_cats = {
            '848_JOIN', '848_LEAVE', '848_STATIC_LEAVE', '848_SESSION_TIMEOUT',
            '848_REBAL_TIMEOUT', '848_FENCE', '848_TARGET', '848_RECONCILE',
            '848_REVOKE', '848_SEND', '848_PENDING_CLR', '848_LATE_HB',
            'CLASSIC_JOIN', 'CLASSIC_SYNC', 'CLASSIC_LEAVE',
            'CLASSIC_REBALANCE', 'CLASSIC_COMPLETE',
            'CLASSIC_SESSION_TIMEOUT', 'CLASSIC_REBAL_TIMEOUT',
            'CLASSIC_STATIC_REPLACE',
            'COMMIT', 'COMMIT_STALE', 'TXN_COMMIT', 'TXN_APPLY', 'TXN_DISCARD',
        }

        lifecycle_events = [(ts, cat, det) for ts, cat, det in events if cat in lifecycle_cats]

        if lifecycle_events:
            show_count = 50 if args.verbose else 20
            if len(lifecycle_events) <= show_count:
                label = 'all'
            else:
                label = f'first/last {show_count // 2}'
            print(f"\n  Lifecycle events ({len(lifecycle_events)} total, showing {label}):")
            if len(lifecycle_events) <= show_count:
                for ts, cat, det in lifecycle_events:
                    print(f"    {ts} {cat}: {det}")
            else:
                half = show_count // 2
                for ts, cat, det in lifecycle_events[:half]:
                    print(f"    {ts} {cat}: {det}")
                print(f"    ... ({len(lifecycle_events) - show_count} events omitted) ...")
                for ts, cat, det in lifecycle_events[-half:]:
                    print(f"    {ts} {cat}: {det}")

        # If classic, show heartbeat count and any gaps
        if gtype == 'classic' and counts.get('CLASSIC_HEARTBEAT', 0) > 0:
            hb_times = [parse_ts(ts) for ts, cat, _ in events if cat == 'CLASSIC_HEARTBEAT']
            if len(hb_times) >= 2:
                max_gap = max(hb_times[i + 1] - hb_times[i] for i in range(len(hb_times) - 1))
                print(f"\n  Heartbeat stats: {len(hb_times)} heartbeats, max gap={max_gap:.1f}s")

        # Check for rebalance loops (classic)
        if gtype == 'classic':
            join_count = counts.get('CLASSIC_JOIN', 0)
            sync_count = counts.get('CLASSIC_SYNC', 0)
            complete_count = counts.get('CLASSIC_COMPLETE', 0)
            if join_count > 0 and complete_count == 0:
                print(f"\n  ** WARNING: {join_count} JoinGroup but 0 completeRebalance - stuck in rebalance!")
            elif join_count > sync_count + 2:
                print(f"\n  ** WARNING: joins ({join_count}) >> syncs ({sync_count}) - possible rebalance loop!")

        # Check for 848 convergence issues
        if gtype == '848':
            send_count = counts.get('848_SEND', 0)
            target_count = counts.get('848_TARGET', 0)
            if send_count == 0 and target_count > 0:
                print(f"\n  ** WARNING: {target_count} computeTarget but 0 RECONCILE SEND - assignment never sent!")
            if counts.get('848_SESSION_TIMEOUT', 0) > 0:
                print(f"\n  ** WARNING: session timeout(s) detected!")
            if counts.get('848_LATE_HB', 0) > 0:
                print(f"\n  ** WARNING: {counts['848_LATE_HB']} late heartbeats!")

        # Commit summary
        commit_count = counts.get('COMMIT', 0) + counts.get('TXN_COMMIT', 0)
        if commit_count == 0:
            print(f"\n  ** ZERO OFFSET COMMITS - this group never committed!")
            fetch_count = counts.get('FETCH', 0)
            produce_count = counts.get('PRODUCE', 0)
            print(f"     Fetch events: {fetch_count}, Produce events: {produce_count}")
        else:
            stale = counts.get('COMMIT_STALE', 0)
            print(f"\n  Commits: {commit_count} (stale: {stale})")

        print()

    # --- Step 5: client-side errors for these groups ---
    if client_lines:
        print(f"{'=' * 80}")
        print("=== CLIENT-SIDE ERRORS ===")
        for prefix in group_prefixes:
            errors = []
            for line in client_lines:
                if prefix not in line:
                    continue
                low = line.lower()
                if any(kw in low for kw in [
                    'error', 'err=', 'stale_member_epoch', 'fenced',
                    'rebalance_in_progress', 'not_coordinator',
                    'coordinator_not_available', 'unknown_member',
                    'illegal_generation', 'group_authorization_failed',
                ]):
                    ts = extract_ts(line)
                    errors.append((ts or '?', line.strip()[:200]))

            if errors:
                print(f"\n  [{prefix}] {len(errors)} error lines (showing first 15):")
                for ts, detail in errors[:15]:
                    print(f"    {ts} {detail}")
            else:
                print(f"\n  [{prefix}] no client-side errors found")
        print()

    # --- Step 6: resolution summary ---
    print("=== RESOLUTION SUMMARY ===")
    for prefix in group_prefixes:
        matches = prefix_to_groups[prefix]
        if not matches:
            found_lines = 0
            sample_lines = []
            for line in server_lines:
                if prefix in line:
                    found_lines += 1
                    if len(sample_lines) < 3:
                        sample_lines.append(line.strip()[:200])
            print(f"  {prefix}: NO GROUP ID matched, but appears in {found_lines} server log lines")
            if sample_lines:
                print(f"    Sample lines:")
                for sl in sample_lines:
                    print(f"      {sl}")
        else:
            evts = sum(len(group_events[gid]) for gid in matches)
            commits = sum(
                sum(1 for _, cat, _ in group_events[gid] if cat in ('COMMIT', 'TXN_COMMIT'))
                for gid in matches
            )
            print(f"  {prefix}: {len(matches)} group(s), {evts} events, {commits} commits")
    print()

def main_test_overview(args):
    """Overview of a test run: log time ranges, test timeout, group counts.

    Shows server/client log time ranges, any test timeout or panic messages,
    and counts of classic (JoinGroup) vs 848 (ConsumerGroupHeartbeat) groups
    with per-group timing summary.
    """
    import os

    # --- Read logs ---
    server_lines = []
    client_lines = []
    if os.path.exists(args.server):
        with open(args.server) as f:
            server_lines = f.readlines()
    if os.path.exists(args.client):
        with open(args.client) as f:
            client_lines = f.readlines()

    p_ts = re.compile(r'^\[?(\d+:\d+:\d+\.\d+)')

    def extract_ts(line):
        m = p_ts.match(line)
        return m.group(1) if m else None

    # --- Log time ranges ---
    print("=== LOG TIME RANGES ===")
    for name, lines in [("server", server_lines), ("client", client_lines)]:
        if not lines:
            print(f"  {name}: (no log found)")
            continue
        first_ts = None
        last_ts = None
        for line in lines:
            ts = extract_ts(line)
            if ts:
                if first_ts is None:
                    first_ts = ts
                last_ts = ts
        print(f"  {name}: {first_ts or '?'} - {last_ts or '?'}")
    print()

    # --- Test timeout / panic ---
    print("=== TEST TIMEOUT / PANIC ===")
    timeout_lines = []
    for line in client_lines:
        low = line.lower()
        if 'test timed out' in low or 'panic:' in low:
            timeout_lines.append(line.rstrip())
    if timeout_lines:
        for tl in timeout_lines[:10]:
            print(f"  {tl}")
    else:
        print("  (no timeout or panic found)")
    print()

    # --- Classic groups ---
    p_join = re.compile(r'group (\S+): handleJoin memberID="([^"]*)"')
    p_sync = re.compile(r'group (\S+): handleSync')
    p_leave_c = re.compile(r'group (\S+): handleLeave')
    p_commit = re.compile(r'OffsetCommit: group=(\S+)')

    classic_groups = {}  # gid_prefix => {first_ts, last_ts, joins, members, commits}

    def cg(gid):
        key = gid[:16]
        if key not in classic_groups:
            classic_groups[key] = {
                'full_id': gid, 'first_ts': None, 'last_ts': None,
                'joins': 0, 'members': set(), 'commits': 0,
            }
        return classic_groups[key]

    # --- 848 groups ---
    p_848_join = re.compile(r'consumerJoin: group=(\S+)\s+member=(\S+)')
    p_848_leave = re.compile(r'consumer(?:Leave|StaticLeave): group=(\S+)\s+member=(\S+)')
    p_848_timeout = re.compile(r'consumer(?:Session|Rebalance)Timeout: group=(\S+)')

    groups_848 = {}  # gid_prefix => {first_ts, last_ts, joins, members}

    def g8(gid):
        key = gid[:16]
        if key not in groups_848:
            groups_848[key] = {
                'full_id': gid, 'first_ts': None, 'last_ts': None,
                'joins': 0, 'members': set(), 'commits': 0,
            }
        return groups_848[key]

    for line in server_lines:
        ts = extract_ts(line)

        m = p_join.search(line)
        if m:
            gid, mid = m.group(1), m.group(2)
            s = cg(gid)
            if mid:
                s['members'].add(mid[:20])
            s['joins'] += 1
            if ts and not s['first_ts']:
                s['first_ts'] = ts
            if ts:
                s['last_ts'] = ts
            continue

        m = p_leave_c.search(line)
        if m:
            gid = m.group(1)
            s = cg(gid)
            if ts:
                s['last_ts'] = ts
            continue

        m = p_commit.search(line)
        if m:
            gid = m.group(1)
            # Could be classic or 848 - check if group is known as 848
            key = gid[:16]
            if key in groups_848:
                groups_848[key]['commits'] += 1
                if ts:
                    groups_848[key]['last_ts'] = ts
            elif key in classic_groups:
                classic_groups[key]['commits'] += 1
                if ts:
                    classic_groups[key]['last_ts'] = ts
            # If unknown, skip - will be attributed on second pass or not at all
            continue

        m = p_848_join.search(line)
        if m:
            gid, mid = m.group(1), m.group(2)
            s = g8(gid)
            s['members'].add(mid[:20])
            s['joins'] += 1
            if ts and not s['first_ts']:
                s['first_ts'] = ts
            if ts:
                s['last_ts'] = ts
            continue

        m = p_848_leave.search(line)
        if m:
            gid = m.group(1)
            s = g8(gid)
            if ts:
                s['last_ts'] = ts
            continue

        m = p_848_timeout.search(line)
        if m:
            gid = m.group(1)
            s = g8(gid)
            if ts:
                s['last_ts'] = ts
            continue

    # --- Print classic groups ---
    print(f"=== CLASSIC GROUPS ({len(classic_groups)}) ===")
    if classic_groups:
        print(f"  {'GROUP':<18} {'MEM':>3} {'JOIN':>4} {'COMMIT':>6} {'FIRST':>15} {'LAST':>15}  DURATION")
        print(f"  {'-'*90}")
        for key in sorted(classic_groups.keys()):
            s = classic_groups[key]
            dur = ''
            if s['first_ts'] and s['last_ts']:
                try:
                    from datetime import datetime
                    fmt = '%H:%M:%S.%f'
                    t0 = datetime.strptime(s['first_ts'], fmt)
                    t1 = datetime.strptime(s['last_ts'], fmt)
                    delta = (t1 - t0).total_seconds()
                    dur = f"{delta:.1f}s"
                except:
                    dur = '?'
            print(f"  {key:<18} {len(s['members']):>3} {s['joins']:>4} {s['commits']:>6} {s['first_ts'] or '?':>15} {s['last_ts'] or '?':>15}  {dur}")
    else:
        print("  (none)")
    print()

    # --- Print 848 groups ---
    print(f"=== 848 GROUPS ({len(groups_848)}) ===")
    if groups_848:
        print(f"  {'GROUP':<18} {'MEM':>3} {'JOIN':>4} {'COMMIT':>6} {'FIRST':>15} {'LAST':>15}  DURATION")
        print(f"  {'-'*90}")
        for key in sorted(groups_848.keys()):
            s = groups_848[key]
            dur = ''
            if s['first_ts'] and s['last_ts']:
                try:
                    from datetime import datetime
                    fmt = '%H:%M:%S.%f'
                    t0 = datetime.strptime(s['first_ts'], fmt)
                    t1 = datetime.strptime(s['last_ts'], fmt)
                    delta = (t1 - t0).total_seconds()
                    dur = f"{delta:.1f}s"
                except:
                    dur = '?'
            print(f"  {key:<18} {len(s['members']):>3} {s['joins']:>4} {s['commits']:>6} {s['first_ts'] or '?':>15} {s['last_ts'] or '?':>15}  {dur}")
    else:
        print("  (none)")
    print()

    # --- Summary ---
    total = len(classic_groups) + len(groups_848)
    print(f"=== SUMMARY ===")
    print(f"  Total groups: {total} (classic: {len(classic_groups)}, 848: {len(groups_848)})")


if __name__ == '__main__':
    main()
