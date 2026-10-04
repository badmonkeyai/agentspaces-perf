/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.perf.bench;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.crdt.EntryState;
import ai.badmonkey.agentspaces.space.crdt.LwwRegister;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.space.replicated.TakeClaim;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * CRDT merge cost, the receive side of anti-entropy. A DIGEST round that pulls
 * deltas ends in {@link SpaceStateCrdt#merge}, so merge cost against realistic
 * space sizes bounds how fast a replica can catch up; the per-entry and
 * per-claim merges below are the inner loops of that work.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class CrdtBenchmark {

    /** Entries per replica state; the merged result holds 1.5x this. */
    @Param({"100", "1000", "10000"})
    public int entries;

    private SpaceStateCrdt left;
    private SpaceStateCrdt right;
    private EntryState entryA;
    private EntryState entryB;
    private TakeClaim claimA;
    private TakeClaim claimB;

    /**
     * Builds two replica states sharing half their entries, plus single-entry
     * and single-claim pairs.
     */
    @Setup
    public void setup() {
        AgentId issuer = new AgentId(PeerId.of("zBenchPeer1111"), "bench");
        AgentId rival = new AgentId(PeerId.of("zBenchPeer2222"), "rival");
        SpaceId spaceId = SpaceId.local("bench");
        long now = System.currentTimeMillis();

        left = SpaceStateCrdt.empty();
        right = SpaceStateCrdt.empty();
        for (int i = 0; i < entries; i++) {
            EntryRecord record = record(spaceId, issuer, now, i);
            LwwRegister<LeaseInfo> lease = new LwwRegister<>(
                    new HlcTimestamp(now, i, "left"),
                    new LeaseInfo(issuer, now + 3_600_000, LeaseKind.WRITE));
            // The lower half is shared history; the upper half diverges.
            if (i < entries / 2) {
                left = left.add(record, new Dot("left", i + 1), lease);
                right = right.add(record, new Dot("left", i + 1), lease);
            } else if (i % 2 == 0) {
                left = left.add(record, new Dot("left", i + 1), lease);
            } else {
                right = right.add(record, new Dot("right", i + 1), lease);
            }
        }

        EntryRecord shared = record(spaceId, issuer, now, -1);
        LwwRegister<LeaseInfo> leaseA = new LwwRegister<>(
                new HlcTimestamp(now, 1, "left"),
                new LeaseInfo(issuer, now + 3_600_000, LeaseKind.WRITE));
        LwwRegister<LeaseInfo> leaseB = new LwwRegister<>(
                new HlcTimestamp(now + 5, 1, "right"),
                new LeaseInfo(rival, now + 3_600_000, LeaseKind.TAKE));
        entryA = EntryState.added(shared, new Dot("left", 1), leaseA);
        entryB = EntryState.added(shared, new Dot("right", 1), leaseB);

        claimA = new TakeClaim(shared.entryId(), spaceId, 3,
                new HlcTimestamp(now, 1, "left"), issuer, 4.0, now + 600_000);
        claimB = new TakeClaim(shared.entryId(), spaceId, 3,
                new HlcTimestamp(now, 2, "right"), rival, 2.5, now + 600_000);
    }

    private static EntryRecord record(SpaceId spaceId, AgentId issuer, long now, int i) {
        return new EntryRecord(
                EntryId.of("bench-" + i), spaceId, "bench.Doc",
                ("payload-" + i).getBytes(StandardCharsets.UTF_8), null,
                issuer, new HlcTimestamp(now, 0, "left"),
                new LeaseInfo(issuer, now + 3_600_000, LeaseKind.WRITE),
                Map.of(), null);
    }

    /** Full-state merge of two replicas, the anti-entropy end game. */
    @Benchmark
    public SpaceStateCrdt spaceMerge() {
        return left.merge(right);
    }

    /** Merge of one entry's diverged state, the per-delta inner loop. */
    @Benchmark
    public EntryState entryMerge() {
        return entryA.merge(entryB);
    }

    /** Take-claim arbitration, run on every competing claim during a settle window. */
    @Benchmark
    public TakeClaim claimMerge() {
        return TakeClaim.merge(claimA, claimB);
    }
}
