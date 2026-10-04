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

import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Single-node space verbs. The local space backs every replicated space, so
 * these numbers give the per-verb floor before replication cost. The write and
 * take cycles remove what they add, which keeps the space at a stable size for
 * the whole run; the read benchmarks scan a preloaded population instead.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class LocalSpaceBenchmark {

    /** The entry type cycled through write, take, and complete. */
    public record CycleDoc(String topic, int seq) {
    }

    /** The entry type preloaded for read scans. */
    public record PoolDoc(String topic, int rank) {
    }

    /** A type with one lone entry among the population, for the type-index case. */
    public record RareDoc(String id) {
    }

    /** Preloaded entries the read benchmarks scan over. */
    @Param({"100", "1000", "10000"})
    public int population;

    private LocalSpace space;
    private Lease writeLease;
    private Lease takeLease;
    private Template<CycleDoc> cycleTemplate;
    private Template<PoolDoc> selectiveRead;
    private Template<PoolDoc> broadRead;
    private Template<RareDoc> rareRead;

    /** Builds the space and preloads the read population. */
    @Setup
    public void setup() {
        AgentId issuer = new AgentId(PeerId.of("zBenchPeer1111"), "bench");
        space = LocalSpace.builder("bench", issuer).build();
        writeLease = Lease.of(Duration.ofHours(1));
        takeLease = Lease.of(Duration.ofMinutes(10));
        cycleTemplate = Template.of(CycleDoc.class);
        selectiveRead = Template.of(PoolDoc.class)
                .where("rank", Matchers.eq(population / 2));
        broadRead = Template.of(PoolDoc.class);
        for (int i = 0; i < population; i++) {
            space.write(new PoolDoc("pool-" + (i % 16), i), writeLease);
        }
        space.write(new RareDoc("rare-1"), writeLease);
        rareRead = Template.of(RareDoc.class);
    }

    /** Closes the space. */
    @TearDown
    public void tearDown() {
        space.close();
    }

    /** Write followed by cancel, isolating write-path cost at stable size. */
    @Benchmark
    public EntryHandle writeCancel() {
        EntryHandle handle = space.write(new CycleDoc("cycle", 1), writeLease);
        handle.cancel();
        return handle;
    }

    /** The full work loop: write, take under lease, complete. */
    @Benchmark
    public Optional<TakenEntry<CycleDoc>> writeTakeComplete() {
        space.write(new CycleDoc("cycle", 1), writeLease);
        Optional<TakenEntry<CycleDoc>> taken =
                space.take(cycleTemplate, takeLease, Duration.ZERO);
        taken.ifPresent(space::complete);
        return taken;
    }

    /** Selective read: one matcher narrows the population to a single entry. */
    @Benchmark
    public Optional<PoolDoc> readSelective() {
        return space.read(selectiveRead);
    }

    /** Broad scan: readAll over the whole population, capped at 100. */
    @Benchmark
    public List<PoolDoc> readAll100() {
        return space.readAll(broadRead, 100);
    }

    /**
     * Read of a type with one entry among the population: with the
     * type-partitioned index the matcher never sees the other types, so the
     * residual population effect is the expiry sweep alone.
     */
    @Benchmark
    public Optional<RareDoc> readRareType() {
        return space.read(rareRead);
    }
}
