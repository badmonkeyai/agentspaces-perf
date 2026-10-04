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

import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.time.InstantSource;
import java.util.concurrent.TimeUnit;

/**
 * Hybrid logical clock cost. Every write, every envelope, and every lease
 * transition takes a timestamp, and every received frame merges one in, so the
 * clock sits on the fabric's hottest path. The clock synchronizes internally,
 * so the numbers here include that lock.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class HlcBenchmark {

    private HybridLogicalClock clock;
    private HlcTimestamp remote;
    private HlcTimestamp parseSample;
    private String encodedSample;

    /** Builds the clock and a plausible remote timestamp. */
    @Setup
    public void setup() {
        clock = new HybridLogicalClock(InstantSource.system(), "bench-node");
        remote = new HlcTimestamp(System.currentTimeMillis(), 3, "remote-node");
        parseSample = clock.now();
        encodedSample = parseSample.encoded();
    }

    /** Takes a local timestamp. */
    @Benchmark
    public HlcTimestamp now() {
        return clock.now();
    }

    /** Merges a remote timestamp, the receive-side operation. */
    @Benchmark
    public HlcTimestamp update() {
        return clock.update(remote);
    }

    /** Encodes a timestamp to its wire string. */
    @Benchmark
    public String encode() {
        return parseSample.encoded();
    }

    /** Parses a timestamp from its wire string. */
    @Benchmark
    public HlcTimestamp parse() {
        return HlcTimestamp.parse(encodedSample);
    }
}
