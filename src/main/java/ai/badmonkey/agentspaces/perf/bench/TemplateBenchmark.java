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

import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.Template;
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

import java.util.concurrent.TimeUnit;

/**
 * Template matching cost. Reads, takes, and notify routing all evaluate
 * templates against candidate entries, and a take under contention evaluates
 * one per candidate per poll, so matching cost bounds how large a space a
 * selective consumer can afford to scan.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class TemplateBenchmark {

    /** The candidate entry type. */
    public record WorkItem(String topic, int priority, String region) {
    }

    private Template<WorkItem> typeOnly;
    private Template<WorkItem> twoFields;
    private WorkItem hit;
    private WorkItem miss;

    /** Builds templates and matching plus non-matching candidates. */
    @Setup
    public void setup() {
        typeOnly = Template.of(WorkItem.class);
        twoFields = Template.of(WorkItem.class)
                .where("topic", Matchers.eq("indexing"))
                .where("priority", Matchers.gte(5));
        hit = new WorkItem("indexing", 7, "us-east");
        miss = new WorkItem("billing", 2, "eu-west");
    }

    /** Type-only match, the cheapest template. */
    @Benchmark
    public boolean matchTypeOnly() {
        return typeOnly.matches(hit);
    }

    /** Two-matcher template against a matching candidate. */
    @Benchmark
    public boolean matchHit() {
        return twoFields.matches(hit);
    }

    /** Two-matcher template against a candidate that fails the first matcher. */
    @Benchmark
    public boolean matchMiss() {
        return twoFields.matches(miss);
    }

    /** Building a two-matcher template from scratch. */
    @Benchmark
    public Template<WorkItem> build() {
        return Template.of(WorkItem.class)
                .where("topic", Matchers.eq("indexing"))
                .where("priority", Matchers.gte(5));
    }
}
