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
package ai.badmonkey.agentspaces.perf;

import ai.badmonkey.agentspaces.perf.bench.TemplateBenchmark;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the JMH toolchain works end to end: the annotation processor
 * generated the benchmark list, the runner finds a benchmark by name, and one
 * tiny in-process measurement completes with a sane score. Real measurements
 * come from {@link BenchmarkRunner}, with forking on.
 */
class BenchmarkSmokeTest {

    @Test
    void generatedBenchmarksRunInProcess() throws Exception {
        Options options = new OptionsBuilder()
                .include(TemplateBenchmark.class.getSimpleName() + "\\.matchHit$")
                .forks(0)
                .warmupIterations(0)
                .measurementIterations(1)
                .measurementTime(TimeValue.milliseconds(100))
                .build();

        Collection<RunResult> results = new Runner(options).run();

        assertThat(results).hasSize(1);
        RunResult result = results.iterator().next();
        assertThat(result.getPrimaryResult().getScore()).isGreaterThan(0.0);
    }
}
