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

import java.io.IOException;

/**
 * Entry point for the JMH benchmark suite, and the main class of
 * {@code target/benchmarks.jar}. Delegates to the standard JMH launcher, so
 * every JMH command-line option works unchanged:
 *
 * <pre>{@code
 * java -jar target/benchmarks.jar             # everything
 * java -jar target/benchmarks.jar Cbor        # one family
 * java -jar target/benchmarks.jar -lprof      # list profilers
 * }</pre>
 *
 * <p>Run it from the jar, not {@code mvn exec:java}: JMH forks fresh JVMs that
 * inherit the jar's classpath, which a Maven-hosted run cannot pass on.
 */
public final class BenchmarkRunner {

    private BenchmarkRunner() {
    }

    /**
     * Runs the JMH launcher with the given arguments.
     *
     * @param args JMH command-line arguments (benchmark regex, options)
     * @throws IOException when JMH cannot read its generated benchmark list
     */
    public static void main(String[] args) throws IOException {
        org.openjdk.jmh.Main.main(args);
    }
}
