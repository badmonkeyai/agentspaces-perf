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
package ai.badmonkey.agentspaces.perf.stress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A thread-safe latency recorder with percentile summaries. Samples are
 * nanoseconds from {@link System#nanoTime()}; the summary reports
 * milliseconds.
 */
public final class Latencies {

    /**
     * A percentile summary in milliseconds.
     *
     * @param count sample count
     * @param p50   median
     * @param p95   95th percentile
     * @param p99   99th percentile
     * @param max   worst sample
     */
    public record Summary(int count, double p50, double p95, double p99, double max) {

        @Override
        public String toString() {
            return String.format("n=%d p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms",
                    count, p50, p95, p99, max);
        }
    }

    private final List<Long> samples = Collections.synchronizedList(new ArrayList<>());

    /**
     * Records one sample.
     *
     * @param nanos elapsed nanoseconds
     */
    public void record(long nanos) {
        samples.add(nanos);
    }

    /** Returns how many samples were recorded. */
    public int count() {
        return samples.size();
    }

    /** Returns the percentile summary of everything recorded so far. */
    public Summary summarize() {
        List<Long> sorted;
        synchronized (samples) {
            sorted = new ArrayList<>(samples);
        }
        if (sorted.isEmpty()) {
            return new Summary(0, 0, 0, 0, 0);
        }
        Collections.sort(sorted);
        return new Summary(sorted.size(),
                millis(percentile(sorted, 0.50)),
                millis(percentile(sorted, 0.95)),
                millis(percentile(sorted, 0.99)),
                millis(sorted.get(sorted.size() - 1)));
    }

    private static long percentile(List<Long> sorted, double q) {
        int index = (int) Math.ceil(q * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000.0;
    }
}
