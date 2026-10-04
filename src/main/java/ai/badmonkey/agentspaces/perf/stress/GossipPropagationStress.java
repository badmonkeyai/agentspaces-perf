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

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.Template;

import java.time.Duration;

/**
 * Write visibility across the fleet: each entry written at one peer is polled
 * for at every other peer, and the sample is the time until the slowest peer
 * sees it. This measures the push channel's rumor spread end to end, including
 * signing, TCP delivery, and replica apply, and it is the number behind the
 * question "how stale can a reader be".
 *
 * <p>Run: {@code mvn exec:java
 * -Dexec.mainClass=ai.badmonkey.agentspaces.perf.stress.GossipPropagationStress
 * -Dpeers=4 -Dentries=50}
 */
public final class GossipPropagationStress {

    /** The propagated entry. */
    public record Bulletin(String id, int seq) {
    }

    /**
     * The run's results.
     *
     * @param propagated  entries every peer saw before the per-entry deadline
     * @param propagation write-to-slowest-peer latency summary
     */
    public record Report(int propagated, Latencies.Summary propagation) {
    }

    private GossipPropagationStress() {
    }

    /**
     * Runs the stress with the given shape.
     *
     * @param peerCount  peers in the fleet
     * @param entryCount entries to propagate, one at a time
     * @return the report
     * @throws Exception when the fleet fails to start
     */
    public static Report run(int peerCount, int entryCount) throws Exception {
        try (MiniFleet fleet = MiniFleet.start("propagation-stress", peerCount,
                Duration.ofMillis(100))) {
            Thread.sleep(1500);

            Latencies latencies = new Latencies();
            Lease lease = Lease.of(Duration.ofHours(1));
            int propagated = 0;

            for (int i = 0; i < entryCount; i++) {
                String id = "bulletin-" + i;
                Template<Bulletin> template = Template.of(Bulletin.class)
                        .where("id", Matchers.eq(id));
                long born = System.nanoTime();
                fleet.peer(0).space().write(new Bulletin(id, i), lease);

                long deadline = born + Duration.ofSeconds(30).toNanos();
                boolean everywhere = false;
                while (!everywhere && System.nanoTime() < deadline) {
                    everywhere = true;
                    for (int p = 1; p < peerCount; p++) {
                        if (fleet.peer(p).space().read(template).isEmpty()) {
                            everywhere = false;
                            break;
                        }
                    }
                    if (!everywhere) {
                        Thread.sleep(2);
                    }
                }
                if (everywhere) {
                    latencies.record(System.nanoTime() - born);
                    propagated++;
                }
            }
            return new Report(propagated, latencies.summarize());
        }
    }

    /**
     * Command-line entry point; every knob is a system property.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        int peers = Integer.getInteger("peers", 4);
        int entries = Integer.getInteger("entries", 50);

        System.out.printf("gossip propagation: %d peers, %d entries%n", peers, entries);
        Report report = run(peers, entries);
        System.out.printf("propagated  %d/%d%n", report.propagated(), entries);
        System.out.println("write-to-everywhere  " + report.propagation());
    }
}
