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
import ai.badmonkey.agentspaces.api.space.Template;

import java.time.Duration;

/**
 * Recovery after churn: the fleet loses a peer, keeps writing, and a fresh
 * peer then joins from nothing. The measurement is anti-entropy catch-up
 * time, from the newcomer's join to a full replica of every live entry, which
 * is the cost a fleet pays every time an instance restarts or scales up.
 *
 * <p>Run: {@code mvn exec:java
 * -Dexec.mainClass=ai.badmonkey.agentspaces.perf.stress.ChurnRecoveryStress
 * -Dpeers=3 -Dentries=100}
 */
public final class ChurnRecoveryStress {

    /** The replicated entry. */
    public record Ledger(String id, int seq) {
    }

    /**
     * The run's results.
     *
     * @param liveEntries entries alive when the newcomer joined
     * @param caughtUp    whether the newcomer reached a full replica in time
     * @param catchUp     join-to-full-replica duration
     */
    public record Report(int liveEntries, boolean caughtUp, Duration catchUp) {
    }

    private ChurnRecoveryStress() {
    }

    /**
     * Runs the stress with the given shape.
     *
     * @param peerCount      peers at the start; one dies mid-run
     * @param entriesPerWave entries written before and again after the death
     * @return the report
     * @throws Exception when the fleet fails to start
     */
    public static Report run(int peerCount, int entriesPerWave) throws Exception {
        try (MiniFleet fleet = MiniFleet.start("churn-stress", peerCount,
                Duration.ofMillis(100))) {
            Thread.sleep(1500);

            Lease lease = Lease.of(Duration.ofHours(1));
            Template<Ledger> all = Template.of(Ledger.class);
            int total = 2 * entriesPerWave;

            for (int i = 0; i < entriesPerWave; i++) {
                fleet.peer(0).space().write(new Ledger("wave1-" + i, i), lease);
            }
            awaitCount(fleet, 1, entriesPerWave, Duration.ofSeconds(30));

            fleet.stopPeer(fleet.size() - 1);
            for (int i = 0; i < entriesPerWave; i++) {
                fleet.peer(0).space().write(new Ledger("wave2-" + i, i), lease);
            }
            awaitCount(fleet, Math.min(1, fleet.size() - 1), total, Duration.ofSeconds(30));

            long joinNanos = System.nanoTime();
            MiniFleet.Peer newcomer = fleet.addPeer();
            long deadline = joinNanos + Duration.ofSeconds(120).toNanos();
            boolean caughtUp = false;
            while (!caughtUp && System.nanoTime() < deadline) {
                caughtUp = newcomer.space().readAll(all, total + 1).size() >= total;
                if (!caughtUp) {
                    Thread.sleep(20);
                }
            }
            Duration catchUp = Duration.ofNanos(System.nanoTime() - joinNanos);
            return new Report(total, caughtUp, catchUp);
        }
    }

    private static void awaitCount(MiniFleet fleet, int peerIndex, int expected,
                                   Duration timeout) throws InterruptedException {
        Template<Ledger> all = Template.of(Ledger.class);
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (fleet.peer(peerIndex).space().readAll(all, expected + 1).size() >= expected) {
                return;
            }
            Thread.sleep(20);
        }
    }

    /**
     * Command-line entry point; every knob is a system property.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        int peers = Integer.getInteger("peers", 3);
        int entries = Integer.getInteger("entries", 100);

        System.out.printf("churn recovery: %d peers, %d entries per wave%n", peers, entries);
        Report report = run(peers, entries);
        System.out.printf("live entries  %d%n", report.liveEntries());
        System.out.printf("caught up     %s in %.1fs%n", report.caughtUp(),
                report.catchUp().toNanos() / 1_000_000_000.0);
    }
}
