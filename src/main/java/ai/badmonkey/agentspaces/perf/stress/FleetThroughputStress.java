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
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end task throughput under contention: one producer floods the space,
 * workers on every peer race to take, and each take waits out the LEASE_RACE
 * settle window and rides gossip replication, so the numbers cover the whole
 * fabric rather than a lone data structure. Latency runs from the producer's
 * write to the winning worker's take.
 *
 * <p>Run: {@code mvn exec:java
 * -Dexec.mainClass=ai.badmonkey.agentspaces.perf.stress.FleetThroughputStress
 * -Dpeers=3 -Dworkers=2 -Dtasks=200 -DsettleMs=100}
 */
public final class FleetThroughputStress {

    /** A produced task, born at a nanosecond stamp for latency measurement. */
    public record StressTask(String id, long bornNanos) {
    }

    /**
     * The run's results.
     *
     * @param completed    tasks completed before the deadline
     * @param elapsed      wall time from first write to last completion
     * @param takeLatency  producer-write-to-worker-take latency summary
     * @param takesPerPeer completed takes per peer index
     */
    public record Report(int completed, Duration elapsed, Latencies.Summary takeLatency,
                         List<Integer> takesPerPeer) {
    }

    private FleetThroughputStress() {
    }

    /**
     * Runs the stress with the given shape.
     *
     * @param peerCount      peers in the fleet
     * @param workersPerPeer take loops per peer
     * @param taskCount      tasks the producer writes
     * @param settleWindow   the LEASE_RACE settle window
     * @param deadline       hard stop for the whole run
     * @return the report
     * @throws Exception when the fleet fails to start
     */
    public static Report run(int peerCount, int workersPerPeer, int taskCount,
                             Duration settleWindow, Duration deadline) throws Exception {
        try (MiniFleet fleet = MiniFleet.start("throughput-stress", peerCount, settleWindow)) {
            Thread.sleep(1500);

            Latencies latencies = new Latencies();
            AtomicInteger completed = new AtomicInteger();
            List<AtomicInteger> perPeer = new ArrayList<>();
            ConcurrentHashMap<String, Boolean> takenIds = new ConcurrentHashMap<>();
            CountDownLatch done = new CountDownLatch(1);
            List<Thread> workers = new ArrayList<>();
            Template<StressTask> template = Template.of(StressTask.class);
            Lease takeLease = Lease.of(Duration.ofMinutes(1));

            for (int p = 0; p < peerCount; p++) {
                AtomicInteger peerTakes = new AtomicInteger();
                perPeer.add(peerTakes);
                MiniFleet.Peer peer = fleet.peer(p);
                for (int w = 0; w < workersPerPeer; w++) {
                    Thread worker = Thread.ofVirtual()
                            .name("stress-worker-" + p + "-" + w)
                            .start(() -> {
                                while (!Thread.currentThread().isInterrupted()
                                        && completed.get() < taskCount) {
                                    Optional<TakenEntry<StressTask>> taken;
                                    try {
                                        taken = peer.space().take(template, takeLease,
                                                Duration.ofMillis(500));
                                    } catch (RuntimeException e) {
                                        break; // space closed at run end
                                    }
                                    if (taken.isEmpty()) {
                                        continue;
                                    }
                                    StressTask task = taken.get().entry();
                                    if (takenIds.putIfAbsent(task.id(), true) != null) {
                                        continue; // a lease lapse re-exposed a counted task
                                    }
                                    latencies.record(System.nanoTime() - task.bornNanos());
                                    peerTakes.incrementAndGet();
                                    peer.space().complete(taken.get());
                                    if (completed.incrementAndGet() >= taskCount) {
                                        done.countDown();
                                    }
                                }
                            });
                    workers.add(worker);
                }
            }

            long startNanos = System.nanoTime();
            Lease writeLease = Lease.of(Duration.ofHours(1));
            for (int i = 0; i < taskCount; i++) {
                fleet.peer(0).space().write(
                        new StressTask("task-" + i, System.nanoTime()), writeLease);
            }

            done.await(deadline.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
            workers.forEach(Thread::interrupt);
            for (Thread worker : workers) {
                worker.join(2000);
            }
            List<Integer> takes = perPeer.stream().map(AtomicInteger::get).toList();
            return new Report(completed.get(), elapsed, latencies.summarize(), takes);
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
        int workers = Integer.getInteger("workers", 2);
        int tasks = Integer.getInteger("tasks", 200);
        Duration settle = Duration.ofMillis(Integer.getInteger("settleMs", 100));

        System.out.printf("fleet throughput: %d peers x %d workers, %d tasks, settle %dms%n",
                peers, workers, tasks, settle.toMillis());
        Report report = run(peers, workers, tasks, settle, Duration.ofMinutes(3));

        double seconds = report.elapsed().toNanos() / 1_000_000_000.0;
        System.out.printf("completed  %d/%d in %.1fs (%.1f tasks/s)%n",
                report.completed(), tasks, seconds, report.completed() / seconds);
        System.out.println("take latency  " + report.takeLatency());
        for (int p = 0; p < report.takesPerPeer().size(); p++) {
            System.out.printf("peer %d takes  %d%n", p, report.takesPerPeer().get(p));
        }
    }
}
