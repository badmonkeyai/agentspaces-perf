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

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.Fetch;
import ai.badmonkey.agentspaces.content.FilePieceStore;
import ai.badmonkey.agentspaces.content.PublishOptions;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * The content swarm under load (srad/large-networks-bulk-data.md LB-O-08): one peer
 * publishes an artifact from a file, every other peer fetches it at once over real TCP,
 * and the report says how fast the swarm moved it and how much of each fetcher's bytes
 * came from the seeder as against the other fetchers. The swarm effect is that share:
 * with one holder every byte comes from it; with eight peers most come from each other.
 *
 * <pre>
 * mvn exec:java -Dexec.mainClass=ai.badmonkey.agentspaces.perf.stress.ContentSwarmStress \
 *   -Dpeers=8 -Dmebibytes=1024 -DpieceKiB=1024 -Dbinding=tcp -DdeadlineSeconds=1800
 * </pre>
 */
public final class ContentSwarmStress {

    /** One fetcher's outcome. */
    public record FetcherReport(int peer, boolean complete, long millis, long bytesFromSeeder, long bytesFromPeers) {
    }

    /**
     * The run's results.
     *
     * @param bytes           the artifact size
     * @param pieceSize       the piece size
     * @param peers           peers in the fleet, the seeder included
     * @param elapsed         wall time from the first fetch to the last completion
     * @param mebibytesPerSec aggregate bytes delivered to fetchers per second of wall time
     * @param seederShare     the fraction of all delivered bytes the seeder served
     * @param fetchers        per fetcher
     */
    public record Report(long bytes, int pieceSize, int peers, String binding, Duration elapsed, double mebibytesPerSec,
                         double seederShare, List<FetcherReport> fetchers) {
        @Override
        public String toString() {
            StringBuilder out = new StringBuilder(String.format(
                    "content swarm: %d MiB in %d KiB pieces over %d peers on %s: %d ms, %.1f MiB/s aggregate, seeder share %.0f%%%n",
                    bytes / (1024 * 1024), pieceSize / 1024, peers, binding, elapsed.toMillis(), mebibytesPerSec, seederShare * 100));
            for (FetcherReport f : fetchers) {
                out.append(String.format("  peer %d: %s in %d ms, %d KiB from the seeder, %d KiB from peers%n", f.peer(),
                        f.complete() ? "complete" : "INCOMPLETE", f.millis(), f.bytesFromSeeder() / 1024, f.bytesFromPeers() / 1024));
            }
            return out.toString();
        }
    }

    private record Member(MiniFleet.Peer peer, Content content, ReplicatedSpace contentSpace) {
    }

    private ContentSwarmStress() {
    }

    /**
     * Runs the stress.
     *
     * @param peerCount peers, the seeder included (at least 2)
     * @param bytes     the artifact size
     * @param pieceSize the piece size, a power of two in [64 KiB, 4 MiB]
     * @param deadline  the hard stop
     * @return the report
     * @throws Exception when the fleet fails to start
     */
    public static Report run(int peerCount, long bytes, int pieceSize, Duration deadline) throws Exception {
        return run(peerCount, bytes, pieceSize, "tcp", deadline);
    }

    /**
     * Runs the stress on a binding.
     *
     * @param peerCount peers, the seeder included (at least 2)
     * @param bytes     the artifact size
     * @param pieceSize the piece size, a power of two in [64 KiB, 4 MiB]
     * @param binding   {@code tcp} or {@code tls}
     * @param deadline  the hard stop
     * @return the report
     * @throws Exception when the fleet fails to start
     */
    public static Report run(int peerCount, long bytes, int pieceSize, String binding, Duration deadline) throws Exception {
        if (peerCount < 2) {
            throw new IllegalArgumentException("at least two peers: a seeder and a fetcher");
        }
        CborCodec codec = CborCodec.defaultCodec();
        Path root = Files.createTempDirectory("content-stress");
        try (MiniFleet fleet = MiniFleet.start("content-stress", peerCount, Duration.ofMillis(100), binding)) {
            List<Member> members = new ArrayList<>();
            for (int i = 0; i < peerCount; i++) {
                MiniFleet.Peer peer = fleet.peer(i);
                BlockExchange blocks = new BlockExchange(peer.runtime(), codec);
                ReplicatedSpace contentSpace = ReplicatedSpace
                        .builder(peer.runtime(), "content", peer.identity(), "content")
                        .settleWindow(Duration.ofMillis(100)).blocks(blocks).build();
                Content content = Content.builder(new CapabilityPipes(peer.runtime(), codec), peer.runtime(),
                                contentSpace, peer.identity().peerId())
                        .codec(codec).store(new FilePieceStore(root.resolve("peer-" + i), codec, InstantSource.system(), 0))
                        .blocks(blocks).random(new Random(i)).build();
                peer.runtime().onTick(content::tick);
                content.start();
                members.add(new Member(peer, content, contentSpace));
            }
            Thread.sleep(1500);

            Path artifact = root.resolve("artifact.bin");
            writeRandom(artifact, bytes, 29);
            Member seeder = members.get(0);
            Manifest manifest = seeder.content().publish(artifact,
                    PublishOptions.defaults().withPieceSize(pieceSize).withName("artifact.bin"));

            long start = System.nanoTime();
            List<Fetch> fetches = new ArrayList<>();
            List<Long> starts = new ArrayList<>();
            for (int i = 1; i < peerCount; i++) {
                fetches.add(members.get(i).content().fetch(manifest.root()));
                starts.add(System.nanoTime());
            }
            // a progress line every ten seconds, and the trace tail of any fetch that stalls for thirty
            Thread watcher = Thread.ofPlatform().daemon().name("content-stress-watch").start(() -> {
                int[] last = new int[fetches.size()];
                long[] since = new long[fetches.size()];
                java.util.Arrays.fill(since, System.nanoTime());
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Thread.sleep(10_000);
                        StringBuilder line = new StringBuilder("  progress:");
                        for (int i = 0; i < fetches.size(); i++) {
                            Fetch.Progress p = fetches.get(i).progress();
                            line.append(String.format(" %d/%d", p.piecesVerified(), p.pieces()));
                            if (p.piecesVerified() != last[i]) {
                                last[i] = p.piecesVerified();
                                since[i] = System.nanoTime();
                            } else if (!fetches.get(i).complete()
                                    && System.nanoTime() - since[i] > TimeUnit.SECONDS.toNanos(30)) {
                                List<String> trace = fetches.get(i).trace();
                                System.out.printf("  peer %d stalled at %d/%d; trace tail:%n    %s%n", i + 1,
                                        p.piecesVerified(), p.pieces(),
                                        String.join("\n    ", trace.subList(Math.max(0, trace.size() - 15), trace.size())));
                                since[i] = System.nanoTime();
                            }
                        }
                        System.out.println(line);
                    }
                } catch (InterruptedException ignored) {
                    // the run ended
                }
            });
            long end = start;
            List<FetcherReport> reports = new ArrayList<>();
            long fromSeeder = 0;
            long delivered = 0;
            PeerId seederId = seeder.peer().identity().peerId();
            long deadlineNanos = start + deadline.toNanos();
            for (int i = 0; i < fetches.size(); i++) {
                Fetch fetch = fetches.get(i);
                long remaining = Math.max(1, deadlineNanos - System.nanoTime());
                boolean complete = fetch.await(Duration.ofNanos(remaining));
                long done = System.nanoTime();
                end = Math.max(end, done);
                Map<PeerId, Long> bySource = fetch.progress().bytesBySource();
                long seederBytes = bySource.getOrDefault(seederId, 0L);
                long total = bySource.values().stream().mapToLong(Long::longValue).sum();
                fromSeeder += seederBytes;
                delivered += total;
                reports.add(new FetcherReport(i + 1, complete, TimeUnit.NANOSECONDS.toMillis(done - starts.get(i)),
                        seederBytes, total - seederBytes));
            }
            watcher.interrupt();
            Duration elapsed = Duration.ofNanos(Math.max(1, end - start));
            double mebibytesPerSec = delivered / (1024.0 * 1024.0) / Math.max(1e-9, elapsed.toNanos() / 1e9);
            double seederShare = delivered == 0 ? 0 : (double) fromSeeder / delivered;
            for (Member m : members) {
                m.content().close();
                m.contentSpace().close();
            }
            return new Report(bytes, pieceSize, peerCount, binding, elapsed, mebibytesPerSec, seederShare, reports);
        } finally {
            deleteTree(root);
        }
    }

    private static void writeRandom(Path file, long bytes, long seed) throws IOException {
        Random random = new Random(seed);
        byte[] chunk = new byte[1024 * 1024];
        try (FileChannel out = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            long left = bytes;
            while (left > 0) {
                random.nextBytes(chunk);
                int n = (int) Math.min(chunk.length, left);
                ByteBuffer buffer = ByteBuffer.wrap(chunk, 0, n);
                while (buffer.hasRemaining()) {
                    out.write(buffer);
                }
                left -= n;
            }
        }
    }

    private static void deleteTree(Path root) {
        try (var walk = Files.walk(root)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // a temp tree; the OS reclaims it
                }
            });
        } catch (IOException ignored) {
            // a temp tree; the OS reclaims it
        }
    }

    public static void main(String[] args) throws Exception {
        int peers = Integer.getInteger("peers", 8);
        long mebibytes = Long.getLong("mebibytes", 256L);
        int pieceKiB = Integer.getInteger("pieceKiB", 1024);
        int deadlineSeconds = Integer.getInteger("deadlineSeconds", 1800);
        String binding = System.getProperty("binding", "tcp");
        Report report = run(peers, mebibytes * 1024 * 1024, pieceKiB * 1024, binding, Duration.ofSeconds(deadlineSeconds));
        System.out.print(report);
    }
}
