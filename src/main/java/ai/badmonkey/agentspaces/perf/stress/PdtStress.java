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

import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.FilePieceStore;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.blob.BlobIdentity;
import ai.badmonkey.agentspaces.pdt.blob.BlobTree;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.store.PackNodeStore;
import ai.badmonkey.agentspaces.pdt.swarm.PdtSwarm;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * The persistent trees across a real fleet (srad/agentspaces-pdt.md PD-O-05): the
 * measures of §5.4 on {@code tcp}, {@code tls}, or {@code dtls}, every peer in
 * one JVM with its own pack node store on disk. Peer 0 writes; the others
 * read. The modes:
 *
 * <ul>
 * <li>{@code map}: a 10,000-entry map of about 2 MiB, fetched whole by every
 * reader, then one key changed and fetched by delta against the held base
 * (PD-Q-PRF-01: at most 8 KiB per reader; PD-Q-PRF-06: at most two round trips).</li>
 * <li>{@code cold}: a fresh blob of the given size fetched by every reader at
 * once from the writer and, as they complete, from each other; the wall time
 * against {@link ContentSwarmStress} moving the same bytes in 1 MiB pieces on
 * the same binding (PD-Q-PRF-08: at most 1.25 times).</li>
 * <li>{@code blob-edit}: ten one-byte edits in place, each fetched by delta
 * (PD-Q-PRF-02: the edited chunk, at most one neighbour, and the path).</li>
 * <li>{@code blob-insert}: ten inserts of 100 bytes (PD-Q-PRF-03: at most 256 KiB at the median).</li>
 * <li>{@code blob-append}: ten appends of 1 MiB (PD-Q-PRF-04: at most 1 MiB plus the old last chunk plus the path).</li>
 * <li>{@code catch-up}: a reader that held nothing fetches the final version
 * naming a base it lacks, so the holder sends the top levels and the reader walks
 * its own frontier (PD-Q-PRF-06: at most the tree's depth plus one round trips).</li>
 * <li>{@code verified-window} (PDT 2 phase 4, P2-B-03, P2-Q-PRF-05): a member that
 * holds nothing of the blob reads {@link #WINDOW_BYTES} from its middle through the
 * outboard the writer keeps, loading the window's chunks, their path, and the
 * {@code bo} nodes on the window's path by {@code NODE_WANT}; the transfer is
 * every node it stored, by kind (P2-Q-PRF-05 as the owner restated it on 2026-11-09: a
 * window of W bytes moves at most W + 128 KiB in all, whatever the blob's size; a {@code bo}
 * leaf covers 8 MiB, so the proof costs about 64 KiB).</li>
 * </ul>
 *
 * <p>The modes {@code push} and {@code reconcile} of PDT 2 phase 5 (P2-Q-PRF-04, P2-Q-PRF-06) need a ref space
 * and a set of roots per member, so {@link PushReconcileStress} runs them on a fleet of their own; {@link #main}
 * hands them over, as {@code -Dmodes=push,reconcile -Dpeers=21}. The modes {@code store-cold}, {@code rrb-splice},
 * and {@code text} of PDT 2 phase 8 (P2-O-04; P2-Q-PRF-02, P2-Q-PRF-07, P2-Q-PRF-09) need a ref facade per member
 * and two content stores, so {@link PdtRefStress} runs them, and {@link #main} hands them over too.
 *
 * <p>No run moves a value above {@link #MAX_BYTES}, 64 MiB (the owner's ruling of 2026-11-09,
 * srad/agentspaces-pdt2.md §10.16): the rows that named 1 GiB or 10 GiB read as rates and ratios, which a 64 MiB
 * run measures.
 *
 * <p>Every member advertises the PDT feature level {@code pdtLevel} (2 by default, 1 with
 * {@code -Dlevel=1}). At level 2 the writer's edits keep intra-chunk delta frames, so
 * {@code blob-edit} moves a frame and the path in place of the edited chunk (PDT 2
 * phase 4, P2-Z-03, P2-Q-PRF-08, as the owner restated them on 2026-11-09: a median under
 * 8 KiB and every edit under 16 KiB); at level 1 the chunk moves whole,
 * as before (87 to 111 KB per edit at 1 GiB, srad/agentspaces-pdt.md §10).
 *
 * <p>The blob modes share one blob: {@code cold} fetches it whole, the edits
 * follow on the held base, and {@code catch-up} uses the reader the cold fetch
 * left out. The writer's {@link PBlob#writtenBytes} is the delta each edit
 * produced; a reader's cost is the bytes its fetch received, duplicate
 * deliveries included, which is what the row bounds.
 */
public final class PdtStress {

    /** The map's entries (PD-Q-PRF-01). */
    public static final int MAP_ENTRIES = 10_000;

    /** Edits, inserts, and appends per mode. */
    public static final int EDITS = 10;

    /** Every mode. */
    public static final Set<String> ALL = Set.of("map", "cold", "blob-edit", "blob-insert", "blob-append", "catch-up",
            "verified-window");

    /** The bytes of the verified window (P2-Q-PRF-05: 2 MB from the middle of the blob). */
    public static final int WINDOW_BYTES = 2 * 1024 * 1024;

    /** The largest blob a run moves (the owner's ruling of 2026-11-09). */
    public static final long MAX_BYTES = 64L << 20;

    private static final Codec<String> STR = Codec.of(String.class);
    private static final long MIB = 1024L * 1024;

    /**
     * One delta over every reader.
     *
     * @param what        the update
     * @param deltaBytes  the node bytes the writer produced
     * @param readerBytes the bytes each reader received
     * @param roundTrips  the longest round-trip chain among the readers
     * @param millis      wall time until the last reader completed
     * @param note        a figure the row reads against, for example the edited chunk's size
     */
    public record Delta(String what, long deltaBytes, List<Long> readerBytes, int roundTrips, long millis, String note) {
        public long maxReaderBytes() {
            return readerBytes.stream().mapToLong(Long::longValue).max().orElse(0);
        }
    }

    /**
     * The cold fetch of the blob.
     *
     * @param blobBytes       the blob's size
     * @param treeBytes       the node bytes of the whole tree
     * @param chunks          leaf chunks
     * @param height          interior levels
     * @param elapsed         wall time from the first fetch to the last completion
     * @param mebibytesPerSec delivered bytes per second of wall time
     * @param writerShare     the share of delivered bytes the writer served
     * @param readerBytes     bytes received per reader
     * @param roundTrips      the longest round-trip chain among the readers
     * @param flatElapsed     the flat swarm's wall time for the same bytes, or null when not run
     */
    public record Cold(long blobBytes, long treeBytes, int chunks, int height, Duration elapsed, double mebibytesPerSec,
                       double writerShare, List<Long> readerBytes, int roundTrips, Duration flatElapsed) {
        public double ratio() {
            return flatElapsed == null ? Double.NaN : (double) elapsed.toNanos() / Math.max(1, flatElapsed.toNanos());
        }
    }

    /** The catch-up of a reader that names a base it does not hold. */
    public record CatchUp(long treeBytes, long bytesReceived, int roundTrips, int depth, long millis, boolean complete) {
    }

    /**
     * The verified window (P2-B-03, P2-Q-PRF-05).
     *
     * @param offset        the window's first byte
     * @param length        the window's bytes
     * @param transferred   every node the reader stored for it
     * @param chunkBytes    of which raw chunks
     * @param treeBytes     of which {@code b} nodes of the blob tree
     * @param outboardBytes of which {@code bo} nodes of the outboard
     * @param outboardMillis how long the writer took to build the outboard
     * @param millis        how long the reader took to open and read the window
     * @param verified      whether the window's bytes equal the writer's
     */
    public record Window(long offset, int length, long transferred, long chunkBytes, long treeBytes, long outboardBytes,
                         long outboardMillis, long millis, boolean verified) {
    }

    /** The run's figures; a mode that did not run leaves its field empty or null. */
    public record Report(String binding, int peers, int pdtLevel, long blobBytes, List<Delta> map, Cold cold,
                         List<Delta> edits, List<Delta> inserts, List<Delta> appends, CatchUp catchUp, Window window) {

        /** The median of the costliest reader's bytes over the one-byte edits (P2-Z-03, P2-Q-PRF-08). */
        public long editMedian() {
            return median(edits);
        }

        private static long median(List<Delta> deltas) {
            List<Long> sorted = new ArrayList<>();
            for (Delta d : deltas) {
                sorted.add(d.maxReaderBytes());
            }
            sorted.sort(null);
            return sorted.isEmpty() ? 0 : sorted.get(sorted.size() / 2);
        }

        private static long max(List<Delta> deltas) {
            return deltas.stream().mapToLong(Delta::maxReaderBytes).max().orElse(0);
        }

        private static int roundTrips(List<Delta> deltas) {
            return deltas.stream().mapToInt(Delta::roundTrips).max().orElse(0);
        }

        private static String verdict(boolean pass) {
            return pass ? "PASS" : "FAIL";
        }

        @Override
        public String toString() {
            StringBuilder out = new StringBuilder(String.format("pdt stress: %d peers on %s at PDT level %d, blob %d MiB%n",
                    peers, binding, pdtLevel, blobBytes / MIB));
            for (Delta d : map) {
                out.append(line(d));
            }
            if (!map.isEmpty()) {
                Delta oneKey = map.get(map.size() - 1);
                out.append(String.format("  PD-Q-PRF-01 one key of %d entries: %d bytes to the costliest reader (bound 8192) %s;"
                                + " round trips %d (bound 2) %s%n", MAP_ENTRIES, oneKey.maxReaderBytes(),
                        verdict(oneKey.maxReaderBytes() <= 8192), oneKey.roundTrips(), verdict(oneKey.roundTrips() <= 2)));
            }
            if (cold != null) {
                out.append(String.format("  cold: %d MiB blob (%d chunks, height %d, %d tree bytes) to %d readers in %d ms,"
                                + " %.1f MiB/s aggregate, writer share %.0f%%, round trips %d, reader bytes %s%n",
                        cold.blobBytes() / MIB, cold.chunks(), cold.height(), cold.treeBytes(), cold.readerBytes().size(),
                        cold.elapsed().toMillis(), cold.mebibytesPerSec(), cold.writerShare() * 100, cold.roundTrips(),
                        cold.readerBytes()));
                if (cold.flatElapsed() != null) {
                    out.append(String.format(Locale.ROOT, "  PD-Q-PRF-08: flat swarm %d ms, trees %d ms, ratio %.2f (bound 1.25) %s%n",
                            cold.flatElapsed().toMillis(), cold.elapsed().toMillis(), cold.ratio(), verdict(cold.ratio() <= 1.25)));
                }
            }
            for (Delta d : edits) {
                out.append(line(d));
            }
            if (!edits.isEmpty()) {
                out.append(String.format("  PD-Q-PRF-02 one byte in place: median %d bytes (bound 131072) %s, max %d (bound %d) %s,"
                                + " round trips %d (bound 2) %s%n", median(edits), verdict(median(edits) < 128 * 1024), max(edits),
                        2 * 256 * 1024 + 16 * 1024, verdict(max(edits) <= 2 * 256 * 1024 + 16 * 1024), roundTrips(edits),
                        verdict(roundTrips(edits) <= 2)));
                if (pdtLevel >= 2) {
                    out.append(String.format("  P2-Z-03 / P2-Q-PRF-08 one byte with intra-chunk deltas: median %d bytes (bound 8192) %s,"
                                    + " max %d (bound 16384) %s%n", median(edits), verdict(median(edits) < 8 * 1024), max(edits),
                            verdict(max(edits) < 16 * 1024)));
                }
            }
            for (Delta d : inserts) {
                out.append(line(d));
            }
            if (!inserts.isEmpty()) {
                out.append(String.format("  PD-Q-PRF-03 100 bytes inserted: median %d bytes (bound 262144) %s, max %d,"
                                + " round trips %d (bound 2) %s%n", median(inserts), verdict(median(inserts) <= 256 * 1024),
                        max(inserts), roundTrips(inserts), verdict(roundTrips(inserts) <= 2)));
            }
            for (Delta d : appends) {
                out.append(line(d));
            }
            if (!appends.isEmpty()) {
                long bound = MIB + 256 * 1024 + 16 * 1024;
                out.append(String.format("  PD-Q-PRF-04 1 MiB appended: max %d bytes (bound %d) %s, round trips %d (bound 2) %s%n",
                        max(appends), bound, verdict(max(appends) <= bound), roundTrips(appends), verdict(roundTrips(appends) <= 2)));
            }
            if (catchUp != null) {
                out.append(String.format("  PD-Q-PRF-06 catch-up without the base: %d of %d tree bytes in %d round trips"
                                + " (bound depth %d + 1) %s, %d ms, %s%n", catchUp.bytesReceived(), catchUp.treeBytes(),
                        catchUp.roundTrips(), catchUp.depth(), verdict(catchUp.roundTrips() <= catchUp.depth() + 1),
                        catchUp.millis(), catchUp.complete() ? "complete" : "INCOMPLETE"));
            }
            if (window != null) {
                // P2-Q-PRF-05 as the owner restated it on 2026-11-09: a window of W bytes moves at most W + 128 KiB in all
                long bound = window.length() + 128 * 1024L;
                out.append(String.format("  verified window: %d bytes at %d read in %d ms (outboard built in %d ms), %s;"
                                + " %d bytes moved: %d of chunks, %d of b nodes, %d of bo nodes%n", window.length(), window.offset(),
                        window.millis(), window.outboardMillis(), window.verified() ? "bytes equal" : "BYTES DIFFER",
                        window.transferred(), window.chunkBytes(), window.treeBytes(), window.outboardBytes()));
                out.append(String.format("  P2-Q-PRF-05 a verified window: %d bytes moved (bound the window + 128 KiB, %d) %s; the"
                                + " proof %d bytes of bo nodes, the overhang %d bytes of chunks past the window, the path %d bytes"
                                + " of b nodes%n", window.transferred(), bound,
                        verdict(window.transferred() <= bound), window.outboardBytes(), window.chunkBytes() - window.length(),
                        window.treeBytes()));
            }
            return out.toString();
        }

        private static String line(Delta d) {
            return String.format("  %-22s delta %10d bytes; readers %s; round trips %d; %d ms%s%n", d.what(), d.deltaBytes(),
                    d.readerBytes(), d.roundTrips(), d.millis(), d.note() == null ? "" : "; " + d.note());
        }
    }

    private record Member(MiniFleet.Peer peer, Content content, ReplicatedSpace contentSpace, PackNodeStore pack,
                          NodeStore store, PdtSwarm swarm, CapabilityRuntime capabilities) {
        PeerId id() {
            return peer.identity().peerId();
        }

        void close() {
            capabilities.close();
            content.close();
            contentSpace.close();
            try {
                pack.close();
            } catch (Exception ignored) {
                // a temp store
            }
        }
    }

    private PdtStress() {
    }

    /**
     * Runs the modes.
     *
     * @param modes     the modes of the class comment, or {@link #ALL}
     * @param peerCount peers, the writer included (at least 2; {@code catch-up} needs 3)
     * @param blobBytes the blob size
     * @param binding   {@code tcp}, {@code tls}, or {@code dtls}
     * @param deadline  the hard stop per fetch and for the run
     * @return the report
     * @throws Exception when the fleet fails to start or a fetch does not complete
     */
    public static Report run(Set<String> modes, int peerCount, long blobBytes, String binding, Duration deadline) throws Exception {
        return run(modes, peerCount, blobBytes, binding, deadline, 2);
    }

    /**
     * Runs the modes with every member at a PDT feature level.
     *
     * @param modes     the modes of the class comment, or {@link #ALL}
     * @param peerCount peers, the writer included (at least 2; {@code catch-up} and {@code verified-window} each take one)
     * @param blobBytes the blob size
     * @param binding   {@code tcp}, {@code tls}, or {@code dtls}
     * @param deadline  the hard stop per fetch and for the run
     * @param pdtLevel  1, or 2 for the level-2 forms such as intra-chunk deltas (P2-F-05)
     * @return the report
     * @throws Exception when the fleet fails to start or a fetch does not complete
     */
    public static Report run(Set<String> modes, int peerCount, long blobBytes, String binding, Duration deadline,
                             int pdtLevel) throws Exception {
        if (peerCount < 2) {
            throw new IllegalArgumentException("at least two peers: a writer and a reader");
        }
        if (blobBytes > MAX_BYTES) {
            throw new IllegalArgumentException("a run moves blobs of 64 MiB or less (the owner's ruling of 2026-11-09), got "
                    + blobBytes + " bytes");
        }
        for (String mode : modes) {
            if (!ALL.contains(mode)) {
                throw new IllegalArgumentException("no mode " + mode + "; modes are " + ALL);
            }
        }
        boolean window = modes.contains("verified-window");
        boolean readersHold = modes.contains("cold") || modes.contains("blob-edit") || modes.contains("blob-insert")
                || modes.contains("blob-append") || modes.contains("catch-up");
        boolean blob = readersHold || window;
        boolean catchUp = modes.contains("catch-up");
        int apart = (catchUp ? 1 : 0) + (window ? 1 : 0);
        boolean needsReaders = modes.stream().anyMatch(m -> !m.equals("verified-window"));
        if (peerCount - 1 - apart < (needsReaders ? 1 : 0)) {
            throw new IllegalArgumentException("catch-up and verified-window each need a peer that the other modes leave out");
        }
        Duration flatElapsed = null;
        if (modes.contains("cold")) {
            // the flat swarm first, on its own fleet, so the two runs do not share the host's sockets or memory
            ContentSwarmStress.Report flat = ContentSwarmStress.run(peerCount, blobBytes, 1024 * 1024, binding, deadline);
            System.out.print(flat);
            flatElapsed = flat.elapsed();
        }
        CborCodec codec = CborCodec.defaultCodec();
        Path root = Files.createTempDirectory("pdt-stress");
        List<Member> members = new ArrayList<>();
        try (MiniFleet fleet = MiniFleet.start("pdt-stress", peerCount, Duration.ofMillis(100), binding)) {
            for (int i = 0; i < peerCount; i++) {
                members.add(join(fleet.peer(i), i, root.resolve("peer-" + i), codec, deadline, pdtLevel));
            }
            awaitFleet(members, deadline);
            Member writer = members.get(0);
            List<Member> readers = members.subList(1, peerCount - apart);
            Member late = catchUp ? members.get(peerCount - 1) : null;
            Member windowReader = window ? members.get(peerCount - 1 - (catchUp ? 1 : 0)) : null;

            List<Delta> map = new ArrayList<>();
            if (modes.contains("map")) {
                map.addAll(mapRun(writer, readers, deadline));
            }
            Cold cold = null;
            List<Delta> edits = new ArrayList<>();
            List<Delta> inserts = new ArrayList<>();
            List<Delta> appends = new ArrayList<>();
            CatchUp caught = null;
            Window read = null;
            if (blob) {
                Path file = root.resolve("blob.bin");
                writeRandom(file, blobBytes, 29);
                PBlob current = PBlob.of(file, writer.store());
                Files.deleteIfExists(file);
                String root0 = current.root();
                long treeBytes = current.writtenBytes();
                writer.swarm().hold(root0);
                if (window) {
                    read = windowRun(writer, windowReader, current, deadline);
                }
                if (readersHold) {
                    cold = coldRun(writer, readers, current, treeBytes, flatElapsed, deadline);
                    for (Member reader : readers) {
                        reader.swarm().hold(root0);
                    }
                }
                Random random = new Random(42);
                if (modes.contains("blob-edit")) {
                    for (int i = 1; i <= EDITS; i++) {
                        long offset = Math.floorMod(random.nextLong(), current.size());
                        byte old = current.read(offset, 1)[0];
                        PBlob next = current.splice(offset, 1, new byte[]{(byte) (old ^ 0x5a)});
                        BlobTree.Entry chunk = chunkAt(next, offset);
                        edits.add(delta("edit " + i, writer, readers, current, next, deadline,
                                "edited chunk " + chunk.length() + " bytes at " + offset));
                        current = next;
                    }
                }
                if (modes.contains("blob-insert")) {
                    for (int i = 1; i <= EDITS; i++) {
                        long offset = Math.floorMod(random.nextLong(), current.size());
                        byte[] insert = new byte[100];
                        random.nextBytes(insert);
                        PBlob next = current.splice(offset, 0, insert);
                        inserts.add(delta("insert " + i, writer, readers, current, next, deadline, "100 bytes at " + offset));
                        current = next;
                    }
                }
                if (modes.contains("blob-append")) {
                    for (int i = 1; i <= EDITS; i++) {
                        byte[] tail = new byte[(int) MIB];
                        random.nextBytes(tail);
                        long lastOld = current.chunks().get(current.chunks().size() - 1).length();
                        PBlob next = current.append(tail);
                        appends.add(delta("append " + i, writer, readers, current, next, deadline, "old last chunk " + lastOld + " bytes"));
                        current = next;
                    }
                }
                if (catchUp) {
                    caught = catchUpRun(writer, readers, late, current, root0, deadline);
                }
            }
            Report report = new Report(binding, peerCount, pdtLevel, blobBytes, map, cold, edits, inserts, appends, caught, read);
            for (Member m : members) {
                m.close();
            }
            members.clear();
            return report;
        } finally {
            for (Member m : members) {
                m.close();
            }
            deleteTree(root);
        }
    }

    // ------------------------------------------------------------ the fleet

    private static Member join(MiniFleet.Peer peer, int index, Path dir, CborCodec codec, Duration deadline, int pdtLevel)
            throws IOException {
        BlockExchange blocks = new BlockExchange(peer.runtime(), codec);
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(peer.runtime(), "content", peer.identity(), "content")
                .settleWindow(Duration.ofMillis(100)).blocks(blocks).build();
        Files.createDirectories(dir);
        PackNodeStore pack = PackNodeStore.open(dir.resolve("nodes"));
        NodeStore store = new NodeStore(pack, InstantSource.system());
        DiscoveryService discovery = new DiscoveryService(peer.runtime(), new AdCache(codec, InstantSource.system()), codec,
                peer.identity().peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(peer.runtime(), discovery, peer.identity());
        Content content = Content.builder(new CapabilityPipes(peer.runtime(), codec), peer.runtime(), contentSpace,
                        peer.identity().peerId())
                .codec(codec).store(new FilePieceStore(dir.resolve("pieces"), codec, InstantSource.system(), 0))
                .blocks(blocks).random(new Random(index)).discovery(discovery).treeStore(PdtSwarm.treeStore(store))
                .pdtLevel(pdtLevel).build();
        capabilities.register(content);
        PdtSwarm swarm = PdtSwarm.attach(content, store, deadline);
        return new Member(peer, content, contentSpace, pack, store, swarm, capabilities);
    }

    /** Every member sees every other and its content-swarm advertisement with the trees parameter. */
    private static void awaitFleet(List<Member> members, Duration deadline) throws InterruptedException {
        boolean ready = await(Duration.ofSeconds(Math.min(120, Math.max(30, deadline.toSeconds()))), () -> {
            for (Member from : members) {
                if (from.peer().runtime().membership().allMembers().size() < members.size() - 1) {
                    return false;
                }
                for (Member to : members) {
                    if (from != to && !from.content().advertisesTrees(to.id())) {
                        return false;
                    }
                }
            }
            return true;
        });
        if (!ready) {
            throw new IllegalStateException("the fleet did not form: not every member advertises the tree exchange to every other");
        }
    }

    private static boolean await(Duration max, BooleanSupplier until) throws InterruptedException {
        long deadline = System.nanoTime() + max.toNanos();
        while (!until.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }

    // ------------------------------------------------------------ the measures

    private static List<Delta> mapRun(Member writer, List<Member> readers, Duration deadline) throws Exception {
        Random random = new Random(7);
        Editor<String, String> editor = PMap.empty(writer.store(), STR, STR).editor();
        for (int i = 0; i < MAP_ENTRIES; i++) {
            char[] chars = new char[190];
            for (int c = 0; c < chars.length; c++) {
                chars[c] = (char) ('a' + random.nextInt(26));
            }
            editor.assoc("key-" + i, i + ":" + new String(chars));
        }
        Editor.Result<String, String> built = editor.done();
        PMap<String, String> v0 = built.value();
        String root0 = v0.root();
        long mapBytes = 0;
        for (byte[] node : built.nodes().values()) {
            mapBytes += node.length;
        }
        writer.swarm().hold(root0);
        List<Delta> out = new ArrayList<>();
        out.add(fetchAll("map whole", writer, readers, root0, null, mapBytes, deadline, MAP_ENTRIES + " entries"));
        for (Member reader : readers) {
            reader.swarm().hold(root0);
        }
        PMap<String, String> v1 = v0.assoc("key-1234", "changed");
        String root1 = v1.root();
        long delta = bytesNotIn(writer.store(), root1, root0);
        writer.swarm().hold(root1);
        out.add(fetchAll("map one key", writer, readers, root1, root0, delta, deadline, null));
        return out;
    }

    private static Cold coldRun(Member writer, List<Member> readers, PBlob blob, long treeBytes, Duration flatElapsed,
                                Duration deadline) throws Exception {
        long start = System.nanoTime();
        List<TreeFetch> fetches = new ArrayList<>();
        for (Member reader : readers) {
            fetches.add(reader.swarm().fetchTree(blob.root(), List.of(), FetchOptions.defaults().withTimeout(deadline).prefer(writer.id())));
        }
        Thread watcher = Thread.ofPlatform().daemon().name("pdt-stress-watch").start(() -> {
            long[] last = new long[fetches.size()];
            int[] still = new int[fetches.size()];
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(10_000);
                    StringBuilder line = new StringBuilder("  progress:");
                    for (int i = 0; i < fetches.size(); i++) {
                        TreeFetch fetch = fetches.get(i);
                        long received = fetch.progress().bytesReceived();
                        line.append(String.format(" %d MiB", received / MIB));
                        still[i] = received == last[i] && !fetch.isDoneOrFailed() ? still[i] + 1 : 0;
                        last[i] = received;
                    }
                    System.out.println(line);
                    // PDT 2 phase 7, P2-X-07: a reader stalled for thirty seconds prints the tail of its trace once,
                    // as ContentSwarmStress does, so a recorded run says why it waited
                    for (int i = 0; i < fetches.size(); i++) {
                        if (still[i] == 3) {
                            List<String> trace = fetches.get(i).trace();
                            System.out.println("  reader " + (i + 1) + " made no progress for 30 s; its trace ends:");
                            trace.subList(Math.max(0, trace.size() - 8), trace.size()).forEach(t -> System.out.println("    " + t));
                        }
                    }
                }
            } catch (InterruptedException ignored) {
                // the run ended
            }
        });
        long end = start;
        long delivered = 0;
        long fromWriter = 0;
        int roundTrips = 0;
        List<Long> readerBytes = new ArrayList<>();
        try {
            for (TreeFetch fetch : fetches) {
                awaitFetch(fetch, start, deadline, "cold fetch of " + blob.root());
                end = Math.max(end, System.nanoTime());
                TreeFetch.Progress p = fetch.progress();
                readerBytes.add(p.bytesReceived());
                delivered += p.bytesReceived();
                fromWriter += p.bytesBySource().getOrDefault(writer.id(), 0L);
                roundTrips = Math.max(roundTrips, p.roundTrips());
            }
        } finally {
            watcher.interrupt();
        }
        Duration elapsed = Duration.ofNanos(Math.max(1, end - start));
        double mebibytesPerSec = delivered / (double) MIB / Math.max(1e-9, elapsed.toNanos() / 1e9);
        return new Cold(blob.size(), treeBytes, blob.chunks().size(), blob.tree().height(), elapsed, mebibytesPerSec,
                delivered == 0 ? 0 : (double) fromWriter / delivered, readerBytes, roundTrips, flatElapsed);
    }

    private static Delta delta(String what, Member writer, List<Member> readers, PBlob previous, PBlob next, Duration deadline,
                               String note) throws Exception {
        writer.swarm().hold(next.root());
        return fetchAll(what, writer, readers, next.root(), previous.root(), next.writtenBytes(), deadline, note);
    }

    /** Every reader fetches a root at once, with the base in its haves when given; the costliest reader's figures. */
    private static Delta fetchAll(String what, Member writer, List<Member> readers, String root, String base, long deltaBytes,
                                  Duration deadline, String note) throws Exception {
        long start = System.nanoTime();
        List<TreeFetch> fetches = new ArrayList<>();
        for (Member reader : readers) {
            List<String> haves = base != null && reader.store().complete(base) ? List.of(base) : List.of();
            fetches.add(reader.swarm().fetchTree(root, haves, FetchOptions.defaults().withTimeout(deadline).prefer(writer.id())));
        }
        List<Long> readerBytes = new ArrayList<>();
        int roundTrips = 0;
        long end = start;
        for (TreeFetch fetch : fetches) {
            awaitFetch(fetch, start, deadline, what);
            end = Math.max(end, System.nanoTime());
            readerBytes.add(fetch.progress().bytesReceived());
            roundTrips = Math.max(roundTrips, fetch.progress().roundTrips());
        }
        Delta delta = new Delta(what, deltaBytes, readerBytes, roundTrips, TimeUnit.NANOSECONDS.toMillis(end - start), note);
        System.out.print(Report.line(delta));
        return delta;
    }

    /**
     * The verified window (P2-B-03, P2-Q-PRF-05): the writer builds and keeps the outboard, as a blob head's writer does;
     * a member that holds nothing follows the blob's root, so the writer is its source, and reads {@link #WINDOW_BYTES}
     * from the middle through its loader; what it stored is what moved.
     */
    private static Window windowRun(Member writer, Member reader, PBlob blob, Duration deadline) throws Exception {
        long started = System.nanoTime();
        BlobIdentity identity = BlobIdentity.of(blob.outboard());
        long outboardMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        int length = (int) Math.min(WINDOW_BYTES, blob.size());
        long offset = (blob.size() - length) / 2;
        Set<String> before = new java.util.HashSet<>(reader.store().backend().cids());
        reader.swarm().fetcher().follow(blob.root());
        started = System.nanoTime();
        byte[] bytes;
        try (InputStream in = PBlob.load(reader.store(), reader.swarm().loader(), blob.root()).claiming(identity)
                .openVerified(offset, length)) {
            bytes = in.readAllBytes();
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        long chunks = 0;
        long tree = 0;
        long outboard = 0;
        for (String cid : reader.store().backend().cids()) {
            if (before.contains(cid)) {
                continue;
            }
            byte[] node = reader.store().get(cid).orElseThrow();
            if (Cids.parse(cid).codec() == Cids.RAW) {
                chunks += node.length;
            } else if (Kind.BO.equals(NodeCodec.kindOf(node))) {
                outboard += node.length;
            } else {
                tree += node.length;
            }
        }
        boolean verified = java.util.Arrays.equals(bytes, blob.read(offset, length));
        Window window = new Window(offset, length, chunks + tree + outboard, chunks, tree, outboard, outboardMillis, millis, verified);
        System.out.printf("  verified window: %d bytes in %d ms, %d bytes moved%n", length, millis, window.transferred());
        return window;
    }

    private static CatchUp catchUpRun(Member writer, List<Member> readers, Member late, PBlob current, String root0,
                                      Duration deadline) throws Exception {
        List<PeerId> prefer = new ArrayList<>();
        prefer.add(writer.id());
        for (Member reader : readers) {
            prefer.add(reader.id());
        }
        long start = System.nanoTime();
        // the late reader names the first version as a base it never held: the holder sends the top levels
        // and the reader walks its own frontier (S3 of §6.4)
        TreeFetch fetch = late.swarm().fetchTree(current.root(), List.of(root0),
                FetchOptions.defaults().withTimeout(deadline).prefer(prefer.toArray(PeerId[]::new)));
        boolean done = fetch.await(deadline);
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        TreeFetch.Progress p = fetch.progress();
        long treeBytes = bytesUnderTree(current);
        return new CatchUp(treeBytes, p.bytesReceived(), p.roundTrips(), current.tree().height(), millis, done && fetch.complete());
    }

    private static void awaitFetch(TreeFetch fetch, long start, Duration deadline, String what) throws InterruptedException {
        long remaining = Math.max(1, start + deadline.toNanos() - System.nanoTime());
        if (!fetch.await(Duration.ofNanos(remaining)) || !fetch.complete()) {
            List<String> trace = fetch.trace();
            throw new IllegalStateException(what + " did not complete within the deadline; trace tail: "
                    + String.join(" | ", trace.subList(Math.max(0, trace.size() - 10), trace.size())));
        }
    }

    // ------------------------------------------------------------ helpers

    private static long bytesNotIn(NodeStore store, String root, String base) {
        Set<String> shared = base == null ? Set.of() : store.reachable(base);
        long total = 0;
        for (String cid : store.reachable(root)) {
            if (!shared.contains(cid)) {
                total += store.get(cid).orElseThrow().length;
            }
        }
        return total;
    }

    /** The blob's size plus its interior nodes, without reading every chunk back. */
    private static long bytesUnderTree(PBlob blob) {
        long total = blob.size();
        Set<String> seen = new LinkedHashSet<>();
        total += interiorBytes(blob.store(), blob.root(), seen);
        return total;
    }

    private static long interiorBytes(NodeStore store, String cid, Set<String> seen) {
        if (!seen.add(cid) || !cid.startsWith("bafyrei")) {
            return 0;
        }
        byte[] bytes = store.get(cid).orElseThrow();
        long total = bytes.length;
        for (String link : ai.badmonkey.agentspaces.pdt.NodeCodec.links(cid, bytes)) {
            total += interiorBytes(store, link, seen);
        }
        return total;
    }

    private static BlobTree.Entry chunkAt(PBlob blob, long offset) {
        long start = 0;
        for (BlobTree.Entry entry : blob.chunks()) {
            if (offset < start + entry.length()) {
                return entry;
            }
            start += entry.length();
        }
        throw new IllegalArgumentException("offset " + offset + " past the end");
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
        String modesText = System.getProperty("modes", "all");
        Set<String> modes = new LinkedHashSet<>();
        if (modesText.equals("all")) {
            modes.addAll(ALL);
            modes.addAll(PushReconcileStress.MODES);
            modes.addAll(PdtRefStress.MODES);
        } else {
            modes.addAll(List.of(modesText.split(",")));
        }
        int peers = Integer.getInteger("peers", 8);
        long mebibytes = Long.getLong("mebibytes", 64L);
        String binding = System.getProperty("binding", "tcp");
        int deadlineSeconds = Integer.getInteger("deadlineSeconds", 1800);
        int level = Integer.getInteger("level", 2);
        Set<String> phaseFive = new LinkedHashSet<>(modes);
        phaseFive.retainAll(PushReconcileStress.MODES);
        Set<String> rest = new LinkedHashSet<>(modes);
        rest.removeAll(PushReconcileStress.MODES);
        if (!phaseFive.isEmpty()) {
            System.out.print(PushReconcileStress.run(phaseFive, peers, binding, Duration.ofSeconds(deadlineSeconds)));
        }
        Set<String> phaseEight = new LinkedHashSet<>(rest);
        phaseEight.retainAll(PdtRefStress.MODES);
        rest.removeAll(PdtRefStress.MODES);
        if (!phaseEight.isEmpty()) {
            System.out.print(PdtRefStress.run(phaseEight, peers, mebibytes * MIB, Long.getLong("elements", PdtRefStress.VECTOR_ELEMENTS),
                    binding, Duration.ofSeconds(deadlineSeconds)));
        }
        if (rest.isEmpty()) {
            return;
        }
        Report report = run(rest, peers, mebibytes * MIB, binding, Duration.ofSeconds(deadlineSeconds), level);
        System.out.print(report);
    }
}
