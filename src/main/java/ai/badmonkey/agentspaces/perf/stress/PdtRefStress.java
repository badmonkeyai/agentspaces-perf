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

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.MemoryPieceStore;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.store.PackNodeStore;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PText;
import ai.badmonkey.agentspaces.pdt.values.PVector;
import ai.badmonkey.agentspaces.pdt.values.VectorEditor;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.store.ContentStore;
import ai.badmonkey.agentspaces.store.StorePolicy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * The {@code PdtStress} modes of PDT 2 phase 8 that need a ref facade per member (srad/agentspaces-pdt2.md P2-O-04),
 * over a real fleet on {@code tcp}, {@code tls}, or {@code dtls}, every value at 64 MiB or less (the owner's ruling
 * of 2026-11-09, §10.16):
 *
 * <ul>
 * <li>{@code store-cold} (P2-Q-PRF-02, P2-S-03): member 0 writes a blob ref, pins it on members 1 and 2, which run
 * {@link ContentStore}s, and leaves once both receipts are in; every other member then fetches the blob cold at
 * once. The wall time is set against {@link ContentSwarmStress} moving the same bytes in 1 MiB pieces from one seeder
 * to as many fetchers on the same binding (bound 1.25), and the stores' share of the delivered bytes against
 * 90 %.</li>
 * <li>{@code rrb-splice} (P2-Q-PRF-07, P2-R-02): member 0 writes a vector ref of {@code vectorElements} integers,
 * every other member follows it, and the writer inserts one element at {@link #SPLICES} random places, one swap
 * each; each insertion's delta (bound 8 KiB at commit) and the bytes each reader received for it.</li>
 * <li>{@code text} (P2-Q-PRF-09, P2-T-03): members 0 and 1 edit one text of a map ref at once, {@link #TEXT_EDITS}
 * edits in all, half each, one swap each, so the ref holds two heads; every member receives both, and a reader's
 * first read merges them (bound 100 ms). The bytes each member received for both, over the edits, are the
 * shipped bytes per edit (bound 50). The run also reports what one edit after the merge ships, the whole
 * {@code t} node, since a text leaf moves whole.</li>
 * </ul>
 */
public final class PdtRefStress {

    /** The modes this class runs. */
    public static final Set<String> MODES = Set.of("store-cold", "rrb-splice", "text");

    /** The vector's elements in {@code rrb-splice} (P2-Q-PRF-07: a million). */
    public static final long VECTOR_ELEMENTS = 1_000_000;

    /** The insertions of {@code rrb-splice}. */
    public static final int SPLICES = 10;

    /** The concurrent edits of {@code text}, half by each replica (P2-Q-PRF-09). */
    public static final int TEXT_EDITS = 1_000;

    /** The largest value a run moves (the owner's ruling of 2026-11-09). */
    public static final long MAX_BYTES = 64L << 20;

    private static final Codec<String> STR = Codec.of(String.class);
    private static final Codec<Long> LONG = Codec.of(Long.class);
    private static final long MIB = 1024L * 1024;
    private static final String BASE_TEXT = "the quick brown fox jumps over the lazy dog";

    /**
     * The {@code store-cold} figures.
     *
     * @param blobBytes   the blob's size
     * @param readers     the readers that fetched it
     * @param elapsed     from the first fetch to the last completion
     * @param flatElapsed the flat swarm's wall time for the same bytes to as many fetchers
     * @param bySource    the bytes served, by {@code store-a}, {@code store-b}, {@code reader}, or {@code writer}
     * @param readerBytes the bytes each reader received
     * @param roundTrips  the longest round-trip chain among the readers
     * @param complete    whether every reader completed
     */
    public record StoreCold(long blobBytes, int readers, Duration elapsed, Duration flatElapsed, Map<String, Long> bySource,
                            List<Long> readerBytes, int roundTrips, boolean complete) {
        public double ratio() {
            return (double) elapsed.toNanos() / Math.max(1, flatElapsed.toNanos());
        }

        /** The share of the delivered bytes the two stores served. */
        public double storeShare() {
            long all = bySource.values().stream().mapToLong(Long::longValue).sum();
            long stores = bySource.getOrDefault("store-a", 0L) + bySource.getOrDefault("store-b", 0L);
            return all == 0 ? 0 : (double) stores / all;
        }
    }

    /** One insertion of {@code rrb-splice}: where, the delta it wrote, and what each reader received. */
    public record Splice(long index, long deltaBytes, int deltaNodes, List<Long> readerBytes, long millis) {
        public long maxReaderBytes() {
            return readerBytes.stream().mapToLong(Long::longValue).max().orElse(0);
        }
    }

    /** The {@code rrb-splice} figures. */
    public record Rrb(long elements, long vectorBytes, long wholeMillis, List<Splice> splices, boolean relaxed) {
        public long maxDelta() {
            return splices.stream().mapToLong(Splice::deltaBytes).max().orElse(0);
        }

        public long maxReaderBytes() {
            return splices.stream().mapToLong(Splice::maxReaderBytes).max().orElse(0);
        }
    }

    /**
     * The {@code text} figures.
     *
     * @param edits        the edits of both replicas
     * @param heads        the heads the ref held after both swaps
     * @param memberBytes  the bytes each member received for the two swaps
     * @param mergeMillis  the slowest first read that merged both heads
     * @param length       the merged text's length
     * @param converged    whether every member reads the same text
     * @param oneEditBytes the most bytes a member received for one edit after the merge
     */
    public record Text(int edits, int heads, List<Long> memberBytes, double mergeMillis, long length, boolean converged,
                       long oneEditBytes) {
        public double bytesPerEdit() {
            return memberBytes.stream().mapToLong(Long::longValue).max().orElse(0) / (double) edits;
        }
    }

    /** The run's figures; a mode that did not run leaves its field null. */
    public record Report(String binding, int peers, StoreCold storeCold, Rrb rrb, Text text) {

        private static String verdict(boolean pass) {
            return pass ? "PASS" : "FAIL";
        }

        @Override
        public String toString() {
            StringBuilder out = new StringBuilder(String.format("pdt stress, ref modes: %d peers on %s%n", peers, binding));
            if (storeCold != null) {
                StoreCold s = storeCold;
                out.append(String.format(Locale.ROOT, "  store-cold: %d MiB to %d readers after the writer left in %d ms, %s;"
                                + " by source %s; reader bytes %s; round trips %d%n", s.blobBytes() / MIB, s.readers(),
                        s.elapsed().toMillis(), s.complete() ? "complete" : "INCOMPLETE", s.bySource(), s.readerBytes(),
                        s.roundTrips()));
                out.append(String.format(Locale.ROOT, "  P2-Q-PRF-02 store-cold: flat swarm %d ms, stores %d ms, ratio %.2f"
                                + " (bound 1.25) %s; the stores served %.1f%% (bound 90%%) %s%n", s.flatElapsed().toMillis(),
                        s.elapsed().toMillis(), s.ratio(), verdict(s.ratio() <= 1.25), s.storeShare() * 100,
                        verdict(s.storeShare() >= 0.9)));
            }
            if (rrb != null) {
                out.append(String.format("  rrb-splice: a vector of %d elements, %d bytes of nodes, followed whole in %d ms%n",
                        rrb.elements(), rrb.vectorBytes(), rrb.wholeMillis()));
                for (Splice s : rrb.splices()) {
                    out.append(String.format("  insert at %8d: delta %d bytes in %d nodes; readers %s; %d ms%n", s.index(),
                            s.deltaBytes(), s.deltaNodes(), s.readerBytes(), s.millis()));
                }
                out.append(String.format("  P2-Q-PRF-07 one insertion in %d elements: at most %d bytes at commit (bound 8192) %s;"
                                + " at most %d bytes to a reader; the vector is %s%n", rrb.elements(), rrb.maxDelta(),
                        verdict(rrb.maxDelta() < 8192), rrb.maxReaderBytes(), rrb.relaxed() ? "relaxed (an rr root)" : "packed"));
            }
            if (text != null) {
                out.append(String.format(Locale.ROOT, "  text: %d edits by two replicas in one swap each, %d heads; member bytes %s;"
                                + " merged text %d characters, %s%n", text.edits(), text.heads(), text.memberBytes(), text.length(),
                        text.converged() ? "every member reads it" : "MEMBERS DIFFER"));
                out.append(String.format(Locale.ROOT, "  P2-Q-PRF-09 %d concurrent edits: merge %.1f ms (bound 100) %s; %.1f bytes"
                                + " shipped per edit (bound 50) %s; one edit after the merge ships %d bytes (the t node whole)%n",
                        text.edits(), text.mergeMillis(), verdict(text.mergeMillis() < 100), text.bytesPerEdit(),
                        verdict(text.bytesPerEdit() < 50), text.oneEditBytes()));
            }
            return out.toString();
        }
    }

    private record Member(MiniFleet.Peer peer, Content content, ReplicatedSpace contentSpace, ReplicatedSpace refSpace, Pdt pdt,
                          CapabilityRuntime capabilities, DiscoveryService discovery, PackNodeStore pack, ContentStore store) {
        PeerId id() {
            return peer.identity().peerId();
        }

        void close() {
            if (store != null) {
                store.close();
            }
            pdt.close();
            capabilities.close();
            content.close();
            refSpace.close();
            contentSpace.close();
            try {
                pack.close();
            } catch (Exception ignored) {
                // a temp store
            }
        }
    }

    private PdtRefStress() {
    }

    /**
     * Runs the modes.
     *
     * @param modes          {@link #MODES} or a part of it
     * @param peerCount      the members: {@code store-cold} needs 4 (a writer, two stores, a reader), the others 3
     * @param blobBytes      the blob of {@code store-cold}, at most {@link #MAX_BYTES}
     * @param vectorElements the vector of {@code rrb-splice}
     * @param binding        {@code tcp}, {@code tls}, or {@code dtls}
     * @param deadline       the hard stop of each wait
     * @return the report
     * @throws Exception when a fleet fails to start or a wait runs out
     */
    public static Report run(Set<String> modes, int peerCount, long blobBytes, long vectorElements, String binding,
                             Duration deadline) throws Exception {
        if (!MODES.containsAll(modes)) {
            throw new IllegalArgumentException("modes are " + MODES + ", got " + modes);
        }
        if (blobBytes <= 0 || blobBytes > MAX_BYTES) {
            throw new IllegalArgumentException("a run moves values of 64 MiB or less (the owner's ruling of 2026-11-09), got "
                    + blobBytes + " bytes");
        }
        if (peerCount < (modes.contains("store-cold") ? 4 : 3)) {
            throw new IllegalArgumentException("store-cold needs 4 peers, rrb-splice and text 3; got " + peerCount);
        }
        StoreCold storeCold = modes.contains("store-cold") ? storeColdRun(peerCount, blobBytes, binding, deadline) : null;
        Rrb rrb = modes.contains("rrb-splice") ? rrbRun(peerCount, vectorElements, binding, deadline) : null;
        Text text = modes.contains("text") ? textRun(peerCount, binding, deadline) : null;
        return new Report(binding, peerCount, storeCold, rrb, text);
    }

    // ------------------------------------------------------------ the fleet

    private static Member join(MiniFleet.Peer peer, int index, Path dir, Duration deadline, StorePolicy policy, boolean lazy,
                               java.util.function.UnaryOperator<Pdt.Builder> facade) throws IOException {
        CborCodec codec = CborCodec.defaultCodec();
        BlockExchange blocks = new BlockExchange(peer.runtime(), codec);
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(peer.runtime(), "content", peer.identity(), "content")
                .settleWindow(Duration.ofMillis(100)).blocks(blocks).build();
        ReplicatedSpace refSpace = ReplicatedSpace.builder(peer.runtime(), "refs", peer.identity(), "refs")
                .settleWindow(Duration.ofMillis(100)).build();
        DiscoveryService discovery = new DiscoveryService(peer.runtime(), new AdCache(codec, InstantSource.system()), codec,
                peer.identity().peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(peer.runtime(), discovery, peer.identity());
        Files.createDirectories(dir);
        PackNodeStore pack = PackNodeStore.open(dir.resolve("nodes"));
        Pdt.Builder builder = Pdt.builder(refSpace).store(new NodeStore(pack, InstantSource.system())).loadTimeout(deadline)
                .collectEvery(Duration.ZERO).discovery(discovery).identity(peer.identity());
        if (lazy) {
            builder.follow(Pdt.FOLLOW_LAZY);
        }
        Pdt pdt = facade.apply(builder).build();
        Content content = Content.builder(new CapabilityPipes(peer.runtime(), codec), peer.runtime(), contentSpace,
                        peer.identity().peerId())
                .codec(codec).store(new MemoryPieceStore(InstantSource.system(), 0)).blocks(blocks).random(new Random(index))
                .discovery(discovery).treeStore(pdt.treeStore()).pdtLevel(Content.PDT_LEVEL).build();
        capabilities.register(content);
        pdt.attach(content).start();
        peer.runtime().onTick(pdt::tick);
        ContentStore store = null;
        if (policy != null) {
            store = ContentStore.builder(pdt, discovery, peer.identity()).name("store-" + index).policy(policy)
                    .holdingInterval(Duration.ofSeconds(2)).fetchTimeout(deadline).build();
            peer.runtime().onTick(store::tick);
            store.start();
        }
        return new Member(peer, content, contentSpace, refSpace, pdt, capabilities, discovery, pack, store);
    }

    private static List<Member> joinAll(MiniFleet fleet, Path root, Duration deadline, java.util.function.IntFunction<StorePolicy> policies,
                                        java.util.function.IntPredicate lazy, java.util.function.UnaryOperator<Pdt.Builder> facade)
            throws Exception {
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < fleet.size(); i++) {
            members.add(join(fleet.peer(i), i, root.resolve("peer-" + i), deadline, policies.apply(i), lazy.test(i), facade));
        }
        boolean ready = await(Duration.ofSeconds(Math.min(120, Math.max(30, deadline.toSeconds()))), () -> {
            for (Member from : members) {
                if (from.peer().runtime().membership().allMembers().size() < members.size() - 1) {
                    return false;
                }
                for (Member to : members) {
                    if (from != to && from.content().pdtLevel(to.id()) < Content.PDT_LEVEL) {
                        return false;
                    }
                }
            }
            return true;
        });
        if (!ready) {
            members.forEach(Member::close);
            throw new IllegalStateException("the fleet did not form: not every member reads every other at pdt level 2");
        }
        return members;
    }

    private static boolean await(Duration max, BooleanSupplier until) throws InterruptedException {
        long stop = System.nanoTime() + max.toNanos();
        while (!until.getAsBoolean()) {
            if (System.nanoTime() > stop) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }

    private static void require(boolean condition, String what) {
        if (!condition) {
            throw new IllegalStateException(what + " did not happen within the deadline");
        }
    }

    // ------------------------------------------------------------ store-cold

    private static StoreCold storeColdRun(int peerCount, long blobBytes, String binding, Duration deadline) throws Exception {
        int readers = peerCount - 3;
        // the flat swarm first, on its own fleet: one seeder and as many fetchers as the stores will serve
        ContentSwarmStress.Report flat = ContentSwarmStress.run(readers + 1, blobBytes, 1024 * 1024, binding, deadline);
        System.out.print(flat);
        Path root = Files.createTempDirectory("pdt-store-cold");
        List<Member> members = new ArrayList<>();
        try (MiniFleet fleet = MiniFleet.start("pdt-store-cold", peerCount, Duration.ofMillis(100), binding)) {
            StorePolicy policy = StorePolicy.of(1L << 30).acceptRequesters(StorePolicy.Accept.OPEN);
            // the stores and the readers follow lazily: a store holds what is pinned on it, and a reader fetches cold
            members.addAll(joinAll(fleet, root, deadline, i -> i == 1 || i == 2 ? policy : null, i -> i > 0, b -> b));
            Member writer = members.get(0);
            Member storeA = members.get(1);
            Member storeB = members.get(2);
            List<Member> fetchers = List.copyOf(members.subList(3, members.size()));
            require(await(deadline, () -> writer.discovery().find(AssetCard.class,
                    c -> StoreChooser.KIND.equals(c.access().get("kind"))).size() == 2), "the writer sees both store cards");
            Path file = root.resolve("blob.bin");
            writeRandom(file, blobBytes, 31);
            PBlob blob = writer.pdt().blobRef("table").swap(b -> PBlob.of(file, b.store()));
            Files.deleteIfExists(file);
            writer.pdt().storeFor(blob.root(), StoreChooser.Need.of(0, 2), Duration.ofDays(1));
            require(await(deadline, () -> writer.pdt().stores().receipts(blob.root()).size() == 2), "two stores hold the blob");
            // the writer leaves, and the readers start once their views no longer hold it
            PeerId gone = writer.id();
            writer.close();
            members.remove(writer);
            fleet.stopPeer(0);
            require(await(deadline, () -> fetchers.stream().allMatch(f -> f.peer().runtime().membership().member(gone).isEmpty())),
                    "every reader's view drops the writer");
            long start = System.nanoTime();
            List<TreeFetch> fetches = new ArrayList<>();
            for (Member reader : fetchers) {
                fetches.add(reader.content().fetchTree(blob.root(), List.of(),
                        FetchOptions.defaults().withTimeout(deadline).prefer(storeA.id(), storeB.id())));
            }
            boolean complete = true;
            long end = start;
            int roundTrips = 0;
            List<Long> readerBytes = new ArrayList<>();
            Map<String, Long> bySource = new LinkedHashMap<>();
            Set<PeerId> readerIds = new HashSet<>();
            fetchers.forEach(f -> readerIds.add(f.id()));
            for (TreeFetch fetch : fetches) {
                long remaining = Math.max(1, start + deadline.toNanos() - System.nanoTime());
                complete &= fetch.await(Duration.ofNanos(remaining)) && fetch.complete();
                end = Math.max(end, System.nanoTime());
                TreeFetch.Progress p = fetch.progress();
                readerBytes.add(p.bytesReceived());
                roundTrips = Math.max(roundTrips, p.roundTrips());
                p.bytesBySource().forEach((peer, bytes) -> {
                    if (bytes == 0) {
                        return;
                    }
                    String name = peer.equals(storeA.id()) ? "store-a" : peer.equals(storeB.id()) ? "store-b"
                            : readerIds.contains(peer) ? "reader" : peer.equals(gone) ? "writer" : peer.display();
                    bySource.merge(name, bytes, Long::sum);
                });
            }
            return new StoreCold(blobBytes, fetchers.size(), Duration.ofNanos(Math.max(1, end - start)), flat.elapsed(), bySource,
                    readerBytes, roundTrips, complete);
        } finally {
            members.forEach(Member::close);
            deleteTree(root);
        }
    }

    // ------------------------------------------------------------ rrb-splice

    private static Rrb rrbRun(int peerCount, long elements, String binding, Duration deadline) throws Exception {
        Path root = Files.createTempDirectory("pdt-rrb");
        List<Member> members = new ArrayList<>();
        try (MiniFleet fleet = MiniFleet.start("pdt-rrb-splice", peerCount, Duration.ofMillis(100), binding)) {
            members.addAll(joinAll(fleet, root, deadline, i -> null, i -> false, b -> b));
            Member writer = members.get(0);
            List<Member> readers = members.subList(1, members.size());
            List<Ref<PVector<Long>>> refs = new ArrayList<>();
            for (Member m : members) {
                refs.add(m.pdt().vectorRef("transcript", LONG));
            }
            long started = System.nanoTime();
            PVector<Long> current = refs.get(0).swap(v -> {
                VectorEditor<Long> editor = v.editor();
                for (long i = 0; i < elements; i++) {
                    editor.conj(i);
                }
                return editor.done().value();
            });
            String whole = current.root();
            require(await(deadline, () -> readers.stream().allMatch(r -> r.pdt().store().complete(whole))),
                    "every reader holds the vector");
            long wholeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            long vectorBytes = bytesNotIn(writer.pdt().store(), whole, null);
            // a fetch adds its bytes to the facade's counter as it settles, just after its last node lands
            require(await(deadline, () -> readers.stream().allMatch(r -> r.pdt().stats().bytesReceived() >= vectorBytes)),
                    "every reader counts the vector's bytes");
            Random random = new Random(42);
            List<Splice> splices = new ArrayList<>();
            for (int s = 1; s <= SPLICES; s++) {
                long index = Math.floorMod(random.nextLong(), current.count() + 1);
                long value = -s;
                List<Long> before = received(readers);
                long t0 = System.nanoTime();
                PVector<Long> previous = current;
                current = refs.get(0).swap(v -> v.insertAt(index, value));
                String next = current.root();
                require(await(deadline, () -> readers.stream().allMatch(r -> r.pdt().store().complete(next))),
                        "every reader holds insertion " + index);
                require(await(deadline, () -> {
                    List<Long> now = received(readers);
                    for (int i = 0; i < now.size(); i++) {
                        if (now.get(i) <= before.get(i)) {
                            return false;
                        }
                    }
                    return true;
                }), "every reader counts insertion " + index);
                long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                List<Long> after = received(readers);
                List<Long> readerBytes = new ArrayList<>();
                for (int i = 0; i < before.size(); i++) {
                    readerBytes.add(after.get(i) - before.get(i));
                }
                Set<String> added = new HashSet<>(writer.pdt().store().reachable(next));
                added.removeAll(writer.pdt().store().reachable(previous.root()));
                long delta = 0;
                for (String cid : added) {
                    delta += writer.pdt().store().get(cid).orElseThrow().length;
                }
                Splice splice = new Splice(index, delta, added.size(), readerBytes, millis);
                System.out.printf("  insert at %d: delta %d bytes in %d nodes, readers %s%n", index, delta, added.size(), readerBytes);
                splices.add(splice);
            }
            return new Rrb(elements, vectorBytes, wholeMillis, splices, current.relaxed());
        } finally {
            members.forEach(Member::close);
            deleteTree(root);
        }
    }

    private static List<Long> received(List<Member> members) {
        return members.stream().map(m -> m.pdt().stats().bytesReceived()).toList();
    }

    // ------------------------------------------------------------ text

    private static Text textRun(int peerCount, String binding, Duration deadline) throws Exception {
        Path root = Files.createTempDirectory("pdt-text");
        List<Member> members = new ArrayList<>();
        try (MiniFleet fleet = MiniFleet.start("pdt-text", peerCount, Duration.ofMillis(100), binding)) {
            members.addAll(joinAll(fleet, root, deadline, i -> null, i -> false, b -> b.mode("notes", PRef.MODE_MULTI)));
            List<Ref<PMap<String, PText>>> refs = new ArrayList<>();
            for (int i = 0; i < members.size(); i++) {
                refs.add(members.get(i).pdt().mapRef("notes", STR, PText.codec(replica(i))));
            }
            PMap<String, PText> v1 = refs.get(0).swap(m -> m.assoc("doc", PText.empty(replica(0)).insert(0, BASE_TEXT)));
            require(await(deadline, () -> refs.stream().allMatch(r -> r.heads().roots().equals(List.of(v1.root())))),
                    "every member holds the first version");
            List<Long> before = received(members);
            Random random = new Random(47);
            List<List<Op>> scripts = List.of(new ArrayList<>(), new ArrayList<>());
            for (int e = 0; e < TEXT_EDITS; e++) {
                scripts.get(e % 2).add(Op.random(random));
            }
            // both replicas swap at once over the first version: two heads, each with half the edits
            AtomicReference<Throwable> failed = new AtomicReference<>();
            List<Thread> writers = new ArrayList<>();
            List<String> roots = java.util.Collections.synchronizedList(new ArrayList<>());
            java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
            for (int w = 0; w < 2; w++) {
                int replica = w;
                writers.add(Thread.ofPlatform().name("text-" + replica(w)).start(() -> {
                    try {
                        go.await();
                        roots.add(refs.get(replica).swap(m -> m.update("doc", t -> {
                            PText edited = t.withReplica(replica(replica));
                            for (Op op : scripts.get(replica)) {
                                edited = op.apply(edited);
                            }
                            return edited;
                        })).root());
                    } catch (Throwable t) {
                        failed.compareAndSet(null, t);
                    }
                }));
            }
            go.countDown();
            for (Thread t : writers) {
                t.join();
            }
            if (failed.get() != null) {
                throw new IllegalStateException("a replica's swap failed", failed.get());
            }
            require(await(deadline, () -> members.stream().allMatch(m -> roots.stream().allMatch(r -> m.pdt().store().complete(r)))
                    && refs.stream().allMatch(r -> new HashSet<>(r.heads().roots()).containsAll(roots))), "every member holds both heads");
            int heads = refs.get(2).heads().roots().size();
            List<Long> after = received(members);
            List<Long> memberBytes = new ArrayList<>();
            for (int i = 0; i < members.size(); i++) {
                memberBytes.add(after.get(i) - before.get(i));
            }
            double mergeMillis = 0;
            Set<String> texts = new HashSet<>();
            long length = 0;
            for (Ref<PMap<String, PText>> ref : refs) {
                long t0 = System.nanoTime();
                PText merged = ref.deref().get("doc").orElseThrow();
                mergeMillis = Math.max(mergeMillis, (System.nanoTime() - t0) / 1e6);
                texts.add(merged.toString());
                length = merged.length();
            }
            // one edit after the merge: the writer's swap folds both heads and ships the text node whole
            List<Long> beforeOne = received(members);
            PMap<String, PText> folded = refs.get(0).swap(m -> m.update("doc", t -> t.insert(0, "!")));
            require(await(deadline, () -> refs.stream().allMatch(r -> r.heads().roots().equals(List.of(folded.root())))),
                    "every member holds the edit after the merge");
            List<Long> afterOne = received(members);
            long oneEdit = 0;
            for (int i = 1; i < members.size(); i++) {
                oneEdit = Math.max(oneEdit, afterOne.get(i) - beforeOne.get(i));
            }
            return new Text(TEXT_EDITS, heads, memberBytes, mergeMillis, length, texts.size() == 1, oneEdit);
        } finally {
            members.forEach(Member::close);
            deleteTree(root);
        }
    }

    private static String replica(int member) {
        return member == 0 ? "alice" : member == 1 ? "bob" : "reader" + member;
    }

    /** One text edit drawn ahead of the run: a delete of 1 to 4 characters one time in four, else an insert of 1 to 8. */
    private record Op(boolean delete, double at, int length, String text) {
        static Op random(Random random) {
            if (random.nextInt(4) == 0) {
                return new Op(true, random.nextDouble(), 1 + random.nextInt(4), null);
            }
            StringBuilder s = new StringBuilder();
            for (int i = 1 + random.nextInt(8); i > 0; i--) {
                s.append((char) ('a' + random.nextInt(26)));
            }
            return new Op(false, random.nextDouble(), s.length(), s.toString());
        }

        PText apply(PText t) {
            if (delete) {
                if (t.length() == 0) {
                    return t;
                }
                long pos = (long) (at * t.length());
                return t.delete(pos, Math.min(t.length() - pos, length));
            }
            return t.insert((long) (at * (t.length() + 1)), text);
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
}
