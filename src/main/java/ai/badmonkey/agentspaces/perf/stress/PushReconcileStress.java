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
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.MemoryPieceStore;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.refs.PushPublisher;
import ai.badmonkey.agentspaces.pdt.refs.PushReceiver;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * The {@code PdtStress} modes of PDT 2 phase 5 over a real fleet ({@code tcp}, {@code tls}, or {@code dtls}):
 *
 * <ul>
 * <li>{@code push} (P2-P-01 to P2-P-03, P2-Q-PRF-04): every member follows a map ref with push on; every member
 * writes small heads of its own first, so the Plumtree settles as under a fleet's own traffic; then member 0 writes a
 * delta of {@link #PUSH_DELTA_BYTES} and every follower must hold the root without a fetch. P2-Q-PRF-04 is the
 * owner's two checks of 2026-11-09 (srad/agentspaces-pdt2.md §10.16): (a) the writer answers no {@code GRAFT} for a
 * bundle another member has announced, which the run reads as the bundle copies the writer sent beyond its eager
 * pushes, its {@code RUMOR} bytes during the push in whole bundles less the eager copies, a bound that also counts an
 * answer to a member that heard the writer alone; and (b) the writer pushes one copy of each bundle per eager edge it
 * holds at the write, the edge count reported. The {@code GRAFT} frames that reached the writer, and the writer's
 * bytes over the delta's, stay in the report as figures.</li>
 * <li>{@code reconcile} (P2-K-02, P2-K-05, P2-Q-PRF-06): members 0 and 1 reconcile two sets of
 * {@link #RECONCILE_SET} roots that differ by {@link #RECONCILE_DIFFERENCE} at random places, once by negentropy and
 * once by RIBLT, through {@code Content.reconcileHeld}.</li>
 * </ul>
 */
public final class PushReconcileStress {

    /** The modes this class runs. */
    public static final Set<String> MODES = Set.of("push", "reconcile");
    /** The pushed delta of scenario 6: 200 KB. */
    public static final int PUSH_DELTA_BYTES = 200_000;
    /** The roots each reconciled set holds (P2-K-02). */
    public static final int RECONCILE_SET = 100_000;
    /** The roots the two sets differ by, half on each side (P2-K-02). */
    public static final int RECONCILE_DIFFERENCE = 1_000;

    private static final Codec<String> STR = Codec.of(String.class);

    /**
     * The push mode's figures.
     *
     * @param followers      the full followers
     * @param deltaBytes     the node bytes of the pushed delta
     * @param bundles        the bundles of the delta
     * @param copies         the copies of a bundle the writer's bus pushed eagerly on the ref's stream
     * @param writerBytes    every byte the writer's node sent from the write until every follower held the root
     * @param fetched        deltas the followers fetched instead
     * @param fallbacks      announced pushes that did not complete within the grace
     * @param millis         from the write until every follower held the root
     * @param complete       whether every follower held the root before the deadline
     * @param followerMillis from the write until each follower held the root, ascending; -1 for one that never did
     * @param repairs        the swarm's GRAFT frames and repair counters during the push (swarm retries, ROBUSTNESS.md
     *                       §10.2), summed over every member
     * @param eagerEdges     the members the writer's bus pushes to, its view less its lazy members, just before the write
     * @param eagerEdgesAfter the same just after the write, which published the bundles; a PRUNE that lands between the
     *                        two moves the count, so the copies of a bundle lie between them
     * @param graftsAtWriter the GRAFT frames that reached the writer during the push, for any item
     * @param graftAnswers   the bundle copies the writer sent beyond its eager pushes: its RUMOR bytes during the push,
     *                       in whole bundles, less the eager copies
     */
    public record Push(int followers, long deltaBytes, long bundles, long copies, long writerBytes, long fetched, long fallbacks,
                       long millis, boolean complete, List<Long> followerMillis, Map<String, Long> repairs, int eagerEdges,
                       int eagerEdgesAfter, long graftsAtWriter, long graftAnswers) {

        /** P2-Q-PRF-04 (b): one copy of each bundle per eager edge the writer held when it published. */
        public boolean onePerEdge() {
            return copies >= (long) Math.min(eagerEdges, eagerEdgesAfter) * bundles
                    && copies <= (long) Math.max(eagerEdges, eagerEdgesAfter) * bundles;
        }
        /** The writer's cost as a multiple of the delta: every byte it sent over the delta's bytes. */
        public double writerRatio() {
            return deltaBytes == 0 ? Double.NaN : (double) writerBytes / deltaBytes;
        }
    }

    /**
     * One reconcile's figures.
     *
     * @param encoding {@code negentropy}, {@code riblt}, or {@code riblt,negentropy}
     * @param have     roots the asker holds and the other lacks
     * @param need     roots the other holds and the asker lacks
     * @param rounds   the rounds of the session
     * @param bytes    the bytes of every want and answer
     * @param millis   the session's wall time
     */
    public record Reconcile(String encoding, int have, int need, int rounds, long bytes, long millis) {
    }

    /**
     * The run's figures; a mode that did not run leaves its field null or empty.
     *
     * @param binding    the binding
     * @param peers      the members
     * @param push       the push mode
     * @param reconciles the reconcile mode, negentropy then RIBLT
     */
    public record Report(String binding, int peers, Push push, List<Reconcile> reconciles) {

        private static String verdict(boolean pass) {
            return pass ? "PASS" : "FAIL";
        }

        @Override
        public String toString() {
            StringBuilder out = new StringBuilder(String.format("pdt stress, phase 5: %d peers on %s%n", peers, binding));
            if (push != null) {
                out.append(String.format("  push: %d bytes in %d bundles to %d followers in %d ms, %s; %d eager copies, the writer"
                                + " sent %d bytes, %d fetched, %d fallbacks%n", push.deltaBytes(), push.bundles(), push.followers(),
                        push.millis(), push.complete() ? "every follower holds it" : "INCOMPLETE", push.copies(),
                        push.writerBytes(), push.fetched(), push.fallbacks()));
                out.append(String.format("  P2-Q-PRF-04 (a) bundle copies the writer sent in answer to GRAFTs: %d (bound 0) %s,"
                                + " %d GRAFTs reached it; (b) eager copies %d for %d eager edges (%d after the write) x %d bundles %s;"
                                + " the writer sent %.2f x the delta; P2-P-03 TREE_WANTs %d (bound 0) %s%n", push.graftAnswers(),
                        verdict(push.graftAnswers() == 0), push.graftsAtWriter(), push.copies(), push.eagerEdges(),
                        push.eagerEdgesAfter(), push.bundles(), verdict(push.onePerEdge()),
                        push.writerRatio(), push.fetched(), verdict(push.fetched() == 0)));
                out.append(String.format("  each follower held the root after (ms): %s%n  the swarm's repairs during the push: %s%n",
                        push.followerMillis(), push.repairs()));
            }
            for (Reconcile r : reconciles) {
                out.append(String.format("  reconcile by %s: %d of %d roots differ (%d have, %d need) in %d rounds, %d bytes,"
                                + " %d ms%n", r.encoding(), r.have() + r.need(), RECONCILE_SET, r.have(), r.need(), r.rounds(),
                        r.bytes(), r.millis()));
            }
            if (!reconciles.isEmpty()) {
                Reconcile riblt = reconciles.get(reconciles.size() - 1);
                out.append(String.format("  P2-Q-PRF-06 restated: RIBLT %d rounds (bound 3) %s, %d bytes (bound 102400) %s%n",
                        riblt.rounds(), verdict(riblt.rounds() <= 3), riblt.bytes(), verdict(riblt.bytes() < 100 * 1024)));
            }
            return out.toString();
        }
    }

    private record Member(MiniFleet.Peer peer, Content content, ReplicatedSpace contentSpace, ReplicatedSpace refSpace,
                          Pdt pdt, CapabilityRuntime capabilities) {
        void close() {
            pdt.close();
            capabilities.close();
            content.close();
            refSpace.close();
            contentSpace.close();
        }
    }

    private PushReconcileStress() {
    }

    /**
     * Runs the modes.
     *
     * @param modes     {@link #MODES} or a part of it
     * @param peerCount the members: the writer and its followers for {@code push}, at least 2
     * @param binding   {@code tcp}, {@code tls}, or {@code dtls}
     * @param deadline  the hard stop of each wait
     * @return the report
     * @throws Exception when the fleet fails to start
     */
    public static Report run(Set<String> modes, int peerCount, String binding, Duration deadline) throws Exception {
        if (peerCount < 2) {
            throw new IllegalArgumentException("at least two peers");
        }
        if (!MODES.containsAll(modes)) {
            throw new IllegalArgumentException("modes are " + MODES + ", got " + modes);
        }
        Push push = null;
        List<Reconcile> reconciles = new ArrayList<>();
        if (modes.contains("push")) {
            push = pushRun(peerCount, binding, deadline);
        }
        if (modes.contains("reconcile")) {
            reconciles.addAll(reconcileRun(binding, deadline));
        }
        return new Report(binding, peerCount, push, reconciles);
    }

    private static Member join(MiniFleet.Peer peer, int index, CborCodec codec, Duration deadline,
                               Supplier<? extends java.util.Collection<String>> reconcileSet) {
        BlockExchange blocks = new BlockExchange(peer.runtime(), codec);
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(peer.runtime(), "content", peer.identity(), "content")
                .settleWindow(Duration.ofMillis(100)).blocks(blocks).build();
        ReplicatedSpace refSpace = ReplicatedSpace.builder(peer.runtime(), "refs", peer.identity(), "refs")
                .settleWindow(Duration.ofMillis(100)).build();
        DiscoveryService discovery = new DiscoveryService(peer.runtime(), new AdCache(codec, InstantSource.system()), codec,
                peer.identity().peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(peer.runtime(), discovery, peer.identity());
        NodeStore store = new NodeStore(new MemoryBlockStore(), InstantSource.system());
        Pdt pdt = Pdt.builder(refSpace).store(store).loadTimeout(deadline).collectEvery(Duration.ZERO)
                .push(PushPublisher.Config.on()).build();
        Content.Builder builder = Content.builder(new CapabilityPipes(peer.runtime(), codec), peer.runtime(), contentSpace,
                        peer.identity().peerId())
                .codec(codec).store(new MemoryPieceStore(InstantSource.system(), 0)).blocks(blocks).random(new Random(index))
                .discovery(discovery).treeStore(pdt.treeStore()).pdtLevel(Content.PDT_LEVEL);
        if (reconcileSet != null) {
            builder.reconcileSet(reconcileSet);
        }
        Content content = builder.build();
        capabilities.register(content);
        pdt.attach(content).start();
        peer.runtime().onTick(pdt::tick);
        return new Member(peer, content, contentSpace, refSpace, pdt, capabilities);
    }

    private static void awaitFleet(List<Member> members, Duration deadline) throws InterruptedException {
        boolean ready = await(Duration.ofSeconds(Math.min(120, Math.max(30, deadline.toSeconds()))), () -> {
            for (Member from : members) {
                if (from.peer().runtime().membership().allMembers().size() < members.size() - 1) {
                    return false;
                }
                for (Member to : members) {
                    if (from != to && from.content().pdtLevel(to.peer().identity().peerId()) < Content.PDT_LEVEL) {
                        return false;
                    }
                }
            }
            return true;
        });
        if (!ready) {
            throw new IllegalStateException("the fleet did not form: not every member reads every other at pdt level 2");
        }
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

    private static PMap<String, String> grow(PMap<String, String> base, int bytes, long seed) {
        Random random = new Random(seed);
        PMap<String, String> out = base;
        char[] chars = new char[100];
        for (int i = 0; i * 120 < bytes; i++) {
            for (int c = 0; c < chars.length; c++) {
                chars[c] = (char) ('a' + random.nextInt(26));
            }
            out = out.assoc("plan-" + i, new String(chars));
        }
        return out;
    }

    private static Push pushRun(int peerCount, String binding, Duration deadline) throws Exception {
        CborCodec codec = CborCodec.defaultCodec();
        List<Member> members = new ArrayList<>();
        try (MiniFleet fleet = MiniFleet.start("pdt-push", peerCount, Duration.ofMillis(100), binding)) {
            for (int i = 0; i < peerCount; i++) {
                members.add(join(fleet.peer(i), i, codec, deadline, null));
            }
            awaitFleet(members, deadline);
            List<Ref<PMap<String, String>>> refs = new ArrayList<>();
            for (Member m : members) {
                refs.add(m.pdt().mapRef("plans", STR, STR));
            }
            // every member's own small heads settle the tree, as a fleet's own traffic does
            for (int round = 0; round < 3; round++) {
                for (int i = 0; i < members.size(); i++) {
                    int n = round;
                    members.get(i).pdt().mapRef("warm-" + i, STR, STR).swap(m -> m.assoc("n", Integer.toString(n)));
                }
                Thread.sleep(1_500);
            }
            Member writer = members.get(0);
            Map<String, Long> repairsBefore = repairs(members);
            long sentBefore = writer.peer().node().traffic().bytesSent();
            long graftsBefore = graftsReceived(writer);
            long rumorBytesBefore = rumorBytesSent(writer);
            int eagerEdges = eagerEdges(writer);
            long start = System.nanoTime();
            PMap<String, String> big = refs.get(0).swap(m -> grow(m, PUSH_DELTA_BYTES, 7));
            int eagerEdgesAfter = eagerEdges(writer);
            List<Member> followers = members.subList(1, members.size());
            long[] heldAt = new long[followers.size()];
            java.util.Arrays.fill(heldAt, -1);
            boolean complete = await(deadline, () -> {
                boolean all = true;
                for (int i = 0; i < heldAt.length; i++) {
                    if (heldAt[i] < 0 && followers.get(i).pdt().store().complete(big.root())) {
                        heldAt[i] = (System.nanoTime() - start) / 1_000_000;
                    }
                    all &= heldAt[i] >= 0;
                }
                return all;
            });
            long millis = (System.nanoTime() - start) / 1_000_000;
            long writerBytes = writer.peer().node().traffic().bytesSent() - sentBefore;
            long graftsAtWriter = graftsReceived(writer) - graftsBefore;
            long rumorBytes = rumorBytesSent(writer) - rumorBytesBefore;
            Thread.sleep(1_000); // the last settles reach the counters
            PushPublisher.Stats pushed = writer.pdt().refs().pushStats();
            long copies = writer.peer().runtime().gossip().rumorsSentByStream()
                    .getOrDefault(PushReceiver.stream("refs", "plans"), 0L);
            long fetched = followers.stream().mapToLong(f -> f.pdt().stats().deltasFetched()).sum();
            long fallbacks = followers.stream().mapToLong(f -> f.pdt().refs().pushStats().fallbacks()).sum();
            Map<String, Long> repairs = repairs(members);
            repairs.replaceAll((name, total) -> total - repairsBefore.getOrDefault(name, 0L));
            List<Long> followerMillis = java.util.Arrays.stream(heldAt).sorted().boxed().toList();
            return new Push(followers.size(), pushed.bytesPushed(), pushed.bundlesSent(), copies, writerBytes, fetched, fallbacks,
                    millis, complete, followerMillis, repairs, eagerEdges, eagerEdgesAfter, graftsAtWriter,
                    Math.max(0, rumorBytes * pushed.bundlesSent() / Math.max(1, pushed.bytesPushed()) - copies));
        } finally {
            members.forEach(Member::close);
        }
    }

    /** The members the writer's bus pushes a rumor to: every member of its view that it does not hold lazy. */
    private static int eagerEdges(Member writer) {
        Set<ai.badmonkey.agentspaces.common.id.PeerId> lazy = writer.peer().runtime().gossip().lazyPeers();
        return (int) writer.peer().runtime().membership().allMembers().stream().filter(m -> !lazy.contains(m.id())).count();
    }

    /** The bytes of the RUMOR frames a member sent; a bundle travels in a frame of its own, too large for a batch. */
    private static long rumorBytesSent(Member member) {
        return member.peer().node().traffic().bytesSentByKind().getOrDefault(ai.badmonkey.agentspaces.peering.wire.Envelope.Kind.RUMOR, 0L);
    }

    /** The GRAFT frames a member received, batched ones included. */
    private static long graftsReceived(Member member) {
        return member.peer().node().traffic().receivedByKind().getOrDefault(ai.badmonkey.agentspaces.peering.wire.Envelope.Kind.GRAFT, 0L);
    }

    /** The GRAFT frames and the repair counters of every member's bus, summed (swarm retries, ROBUSTNESS.md §10.2). */
    private static Map<String, Long> repairs(List<Member> members) {
        Map<String, Long> out = new java.util.LinkedHashMap<>();
        for (Member m : members) {
            out.merge("GRAFT", m.peer().runtime().gossip().lazyFramesSent("GRAFT"), Long::sum);
            m.peer().runtime().gossip().repairCounters().forEach((name, count) -> out.merge(name, count, Long::sum));
        }
        return out;
    }

    private static String root(int i) {
        return Cids.of(("pdt-stress:" + i).getBytes(StandardCharsets.UTF_8), Cids.DAG_CBOR);
    }

    private static List<Reconcile> reconcileRun(String binding, Duration deadline) throws Exception {
        Random random = new Random(42);
        Set<Integer> onlyA = new HashSet<>();
        Set<Integer> onlyB = new HashSet<>();
        while (onlyA.size() < RECONCILE_DIFFERENCE / 2) {
            onlyA.add(random.nextInt(RECONCILE_SET));
        }
        while (onlyB.size() < RECONCILE_DIFFERENCE / 2) {
            int i = random.nextInt(RECONCILE_SET);
            if (!onlyA.contains(i)) {
                onlyB.add(i);
            }
        }
        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        for (int i = 0; i < RECONCILE_SET; i++) {
            if (!onlyB.contains(i)) {
                a.add(root(i));
            }
            if (!onlyA.contains(i)) {
                b.add(root(i));
            }
        }
        CborCodec codec = CborCodec.defaultCodec();
        List<Member> members = new ArrayList<>();
        try (MiniFleet fleet = MiniFleet.start("pdt-reconcile", 2, Duration.ofMillis(100), binding)) {
            members.add(join(fleet.peer(0), 0, codec, deadline, () -> a));
            members.add(join(fleet.peer(1), 1, codec, deadline, () -> b));
            awaitFleet(members, deadline);
            List<Reconcile> out = new ArrayList<>();
            for (int expected : new int[]{0, RECONCILE_DIFFERENCE}) {
                long start = System.nanoTime();
                Content.Reconciliation r = members.get(0).content()
                        .reconcileHeld(members.get(1).peer().identity().peerId(), expected)
                        .get(deadline.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
                out.add(new Reconcile(r.encoding(), r.have().size(), r.need().size(), r.rounds(), r.bytes(),
                        (System.nanoTime() - start) / 1_000_000));
            }
            return out;
        } finally {
            members.forEach(Member::close);
        }
    }
}
