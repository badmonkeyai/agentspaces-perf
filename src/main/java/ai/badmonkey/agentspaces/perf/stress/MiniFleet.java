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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * A multi-peer fleet in one JVM over real TCP loopback: each peer runs its own
 * node, group runtime, and replicated space, exactly as separate processes
 * would, so stress runs exercise the full wire path including signing,
 * gossip, and anti-entropy. Peers can leave and join mid-run for churn tests.
 */
public final class MiniFleet implements AutoCloseable {

    /**
     * One running peer.
     *
     * @param identity the peer's Ed25519 identity
     * @param node     the peer node
     * @param runtime  the joined group runtime
     * @param space    the peer's replica of the shared space
     * @param port     the TCP listen port
     */
    public record Peer(PeerIdentity identity, PeerNode node, GroupRuntime runtime,
                       ReplicatedSpace space, int port) {

        void shutdown() {
            space.close();
            node.close();
        }
    }

    private static final Duration TICK = Duration.ofMillis(250);

    private final GroupAdvertisement group;
    private final Duration settleWindow;
    private final List<Peer> peers = new ArrayList<>();
    private int seedPort;

    private MiniFleet(GroupAdvertisement group, Duration settleWindow) {
        this.group = group;
        this.settleWindow = settleWindow;
    }

    /**
     * Starts a fleet.
     *
     * @param groupName    the group's founding name
     * @param peerCount    how many peers to start
     * @param settleWindow the take settle window for every replica
     * @return the running fleet
     * @throws Exception when a peer fails to start
     */
    public static MiniFleet start(String groupName, int peerCount, Duration settleWindow)
            throws Exception {
        GroupId groupId = GroupId.fromFounding(groupName.getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement group = new GroupAdvertisement(
                "aspace://" + groupId.value(), PeerIdentity.generate().peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(365), groupName,
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        MiniFleet fleet = new MiniFleet(group, settleWindow);
        for (int i = 0; i < peerCount; i++) {
            fleet.addPeer();
        }
        return fleet;
    }

    /**
     * Starts one more peer, seeded at the fleet's first surviving peer.
     *
     * @return the new peer
     * @throws Exception when the peer fails to start
     */
    public synchronized Peer addPeer() throws Exception {
        int port = freePort();
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), "127.0.0.1:" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(group,
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        ReplicatedSpace space = ReplicatedSpace
                .builder(runtime, "work", identity, "agent-" + peers.size())
                .settleWindow(settleWindow)
                .build();
        node.startTicking(TICK);
        Peer peer = new Peer(identity, node, runtime, space, port);
        peers.add(peer);
        if (seedPort == 0) {
            seedPort = port;
        }
        return peer;
    }

    /**
     * Shuts one peer down mid-run, simulating a crash departure.
     *
     * @param index the peer's index
     */
    public synchronized void stopPeer(int index) {
        Peer peer = peers.remove(index);
        peer.shutdown();
        if (peer.port() == seedPort) {
            seedPort = peers.isEmpty() ? 0 : peers.get(0).port();
        }
    }

    /**
     * Returns a running peer.
     *
     * @param index the peer's index
     * @return the peer
     */
    public Peer peer(int index) {
        return peers.get(index);
    }

    /** Returns how many peers are running. */
    public int size() {
        return peers.size();
    }

    @Override
    public synchronized void close() {
        for (int i = peers.size() - 1; i >= 0; i--) {
            peers.get(i).shutdown();
        }
        peers.clear();
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
