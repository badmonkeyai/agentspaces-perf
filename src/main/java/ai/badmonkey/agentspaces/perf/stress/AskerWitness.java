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

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;

import java.io.IOException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Reads P2-Q-PRF-04 (a) as the owner restated it off the wire of every member of a {@code push} run
 * (srad/agentspaces-pdt2.md §10.16; ROBUSTNESS.md §10.11, "What stays open", item 2): the writer answers no
 * {@code GRAFT} for a bundle from an asker that had heard another member announce that bundle. An answer to an asker
 * that had heard only the writer is allowed.
 *
 * <p>The witness wraps each member's binding. Once armed, it notes each announcement of an item of the pushed stream
 * that a member takes from anyone but the writer, in a {@code HAVE} or on a probe's {@code PING} or {@code ACK}. When a
 * member sends the writer a {@code GRAFT} for an item, the witness notes whether that member had taken such an
 * announcement of the item. When the {@code GRAFT} reaches the writer, which answers each one with the bundle, the
 * witness counts it on one side or the other. A {@code BATCH} or a {@code RELAY_FRAME}, which a {@link MiniFleet}
 * does not send, a frame the witness cannot decode, and a {@code GRAFT} at the writer whose departure the witness did
 * not see, count as unread, so the restated check cannot pass on what the witness did not read. The witness changes no
 * frame and drops none, and reads nothing before it is armed.
 */
final class AskerWitness {

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private final String streamId;
    private volatile PeerId writer;
    /** Member and item: the member took an announcement of the item from a member other than the writer. */
    private final Set<String> heardOther = ConcurrentHashMap.newKeySet();
    /** Asker and item: for each GRAFT on its way to the writer, whether the asker had heard another member. */
    private final Map<String, Queue<Boolean>> asks = new ConcurrentHashMap<>();
    private final AtomicLong afterOther = new AtomicLong();
    private final AtomicLong writerOnly = new AtomicLong();
    private final AtomicLong unread = new AtomicLong();

    /**
     * A witness of one stream.
     *
     * @param streamId the stream the pushed bundles travel on
     */
    AskerWitness(String streamId) {
        this.streamId = Objects.requireNonNull(streamId, "streamId");
    }

    /** From now on, reads the frames of every wrapped member for the bundles {@code writer} pushes. */
    void arm(PeerId writer) {
        this.writer = Objects.requireNonNull(writer, "writer");
    }

    /** The GRAFTs that reached the writer from an asker that had heard another member announce the item. */
    long afterOther() {
        return afterOther.get();
    }

    /** The GRAFTs that reached the writer from an asker that had heard no member but the writer announce the item. */
    long writerOnly() {
        return writerOnly.get();
    }

    /** The frames the witness could not read while armed, and the GRAFTs at the writer whose departure it did not see. */
    long unread() {
        return unread.get();
    }

    /**
     * Wraps a member's binding, so that every connection it dials or accepts passes its frames by the witness.
     *
     * @param self      the member
     * @param transport the member's binding
     * @return the binding the member listens and dials on
     */
    Transport wrap(PeerId self, Transport transport) {
        return new Transport() {
            @Override
            public String scheme() {
                return transport.scheme();
            }

            @Override
            public Set<Capability> capabilities() {
                return transport.capabilities();
            }

            @Override
            public TransportConnection dial(String address) throws IOException {
                return new Watched(self, transport.dial(address));
            }

            @Override
            public TransportConnection dial(String address, PeerId target) throws IOException {
                return new Watched(self, transport.dial(address, target));
            }

            @Override
            public boolean signalled() {
                return transport.signalled();
            }

            @Override
            public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
                return transport.listen(bindAddress, connection -> onAccept.accept(new Watched(self, connection)));
            }
        };
    }

    /** A connection of one member whose frames the witness reads on their way in and out. */
    private final class Watched implements TransportConnection {
        private final PeerId self;
        private final TransportConnection inner;

        Watched(PeerId self, TransportConnection inner) {
            this.self = self;
            this.inner = inner;
        }

        @Override
        public String remoteAddress() {
            return inner.remoteAddress();
        }

        @Override
        public java.util.Optional<PeerId> attestedPeer() {
            return inner.attestedPeer();
        }

        @Override
        public java.security.cert.X509Certificate[] remoteChain() {
            return inner.remoteChain();
        }

        @Override
        public void send(byte[] frame) throws IOException {
            read(() -> sent(self, frame));
            inner.send(frame);
        }

        @Override
        public void onReceive(Consumer<byte[]> receiver) {
            inner.onReceive(frame -> {
                read(() -> taken(self, frame));
                receiver.accept(frame);
            });
        }

        @Override
        public void onClose(Runnable listener) {
            inner.onClose(listener);
        }

        @Override
        public void close() {
            inner.close();
        }
    }

    /** Reads a frame once armed; a body the witness cannot read counts as unread, and the frame goes on. */
    private void read(Runnable reading) {
        if (writer == null) {
            return;
        }
        try {
            reading.run();
        } catch (RuntimeException unreadable) {
            unread.incrementAndGet();
        }
    }

    private static String key(PeerId member, byte[] hash) {
        return member.value() + " " + HexFormat.of().formatHex(hash);
    }

    private static Envelope open(byte[] frame) {
        WireCodec.Signed signed = CODEC.fromWire(frame, WireCodec.Signed.class);
        return signed == null ? null : signed.envelope();
    }

    /** A member sends a frame: a GRAFT to the writer for an item of the stream records what the asker had heard. */
    private void sent(PeerId self, byte[] frame) {
        PeerId at = writer;
        Envelope envelope = open(frame);
        if (envelope == null || envelope.kind() != Envelope.Kind.GRAFT || !at.equals(envelope.to())) {
            return;
        }
        Bodies.Graft graft = CODEC.fromWire(envelope.body(), Bodies.Graft.class);
        if (streamId.equals(graft.streamId()) && graft.hash() != null) {
            String ask = key(self, graft.hash());
            asks.computeIfAbsent(ask, k -> new ConcurrentLinkedQueue<>()).add(heardOther.contains(ask));
        }
    }

    /** A member takes a frame: announcements from others than the writer, and the GRAFTs that reach the writer. */
    private void taken(PeerId self, byte[] frame) {
        PeerId at = writer;
        Envelope envelope = open(frame);
        if (envelope == null) {
            return;
        }
        switch (envelope.kind()) {
            case HAVE -> heard(self, envelope.from(), CODEC.fromWire(envelope.body(), Bodies.Have.class).items());
            case PING -> heard(self, envelope.from(), CODEC.fromWire(envelope.body(), Bodies.Ping.class).have());
            case ACK -> heard(self, envelope.from(), CODEC.fromWire(envelope.body(), Bodies.Ack.class).have());
            case GRAFT -> {
                Bodies.Graft graft = CODEC.fromWire(envelope.body(), Bodies.Graft.class);
                if (self.equals(at) && streamId.equals(graft.streamId()) && graft.hash() != null) {
                    Queue<Boolean> pending = asks.get(key(envelope.from(), graft.hash()));
                    Boolean other = pending == null ? null : pending.poll();
                    (other == null ? unread : other ? afterOther : writerOnly).incrementAndGet();
                }
            }
            case BATCH, RELAY_FRAME -> unread.incrementAndGet();
            default -> {
                // no announcement and no ask
            }
        }
    }

    private void heard(PeerId self, PeerId from, List<Bodies.HaveItem> items) {
        if (items == null || from.equals(writer)) {
            return;
        }
        for (Bodies.HaveItem item : items) {
            if (streamId.equals(item.streamId()) && item.hash() != null) {
                heardOther.add(key(self, item.hash()));
            }
        }
    }
}
