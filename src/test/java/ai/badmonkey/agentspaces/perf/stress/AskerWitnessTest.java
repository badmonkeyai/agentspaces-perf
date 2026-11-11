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
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * P2-Q-PRF-04 (a) as the owner restated it (srad/agentspaces-pdt2.md §10.16, ROBUSTNESS.md §10.11 "What stays open"
 * item 2): the writer may answer a GRAFT from an asker that had heard only the writer announce the bundle, and answers
 * none from an asker that had heard another member announce it. {@link AskerWitness} reads that split off the wire of
 * each member, and the push report checks the restated count while it keeps every answer visible.
 */
class AskerWitnessTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final WireCodec WIRE = new WireCodec(CODEC);
    private static final GroupId GROUP = GroupId.fromFounding("asker-witness".getBytes(StandardCharsets.UTF_8));
    private static final String STREAM = "refs/plans";
    private static final byte[] BUNDLE = {1, 2, 3};
    private static final byte[] SECOND = {4, 5, 6};

    private final PeerIdentity writer = PeerIdentity.generate();
    private final PeerIdentity asker = PeerIdentity.generate();
    private final PeerIdentity other = PeerIdentity.generate();

    /** One end of a connection: the frames its node sent, and the receiver the node registered. */
    static final class End implements TransportConnection {
        final List<byte[]> sent = new ArrayList<>();
        Consumer<byte[]> receiver;

        @Override
        public String remoteAddress() {
            return "test";
        }

        @Override
        public void send(byte[] frame) {
            sent.add(frame);
        }

        @Override
        public void onReceive(Consumer<byte[]> receiver) {
            this.receiver = receiver;
        }

        @Override
        public void close() {
        }
    }

    /** A binding whose every dial returns the same end. */
    static final class OneEnd implements Transport {
        final End end = new End();

        @Override
        public String scheme() {
            return "tcp";
        }

        @Override
        public TransportConnection dial(String address) {
            return end;
        }

        @Override
        public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) {
            onAccept.accept(end);
            return () -> { };
        }
    }

    private static byte[] frame(PeerIdentity from, PeerId to, Envelope.Kind kind, Object body) {
        return WIRE.encode(new Envelope(WireCodec.WIRE_VERSION, GROUP, kind, from.peerId(), to,
                new HlcTimestamp(1, 0, "n"), CODEC.toBytes(body)), from);
    }

    private static Bodies.Have have(byte[] hash) {
        return new Bodies.Have(List.of(new Bodies.HaveItem(STREAM, hash)));
    }

    /** One member's wire: the wrapped connection its node uses, the end under it, and the frames its node took. */
    private record Side(TransportConnection connection, End end, List<byte[]> taken) {
        static Side of(AskerWitness witness, PeerIdentity self) throws Exception {
            OneEnd binding = new OneEnd();
            TransportConnection connection = witness.wrap(self.peerId(), binding).dial("peer");
            List<byte[]> taken = new ArrayList<>();
            connection.onReceive(taken::add);
            return new Side(connection, binding.end, taken);
        }

        /** A frame arrives on this member's wire. */
        void arrives(byte[] frame) {
            end.receiver.accept(frame);
        }
    }

    /** The asker's GRAFT for {@code hash} leaves its wire and reaches the writer's. */
    private static void graft(Side askerSide, Side writerSide, PeerIdentity from, PeerId to, byte[] hash) throws Exception {
        byte[] graft = frame(from, to, Envelope.Kind.GRAFT, new Bodies.Graft(STREAM, hash));
        askerSide.connection().send(graft);
        writerSide.arrives(graft);
    }

    @Test
    void anAskerThatHeardOnlyTheWriterIsCountedApartFromOneThatHeardAnotherMember() throws Exception {
        AskerWitness witness = new AskerWitness(STREAM);
        Side askerSide = Side.of(witness, asker);
        Side writerSide = Side.of(witness, writer);
        witness.arm(writer.peerId());

        // the writer alone announced the bundle: the restated check allows the answer
        askerSide.arrives(frame(writer, asker.peerId(), Envelope.Kind.HAVE, have(BUNDLE)));
        graft(askerSide, writerSide, asker, writer.peerId(), BUNDLE);
        assertThat(witness.writerOnly()).isEqualTo(1);
        assertThat(witness.afterOther()).isZero();

        // another member announced the second bundle on its probe's acknowledgement before the asker asked the writer
        askerSide.arrives(frame(writer, asker.peerId(), Envelope.Kind.HAVE, have(SECOND)));
        askerSide.arrives(frame(other, asker.peerId(), Envelope.Kind.ACK,
                new Bodies.Ack(7, null, null, null, List.of(new Bodies.HaveItem(STREAM, SECOND)))));
        graft(askerSide, writerSide, asker, writer.peerId(), SECOND);
        assertThat(witness.afterOther()).as("an answer the restated check forbids").isEqualTo(1);
        assertThat(witness.writerOnly()).isEqualTo(1);
        assertThat(witness.unread()).isZero();
        assertThat(askerSide.taken()).as("the witness hands every frame on").hasSize(3);
        assertThat(writerSide.taken()).hasSize(2);
    }

    @Test
    void anAnnouncementOnAProbeOfAnotherStreamOrBeforeTheArmingCountsForNothing() throws Exception {
        AskerWitness witness = new AskerWitness(STREAM);
        Side askerSide = Side.of(witness, asker);
        Side writerSide = Side.of(witness, writer);

        askerSide.arrives(frame(other, asker.peerId(), Envelope.Kind.HAVE, have(BUNDLE)));
        witness.arm(writer.peerId());
        askerSide.arrives(frame(other, asker.peerId(), Envelope.Kind.PING,
                new Bodies.Ping(3, null, null, List.of(new Bodies.HaveItem("refs/warm-1", SECOND)))));
        graft(askerSide, writerSide, asker, writer.peerId(), BUNDLE);
        graft(askerSide, writerSide, asker, writer.peerId(), SECOND);
        assertThat(witness.writerOnly()).as("what the asker heard before the push does not count").isEqualTo(2);
        assertThat(witness.afterOther()).isZero();

        // a probe that names the bundle does count
        askerSide.arrives(frame(other, asker.peerId(), Envelope.Kind.PING,
                new Bodies.Ping(4, null, null, List.of(new Bodies.HaveItem(STREAM, BUNDLE)))));
        graft(askerSide, writerSide, asker, writer.peerId(), BUNDLE);
        assertThat(witness.afterOther()).isEqualTo(1);
    }

    @Test
    void aGraftToAnotherMemberIsNoAnswerOfTheWritersAndWhatTheWitnessCannotReadIsCounted() throws Exception {
        AskerWitness witness = new AskerWitness(STREAM);
        Side askerSide = Side.of(witness, asker);
        Side writerSide = Side.of(witness, writer);
        witness.arm(writer.peerId());

        askerSide.connection().send(frame(asker, other.peerId(), Envelope.Kind.GRAFT, new Bodies.Graft(STREAM, BUNDLE)));
        assertThat(witness.writerOnly() + witness.afterOther()).isZero();

        // a GRAFT at the writer whose ask the witness never saw leave, and a batch it does not open
        writerSide.arrives(frame(other, writer.peerId(), Envelope.Kind.GRAFT, new Bodies.Graft(STREAM, BUNDLE)));
        writerSide.arrives(frame(other, writer.peerId(), Envelope.Kind.BATCH, new Bodies.Batch(List.of())));
        assertThat(witness.unread()).as("the restated check cannot pass on frames the witness did not read").isEqualTo(2);
        assertThat(askerSide.end().sent).as("the witness sends every frame on").hasSize(1);
        assertThat(writerSide.taken()).hasSize(2);
    }

    @Test
    void thePushReportChecksTheRestatedCountAndKeepsEveryAnswerVisible() {
        PushReconcileStress.Push allowed = push(3, 0, 3, 0);
        String text = new PushReconcileStress.Report("tcp", 21, allowed, List.of()).toString();
        assertThat(allowed.restatedCheck()).isTrue();
        assertThat(text).contains("(a) answers to askers that had heard another member announce the bundle: 0 (bound 0) PASS")
                .contains("all answers 3 (bundle copies beyond the eager pushes), 3 to askers that had heard the writer alone");

        assertThat(push(3, 1, 2, 0).restatedCheck()).as("one answer after another announcer").isFalse();
        assertThat(push(0, 0, 0, 1).restatedCheck()).as("a frame the witness did not read").isFalse();
        assertThat(new PushReconcileStress.Report("tcp", 21, push(3, 1, 2, 0), List.of()).toString())
                .contains("announce the bundle: 1 (bound 0) FAIL");
    }

    private static PushReconcileStress.Push push(long allAnswers, long afterOther, long writerOnly, long unread) {
        return new PushReconcileStress.Push(20, 200_000, 1, 8, 2_400_000, 0, 0, 150, true, List.of(100L, 150L),
                Map.of("GRAFT", 4L), 8, 8, 4, allAnswers, afterOther, writerOnly, unread);
    }
}
