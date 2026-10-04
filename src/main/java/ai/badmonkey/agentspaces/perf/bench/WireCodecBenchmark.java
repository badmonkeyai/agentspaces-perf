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
package ai.badmonkey.agentspaces.perf.bench;

import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.time.InstantSource;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Complete wire-frame cost: CBOR encoding plus the Ed25519 signature on the
 * way out, CBOR decoding plus verification on the way in. This is the price of
 * one gossip frame end to end, so dividing a peer's CPU budget by these
 * numbers gives its frame-rate ceiling.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class WireCodecBenchmark {

    /** Envelope body size in bytes: a digest line, a rumor, a fat delta. */
    @Param({"128", "1024", "8192"})
    public int bodySize;

    private WireCodec codec;
    private PeerIdentity identity;
    private Envelope envelope;
    private byte[] frame;
    private byte[] bareFrame;
    private java.util.Optional<ai.badmonkey.agentspaces.common.id.PeerId> attested;

    /** Builds the codec, identity, sample envelope, and encoded frame. */
    @Setup
    public void setup() {
        codec = new WireCodec();
        identity = PeerIdentity.generate();
        byte[] body = new byte[bodySize];
        new Random(42).nextBytes(body);
        GroupId group = GroupId.fromFounding(
                "bench-group".getBytes(StandardCharsets.UTF_8));
        HybridLogicalClock clock =
                new HybridLogicalClock(InstantSource.system(), identity.peerId().value());
        envelope = new Envelope(2, group, Envelope.Kind.RUMOR,
                identity.peerId(), identity.peerId(), clock.now(), body);
        frame = codec.encode(envelope, identity);
        bareFrame = codec.encodeBare(envelope);
        attested = java.util.Optional.of(identity.peerId());
    }

    /** Encodes and signs one envelope. */
    @Benchmark
    public byte[] encodeAndSign() {
        return codec.encode(envelope, identity);
    }

    /** Decodes and verifies one frame. */
    @Benchmark
    public Optional<Envelope> decodeAndVerify() {
        return codec.decode(frame);
    }

    /** Encodes one channel-attested frame: CBOR alone, no signature (spec §5.6). */
    @Benchmark
    public byte[] encodeBare() {
        return codec.encodeBare(envelope);
    }

    /** Decodes one channel-attested frame against a matching attestation. */
    @Benchmark
    public Optional<Envelope> decodeBareAttested() {
        return codec.decode(bareFrame, attested);
    }
}
