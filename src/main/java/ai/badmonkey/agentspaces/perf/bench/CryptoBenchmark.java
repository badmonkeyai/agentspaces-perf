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

import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
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
import java.security.KeyPair;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * The cryptographic floor under the fabric: SHA-256 (CIDs and PeerIDs), Ed25519
 * signing and verification (RFC 8032; every wire frame and every signed entry),
 * and AES-256-GCM sealing under a group key (private space payloads). Payload
 * size is a parameter because frames run small while content-addressed blocks
 * run large.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class CryptoBenchmark {

    /** Payload size in bytes: a small frame, an average entry, a full block. */
    @Param({"256", "4096", "65536"})
    public int size;

    private byte[] payload;
    private KeyPair keys;
    private byte[] rawPublicKey;
    private byte[] signature;
    private GroupKey groupKey;
    private byte[] sealed;
    private byte[] aad;

    /** Builds the payload, keys, and precomputed signature and ciphertext. */
    @Setup
    public void setup() {
        System.out.println("signature provider: " + Ed25519.providerName());
        payload = new byte[size];
        new Random(42).nextBytes(payload);
        keys = Ed25519.generate();
        rawPublicKey = Ed25519.rawPublicKey(keys.getPublic());
        signature = Ed25519.sign(keys.getPrivate(), payload);
        groupKey = GroupKey.generate();
        aad = "space:bench".getBytes(StandardCharsets.UTF_8);
        sealed = groupKey.encrypt(payload, aad);
    }

    /** SHA-256 over the payload, the CID and PeerID digest. */
    @Benchmark
    public byte[] sha256() {
        return Digests.sha256(payload);
    }

    /** Ed25519 signature over the payload. */
    @Benchmark
    public byte[] ed25519Sign() {
        return Ed25519.sign(keys.getPrivate(), payload);
    }

    /** Ed25519 verification of a valid signature. */
    @Benchmark
    public boolean ed25519Verify() {
        return Ed25519.verify(keys.getPublic(), payload, signature);
    }

    /**
     * Verification against a raw 32-byte key, the form the wire and CRDT
     * verification sites hold. Today this includes a per-call JDK key
     * reconstruction; the phase 4 key cache targets exactly the difference
     * between this number and {@link #ed25519Verify()}.
     */
    @Benchmark
    public boolean ed25519VerifyRaw() {
        return Ed25519.verifyRaw(rawPublicKey, payload, signature);
    }

    /** AES-256-GCM sealing under the group key. */
    @Benchmark
    public byte[] groupKeySeal() {
        return groupKey.encrypt(payload, aad);
    }

    /** AES-256-GCM opening of a valid ciphertext. */
    @Benchmark
    public Optional<byte[]> groupKeyOpen() {
        return groupKey.decrypt(sealed, aad);
    }
}
