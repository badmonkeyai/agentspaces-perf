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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * CBOR encode and decode cost (RFC 8949 via Jackson). Every entry write, every
 * gossip frame body, and every advertisement goes through this codec, so its
 * per-record cost multiplies through the whole fabric.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class CborCodecBenchmark {

    /** A typical small task entry payload. */
    public record TaskDoc(String topic, int priority, String assignee) {
    }

    /** A payload approaching the 64 KiB inline limit's working sizes. */
    public record BlobDoc(String id, byte[] data) {
    }

    private CborCodec codec;
    private TaskDoc small;
    private byte[] smallBytes;
    private BlobDoc blob;
    private byte[] blobBytes;

    /** Builds the codec and sample documents. */
    @Setup
    public void setup() {
        codec = CborCodec.defaultCodec();
        small = new TaskDoc("indexing", 3, "worker-7");
        smallBytes = codec.toBytes(small);
        byte[] data = new byte[16 * 1024];
        new Random(42).nextBytes(data);
        blob = new BlobDoc("blob-1", data);
        blobBytes = codec.toBytes(blob);
    }

    /** Encodes a three-field record. */
    @Benchmark
    public byte[] encodeSmall() {
        return codec.toBytes(small);
    }

    /** Decodes a three-field record. */
    @Benchmark
    public TaskDoc decodeSmall() {
        return codec.fromBytes(smallBytes, TaskDoc.class);
    }

    /** Encodes a record carrying a 16 KiB binary payload. */
    @Benchmark
    public byte[] encodeBlob16KiB() {
        return codec.toBytes(blob);
    }

    /** Decodes a record carrying a 16 KiB binary payload. */
    @Benchmark
    public BlobDoc decodeBlob16KiB() {
        return codec.fromBytes(blobBytes, BlobDoc.class);
    }
}
