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

import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.blob.BaoOutboard;
import ai.badmonkey.agentspaces.pdt.blob.Blake3;
import ai.badmonkey.agentspaces.pdt.kinds.BoNode;
import ai.badmonkey.agentspaces.pdt.blob.FastCdc;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.peering.blocks.BlockStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

/**
 * The chunker and hasher on one core (srad/agentspaces-pdt.md PD-Q-PRF-07: a
 * 1 GiB file chunked and hashed at 300 MiB/s or better). A plain harness rather
 * than a JMH benchmark because the figure is a byte rate over one large input
 * and the work per operation is a whole pass. The measures over a seeded
 * random input, each the best of the given passes after one warm-up:
 *
 * <ul>
 * <li>{@code cut}: {@link FastCdc#cuts} alone, the rolling Gear hash with the
 * pinned masks and normalization.</li>
 * <li>{@code cut+sha256}: the cuts plus a SHA-256 of every chunk, which is the
 * work that names each leaf.</li>
 * <li>{@code blob}: {@link PBlob#of(InputStream, NodeStore)} into a store that
 * keeps nothing, so the figure includes the CID encoding, the prolly grouping,
 * and the interior nodes.</li>
 * <li>{@code blake3} (PDT 2 phase 4, P2-B-04, P2-B-05, P2-Q-PRF-10): the BLAKE3 hash of
 * the whole input by BouncyCastle's {@code Blake3Digest}; its count is the 16 KiB chunk
 * groups.</li>
 * <li>{@code outboard xN} (P2-B-05): {@link BaoOutboard#of(InputStream, int)}, the identity
 * and the {@code bo} nodes in one pass through this module's compression, which is what a
 * publish hashes, on N threads for N in 1, 2, 4, and every core (PDT 2 phase 4 measures, the
 * owner's ruling of 2026-11-09: parallel pure Java, the target restated per host, for example
 * 1 GiB/s on 4 cores); its count is the groups.</li>
 * </ul>
 *
 * <p>The input is an array of {@code mebibytes} (256 by default; 1,024 needs a
 * heap above 1 GiB, for example {@code -Xmx3g}) or, with {@code -Dfile=path},
 * a file streamed through the chunker's bounded buffer.
 */
public final class FastCdcBench {

    /** One measure. */
    public record Result(String what, long bytes, int chunks, double mebibytesPerSec, long bestMillis) {
        @Override
        public String toString() {
            return String.format(Locale.ROOT, "%-12s %6d MiB in %6d ms: %7.1f MiB/s (%d chunks, mean %d bytes)", what,
                    bytes >> 20, bestMillis, mebibytesPerSec, chunks, chunks == 0 ? 0 : bytes / chunks);
        }
    }

    private FastCdcBench() {
    }

    /**
     * Runs the measures over a seeded random array.
     *
     * @param mebibytes the input size
     * @param passes    measured passes per measure, the best reported
     * @return the results
     */
    public static List<Result> run(int mebibytes, int passes) {
        byte[] data = new byte[mebibytes << 20];
        new Random(7).nextBytes(data);
        List<Result> out = new ArrayList<>();
        out.add(measure("cut", data.length, passes, () -> FastCdc.cuts(data).size()));
        out.add(measure("cut+sha256", data.length, passes, () -> hashChunks(data)));
        out.add(measure("blob", data.length, passes, () -> PBlob.of(data, discarding()).chunks().size()));
        out.add(measure("blake3", data.length, passes, () -> {
            Blake3.digest(data);
            return (int) BoNode.groups(data.length);
        }));
        for (int threads : outboardThreads()) {
            out.add(measure("outboard x" + threads, data.length, passes,
                    () -> (int) BoNode.groups(BaoOutboard.of(new ByteArrayInputStream(data), threads).size())));
        }
        return out;
    }

    /** The thread counts of the outboard measures: 1, 2, 4, and every core of the host, in order and once each. */
    public static List<Integer> outboardThreads() {
        return List.copyOf(new java.util.TreeSet<>(List.of(1, 2, 4, Runtime.getRuntime().availableProcessors())));
    }

    /**
     * Runs the streaming measures over a file: the chunker's bounded buffer over a
     * channel, with and without the hash, and the blob build.
     *
     * @param file   the file
     * @param passes measured passes per measure, the best reported
     * @return the results
     * @throws IOException when the file cannot be read
     */
    public static List<Result> run(Path file, int passes) throws IOException {
        long bytes = Files.size(file);
        List<Result> out = new ArrayList<>();
        out.add(measure("cut", bytes, passes, () -> {
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
                int chunks = 0;
                Iterator<byte[]> it = FastCdc.chunks(channel);
                while (it.hasNext()) {
                    it.next();
                    chunks++;
                }
                return chunks;
            }
        }));
        out.add(measure("cut+sha256", bytes, passes, () -> {
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
                MessageDigest digest = sha256();
                int chunks = 0;
                Iterator<byte[]> it = FastCdc.chunks(channel);
                while (it.hasNext()) {
                    digest.digest(it.next());
                    chunks++;
                }
                return chunks;
            }
        }));
        out.add(measure("blob", bytes, passes, () -> {
            try (InputStream in = Files.newInputStream(file)) {
                return PBlob.of(in, discarding()).chunks().size();
            }
        }));
        return out;
    }

    private interface Pass {
        int run() throws IOException;
    }

    private static Result measure(String what, long bytes, int passes, Pass pass) {
        try {
            int chunks = pass.run();
            long best = Long.MAX_VALUE;
            for (int i = 0; i < passes; i++) {
                long started = System.nanoTime();
                chunks = pass.run();
                best = Math.min(best, System.nanoTime() - started);
            }
            double seconds = best / 1e9;
            Result result = new Result(what, bytes, chunks, bytes / (1024.0 * 1024.0) / seconds, best / 1_000_000);
            System.out.println(result);
            return result;
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static int hashChunks(byte[] data) {
        MessageDigest digest = sha256();
        int chunks = 0;
        int offset = 0;
        for (int end : FastCdc.cuts(data)) {
            digest.update(data, offset, end - offset);
            digest.digest();
            offset = end;
            chunks++;
        }
        return chunks;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A node store that accepts every node and keeps none, so the build is measured without the store's cost. */
    static NodeStore discarding() {
        return new NodeStore(new BlockStore() {
            @Override
            public Optional<byte[]> get(String cid, long nowMillis) {
                return Optional.empty();
            }

            @Override
            public boolean put(String cid, byte[] bytes, long nowMillis) {
                return true;
            }

            @Override
            public long remove(String cid) {
                return 0;
            }

            @Override
            public Set<String> cids() {
                return Set.of();
            }

            @Override
            public long lastAccessMillis(String cid) {
                return 0;
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public long bytesUsed() {
                return 0;
            }

            @Override
            public String kind() {
                return "discarding";
            }
        }, InstantSource.system());
    }

    public static void main(String[] args) throws IOException {
        int passes = Integer.getInteger("passes", 3);
        String file = System.getProperty("file");
        List<Result> results = file == null ? run(Integer.getInteger("mebibytes", 64), passes) : run(Path.of(file), passes);
        Result hashed = results.get(1);
        System.out.printf(Locale.ROOT, "PD-Q-PRF-07: chunk and hash at %.1f MiB/s on one core (bound 300) %s%n",
                hashed.mebibytesPerSec(), hashed.mebibytesPerSec() >= 300 ? "PASS" : "FAIL");
        if (results.size() > 3) {
            System.out.printf(Locale.ROOT, "P2-B-04: BouncyCastle's Blake3Digest at %.1f MiB/s on one core%n",
                    results.get(3).mebibytesPerSec());
            for (Result outboard : results.subList(4, results.size())) {
                boolean bounded = outboard.what().equals("outboard x4");
                System.out.printf(Locale.ROOT, "P2-B-05: %s at %.1f MiB/s%s%n", outboard.what(), outboard.mebibytesPerSec(),
                        bounded ? " (bound 1024 on 4 cores) " + (outboard.mebibytesPerSec() >= 1024 ? "PASS" : "FAIL") : "");
            }
        }
    }
}
