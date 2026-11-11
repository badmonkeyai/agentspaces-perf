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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs each stress harness at toy scale over real TCP loopback, proving the
 * harnesses themselves stay healthy: fleets start, work flows, reports carry
 * data. Meaningful numbers come from the {@code main} entry points at their
 * default or larger shapes.
 */
class StressSmokeTest {

    @Test
    @Timeout(90)
    void throughputStressCompletesAllTasks() throws Exception {
        FleetThroughputStress.Report report = FleetThroughputStress.run(
                2, 1, 10, Duration.ofMillis(80), Duration.ofSeconds(60));

        assertThat(report.completed()).isEqualTo(10);
        assertThat(report.takeLatency().count()).isEqualTo(10);
        assertThat(report.takeLatency().max()).isGreaterThan(0.0);
    }

    @Test
    @Timeout(90)
    void propagationStressSeesEveryEntryEverywhere() throws Exception {
        GossipPropagationStress.Report report = GossipPropagationStress.run(2, 5);

        assertThat(report.propagated()).isEqualTo(5);
        assertThat(report.propagation().count()).isEqualTo(5);
    }

    @Test
    @Timeout(120)
    void churnRecoveryCatchesUpANewcomer() throws Exception {
        ChurnRecoveryStress.Report report = ChurnRecoveryStress.run(2, 10);

        assertThat(report.liveEntries()).isEqualTo(20);
        assertThat(report.caughtUp()).isTrue();
        assertThat(report.catchUp()).isLessThan(Duration.ofSeconds(120));
    }

    @Test
    @Timeout(180)
    void contentSwarmStressMovesTheArtifactToEveryFetcher() throws Exception {
        ContentSwarmStress.Report report = ContentSwarmStress.run(3, 6L * 1024 * 1024 + 321, 256 * 1024,
                Duration.ofSeconds(120));

        assertThat(report.fetchers()).hasSize(2).allMatch(ContentSwarmStress.FetcherReport::complete);
        assertThat(report.fetchers()).allMatch(f -> f.bytesFromSeeder() + f.bytesFromPeers() == report.bytes());
        assertThat(report.mebibytesPerSec()).isGreaterThan(0.0);
        assertThat(report.seederShare()).isBetween(0.0, 1.0);
    }

    @Test
    @Timeout(180)
    void contentSwarmStressRunsOnTheTlsBinding() throws Exception {
        ContentSwarmStress.Report report = ContentSwarmStress.run(2, 2L * 1024 * 1024 + 7, 256 * 1024, "tls",
                Duration.ofSeconds(120));

        assertThat(report.binding()).isEqualTo("tls");
        assertThat(report.fetchers()).singleElement().matches(ContentSwarmStress.FetcherReport::complete);
    }

    /** Whether the throughput floors of PD-Q-PRF-07 and PD-Q-PRF-08 are asserted (on unless {@code -Dperf.floors=false}). */
    private static boolean floors() {
        return !"false".equalsIgnoreCase(System.getProperty("perf.floors", "true"));
    }

    @Test
    @Timeout(240)
    void pdtStressMovesTheMapAndTheBlobDeltasToEveryReader() throws Exception {
        PdtStress.Report report = PdtStress.run(Set.of("map", "cold", "blob-edit", "blob-insert", "blob-append", "catch-up"), 3,
                8L * 1024 * 1024 + 123, "tcp", Duration.ofSeconds(120));
        System.out.print(report);

        assertThat(report.map()).hasSize(2);
        assertThat(report.map().get(1).maxReaderBytes()).as("PD-Q-PRF-01").isLessThanOrEqualTo(8 * 1024);
        assertThat(report.cold()).isNotNull();
        assertThat(report.cold().readerBytes()).hasSize(1).allMatch(b -> b >= report.blobBytes());
        assertThat(report.cold().flatElapsed()).isNotNull();
        // PD-Q-PRF-08: the cold tree fetch within 1.25 times the flat swarm's time for the same bytes
        // (0.12 at this scale on 2026-11-08); -Dperf.floors=false skips the bound on a slow CI host
        if (floors()) {
            assertThat(report.cold().ratio()).as("PD-Q-PRF-08: " + report.cold()).isLessThanOrEqualTo(1.25);
        }
        assertThat(report.edits()).hasSize(PdtStress.EDITS).allMatch(d -> d.maxReaderBytes() <= 2 * 256 * 1024 + 16 * 1024);
        // PDT 2 phase 4, P2-Z-03 and P2-Q-PRF-08: the members are at level 2, so a one-byte edit moves its frame and path;
        // the owner's ruling of 2026-11-09 restated the bound as a median under 8 KiB and every edit under 16 KiB
        assertThat(report.pdtLevel()).isEqualTo(2);
        assertThat(report.edits()).allMatch(d -> d.maxReaderBytes() < 16 * 1024, "P2-Q-PRF-08: every edit under 16 KiB");
        assertThat(report.editMedian()).as("P2-Q-PRF-08: the median edit").isLessThan(8 * 1024);
        // PDT 2 phase 7, P2-X-07: the inserts run on tcp as well as dtls, each within PD-Q-PRF-03's 256 KiB in two round trips
        assertThat(report.inserts()).hasSize(PdtStress.EDITS)
                .allMatch(d -> d.maxReaderBytes() <= 256 * 1024 && d.roundTrips() <= 2, "PD-Q-PRF-03: every insert");
        assertThat(report.appends()).hasSize(PdtStress.EDITS).allMatch(d -> d.maxReaderBytes() <= 1024 * 1024 + 256 * 1024 + 16 * 1024);
        assertThat(report.catchUp().complete()).isTrue();
        assertThat(report.catchUp().roundTrips()).isLessThanOrEqualTo(report.catchUp().depth() + 1);
    }

    @Test
    @Timeout(240)
    void pdtStressReadsAVerifiedWindowFromTheMiddleOfABlobItsReaderNeverHeld() throws Exception {
        // PDT 2 phase 4, P2-B-03 and P2-Q-PRF-05 at test scale: a 2 MiB window of a 64 MiB blob, its outboard one bo leaf
        PdtStress.Report report = PdtStress.run(Set.of("verified-window"), 2, 64L * 1024 * 1024 - 5, "tcp",
                Duration.ofSeconds(120));
        System.out.print(report);

        PdtStress.Window window = report.window();
        assertThat(window).isNotNull();
        assertThat(window.verified()).as("the window's bytes equal the writer's").isTrue();
        assertThat(window.length()).isEqualTo(PdtStress.WINDOW_BYTES);
        assertThat(window.chunkBytes()).as("the window and a chunk of overhang at each end")
                .isBetween((long) PdtStress.WINDOW_BYTES, PdtStress.WINDOW_BYTES + 2L * 256 * 1024 + 2 * 16 * 1024);
        // PDT 2 phase 4 measures, P2-Q-PRF-05 (owner ruling of 2026-11-09): a bo leaf covers 8 MiB in 32,728 bytes, so
        // the window across the 32 MiB seam pays two leaves and the root above them
        assertThat(window.outboardBytes()).as("two 8 MiB leaves and the root").isBetween(2L * 32_728, 2L * 32_728 + 1_024);
        assertThat(window.transferred()).isEqualTo(window.chunkBytes() + window.treeBytes() + window.outboardBytes());
        // P2-Q-PRF-05 as the owner restated it on 2026-11-09: the window plus 128 KiB in all, whatever the blob's size
        assertThat(window.transferred()).as("P2-Q-PRF-05").isLessThanOrEqualTo(window.length() + 128 * 1024L);
    }

    @Test
    @Timeout(120)
    void pdtStressRunsOnTheDtlsBinding() throws Exception {
        PdtStress.Report report = PdtStress.run(Set.of("map", "blob-insert"), 2, 4L * 1024 * 1024 + 7, "dtls",
                Duration.ofSeconds(90));
        System.out.print(report);

        assertThat(report.binding()).isEqualTo("dtls");
        assertThat(report.map().get(1).maxReaderBytes()).isLessThanOrEqualTo(8 * 1024);
        assertThat(report.inserts()).hasSize(PdtStress.EDITS).allMatch(d -> d.maxReaderBytes() <= 1024 * 1024);
    }

    @Test
    @Timeout(120)
    void fastCdcBenchChunksAndHashesAnArray() {
        List<ai.badmonkey.agentspaces.perf.bench.FastCdcBench.Result> results =
                ai.badmonkey.agentspaces.perf.bench.FastCdcBench.run(16, 1);

        // PDT 2 phase 4, P2-B-05: BouncyCastle's BLAKE3, then the outboard pass on 1, 2, 4, and all cores (PDT 2 phase 4
        // measures, owner ruling of 2026-11-09), each counting 16 KiB groups
        java.util.SortedSet<Integer> threads = new java.util.TreeSet<>(List.of(1, 2, 4, Runtime.getRuntime().availableProcessors()));
        assertThat(results).hasSize(4 + threads.size());
        assertThat(results).allMatch(r -> r.chunks() > 16 && r.mebibytesPerSec() > 0);
        assertThat(results.get(3).what()).isEqualTo("blake3");
        assertThat(results.subList(4, results.size())).extracting(ai.badmonkey.agentspaces.perf.bench.FastCdcBench.Result::what)
                .containsExactlyElementsOf(threads.stream().map(t -> "outboard x" + t).toList());
        assertThat(results.subList(4, results.size())).allMatch(r -> r.chunks() == 16 * 1024 / 16);
        // PD-Q-PRF-07: cuts plus sha256 at 300 MiB/s or better on one core (1,424 MiB/s on the
        // reference laptop at 1,024 MiB, 2026-11-08); -Dperf.floors=false skips it on a slow CI host
        if (floors()) {
            assertThat(results.get(1).mebibytesPerSec()).as("PD-Q-PRF-07: " + results.get(1))
                    .isGreaterThanOrEqualTo(300);
        }
        assertThat(results.get(0).chunks()).isEqualTo(results.get(1).chunks()).isEqualTo(results.get(2).chunks());
    }

    @Test
    @Timeout(240)
    void pdtStressPushesADeltaToEveryFollowerAndReconcilesTwoSetsOfRoots() throws Exception {
        // PDT 2 phase 5 at test scale: six followers of a 200 KB delta (P2-Q-PRF-04) and two sets of 100,000 roots
        // that differ by 1,000 (P2-K-02, P2-Q-PRF-06)
        PushReconcileStress.Report report = PushReconcileStress.run(Set.of("push", "reconcile"), 7, "tcp", Duration.ofSeconds(120));
        System.out.print(report);

        assertThat(report.push().complete()).as("every follower holds the root").isTrue();
        assertThat(report.push().fetched()).as("P2-P-03: no TREE_WANT").isZero();
        // P2-Q-PRF-04 as the owner restated it on 2026-11-09 (srad/agentspaces-pdt2.md §10.16): the run reports the
        // writer's eager edges and the GRAFTs that reached it, and (b) one eager copy per edge it held at the write
        assertThat(report.push().eagerEdges()).as("the writer holds at least one eager edge").isBetween(1, report.push().followers());
        assertThat(report.push().onePerEdge()).as("P2-Q-PRF-04 (b): " + report.push()).isTrue();
        assertThat(report.push().graftsAtWriter()).as("the GRAFTs that reached the writer are counted").isNotNegative();
        assertThat(report.push().graftAnswers()).as("P2-Q-PRF-04 (a), a figure: the writer's copies beyond its eager pushes")
                .isBetween(0L, (long) report.push().followers());
        // the restated (a) (ROBUSTNESS.md §10.11): the witness read every frame and split the GRAFTs at the writer
        assertThat(report.push().unread()).as("the witness read every frame of the push").isZero();
        assertThat(report.push().askedAfterOther() + report.push().askedWriterOnly())
                .as("the bundle's GRAFTs are among those that reached the writer").isBetween(0L, report.push().graftsAtWriter());
        assertThat(report.push().writerRatio()).as("at most one copy a follower").isLessThanOrEqualTo(report.push().followers() * 1.1);
        assertThat(report.reconciles()).hasSize(2);
        assertThat(report.reconciles()).allMatch(r -> r.have() == 500 && r.need() == 500);
        assertThat(report.reconciles().get(0).encoding()).isEqualTo("negentropy");
        assertThat(report.reconciles().get(1).encoding()).isEqualTo("riblt");
        assertThat(report.reconciles().get(1).rounds()).isLessThanOrEqualTo(3);
        assertThat(report.reconciles().get(1).bytes()).isLessThan(100 * 1024);
    }

    @Test
    @Timeout(300)
    void pdtRefStressHoldsABlobOnTwoStoresAfterItsWriterLeaves() throws Exception {
        // PDT 2 phase 8, P2-O-04 and P2-Q-PRF-02 at test scale: a writer, two stores, and two readers; an 8 MiB blob
        PdtRefStress.Report report = PdtRefStress.run(Set.of("store-cold"), 5, 8L * 1024 * 1024 + 11, 0, "tcp",
                Duration.ofSeconds(120));
        System.out.print(report);

        PdtRefStress.StoreCold cold = report.storeCold();
        assertThat(cold.complete()).as("every reader fetched the blob: " + cold).isTrue();
        assertThat(cold.readers()).isEqualTo(2);
        assertThat(cold.readerBytes()).allMatch(b -> b >= cold.blobBytes());
        assertThat(cold.bySource()).as("the writer had left").doesNotContainKey("writer");
        assertThat(cold.bySource().keySet()).isSubsetOf(Set.of("store-a", "store-b", "reader"));
        assertThat(cold.storeShare()).as("P2-Q-PRF-02: the stores serve the readers").isGreaterThan(0.5);
        assertThat(cold.flatElapsed()).isPositive();
    }

    @Test
    @Timeout(300)
    void pdtRefStressShipsAnInsertionInTheMiddleOfAVectorAsItsPath() throws Exception {
        // PDT 2 phase 8, P2-O-04 and P2-Q-PRF-07 at test scale: 200,000 elements, ten insertions
        PdtRefStress.Report report = PdtRefStress.run(Set.of("rrb-splice"), 3, 1, 200_000, "tcp", Duration.ofSeconds(120));
        System.out.print(report);

        PdtRefStress.Rrb rrb = report.rrb();
        assertThat(rrb.splices()).hasSize(PdtRefStress.SPLICES);
        assertThat(rrb.relaxed()).as("an insertion leaves the vector relaxed").isTrue();
        assertThat(rrb.maxDelta()).as("P2-Q-PRF-07: under 8 KiB at commit").isLessThan(8 * 1024);
        assertThat(rrb.splices()).allMatch(s -> s.readerBytes().stream().allMatch(b -> b > 0 && b < 16 * 1024),
                "every reader received about the delta");
    }

    @Test
    @Timeout(300)
    void pdtRefStressMergesTwoReplicasConcurrentTextEdits() throws Exception {
        // PDT 2 phase 8, P2-O-04 and P2-Q-PRF-09: two replicas, 1,000 edits, one swap each
        PdtRefStress.Report report = PdtRefStress.run(Set.of("text"), 3, 1, 0, "tcp", Duration.ofSeconds(120));
        System.out.print(report);

        PdtRefStress.Text text = report.text();
        assertThat(text.edits()).isEqualTo(PdtRefStress.TEXT_EDITS);
        assertThat(text.converged()).as("every member reads one text").isTrue();
        assertThat(text.bytesPerEdit()).as("P2-Q-PRF-09: under 50 bytes shipped per edit").isLessThan(50);
        if (floors()) {
            assertThat(text.mergeMillis()).as("P2-Q-PRF-09: the merge under 100 ms").isLessThan(100);
        }
    }

    @Test
    void pdtRefStressRefusesAValueAbove64MiB() {
        // the owner's ruling of 2026-11-09 (srad/agentspaces-pdt2.md §10.16): no measure moves more than 64 MiB
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> PdtRefStress.run(Set.of("store-cold"), 5, 65L * 1024 * 1024,
                0, "tcp", Duration.ofSeconds(10))).hasMessageContaining("64 MiB");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> PdtStress.run(Set.of("cold"), 3, 65L * 1024 * 1024, "tcp",
                Duration.ofSeconds(10))).hasMessageContaining("64 MiB");
    }
}
