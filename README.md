# AgentSpaces Perf

Performance testing, stress testing, and benchmarking for
[AgentSpaces](https://github.com/badmonkeyai/AgentSpaces). The project follows
the approach the Embabel project takes for its performance work:
[JMH](https://github.com/openjdk/jmh) microbenchmarks for the hot paths, kept
apart so measurement code never leaks into the shipped artifacts, plus plain
`main` harnesses for whole-fleet stress runs.

## Building

The build needs JDK 21 and Maven. It runs against the published AgentSpaces
libraries at the version `agentspaces.version` names in `pom.xml`. Until those
artifacts are on Maven Central, install the core into your local repository
first:

```bash
git clone https://github.com/badmonkeyai/AgentSpaces.git agentspaces
mvn -f agentspaces/pom.xml install -DskipTests
```

Then build this project:

```bash
mvn package
```

`mvn package` runs the smoke tests and writes `target/benchmarks.jar`, a
self-contained jar with the benchmarks, the stress harnesses, and every
dependency.

## Microbenchmarks (JMH)

The benchmarks cover the layers whose per-operation cost multiplies through
the whole fabric:

| Benchmark              | What it measures                                                |
|------------------------|-----------------------------------------------------------------|
| `CborCodecBenchmark`   | CBOR (RFC 8949) encode and decode, small records and 16 KiB blobs |
| `CryptoBenchmark`      | SHA-256, Ed25519 (RFC 8032) sign and verify, AES-256-GCM group-key seal and open, at 256 B, 4 KiB, and 64 KiB |
| `HlcBenchmark`         | Hybrid logical clock: local stamp, remote merge, encode, parse   |
| `TemplateBenchmark`    | Template matching, the inner loop of read, take, and notify routing |
| `LocalSpaceBenchmark`  | Space verbs at populations of 100 to 10,000 entries              |
| `CrdtBenchmark`        | Full-state, per-entry, and take-claim CRDT merges at 100 to 10,000 entries |
| `WireCodecBenchmark`   | A complete signed wire frame: encode plus sign, decode plus verify |

Run everything:

```bash
java -jar target/benchmarks.jar
```

Run one family, or pass any JMH option:

```bash
java -jar target/benchmarks.jar Cbor
java -jar target/benchmarks.jar -prof gc CrdtBenchmark
java -jar target/benchmarks.jar -l          # list the benchmarks
```

The class-level defaults (3 warmup and 5 measurement iterations, 1 fork) suit
a laptop; raise forks and iterations on the command line for publishable
numbers. Run JMH from the jar rather than `mvn exec:java`: each fork is a
fresh JVM that inherits the jar's classpath, which a Maven-hosted run cannot
pass on.

## Stress harnesses

The stress harnesses start a real fleet in one JVM: every peer runs its own
node, Ed25519 identity, TCP listener, gossip, and space replica, so results
include the full wire path. Every peer listens on `127.0.0.1` only. Each
harness reads its shape from system properties and prints a compact report
with latency percentiles. They run with no forks, so `mvn exec:java` suits
them (after a `mvn compile`); `FleetThroughputStress` is the default.

```bash
# Contended takes across the fleet: throughput and write-to-take latency
mvn exec:java \
  -Dexec.mainClass=ai.badmonkey.agentspaces.perf.stress.FleetThroughputStress \
  -Dpeers=3 -Dworkers=2 -Dtasks=200 -DsettleMs=100

# Rumor spread: time until a write is visible on every peer
mvn exec:java \
  -Dexec.mainClass=ai.badmonkey.agentspaces.perf.stress.GossipPropagationStress \
  -Dpeers=4 -Dentries=50

# Churn: a peer dies, writes continue, a newcomer joins and catches up
mvn exec:java \
  -Dexec.mainClass=ai.badmonkey.agentspaces.perf.stress.ChurnRecoveryStress \
  -Dpeers=3 -Dentries=100

# The content swarm: one seeder, every other peer fetching at once over TCP;
# aggregate MiB/s and the share of bytes the seeder served against the swarm
mvn exec:java \
  -Dexec.mainClass=ai.badmonkey.agentspaces.perf.stress.ContentSwarmStress \
  -Dpeers=8 -Dmebibytes=64 -DpieceKiB=1024 -Dbinding=tcp -DdeadlineSeconds=1800

# The persistent trees (srad/agentspaces-pdt.md PD-O-05, srad/agentspaces-pdt2.md
# P2-O-04): every mode, a 64 MiB blob across eight peers
MAVEN_OPTS=-Xmx4g mvn exec:java \
  -Dexec.mainClass=ai.badmonkey.agentspaces.perf.stress.PdtStress \
  -Dmodes=all -Dpeers=8 -Dmebibytes=64 -Dbinding=tcp -DdeadlineSeconds=1800

# The chunker and hasher (PD-Q-PRF-07, P2-B-05) over a 64 MiB array
mvn exec:java \
  -Dexec.mainClass=ai.badmonkey.agentspaces.perf.bench.FastCdcBench -Dmebibytes=64 -Dpasses=3
```

`ContentSwarmStress` (srad/large-networks-bulk-data.md LB-O-08) writes the
artifact to a temporary file, publishes it from peer 0 with a file-backed
piece store on every peer, and reports per fetcher how many bytes came from
the seeder and how many from the other fetchers; with one holder the seeder
share is 100%, and it falls as the fleet grows. `-Dbinding=tcp` (the default)
`tls` (the TLS binding with per-peer channel certificates), or `dtls` picks what
every peer listens on; `quic` lives in its own module and is not wired into
`MiniFleet`. Every ten seconds the run prints each fetcher's verified pieces, and a fetcher
that makes no progress for thirty seconds prints the tail of its trace.

Recorded on 2026-10-27 on one macOS laptop, 1 GiB in 1 MiB pieces, every peer in one JVM:

| Peers | Binding | Wall time | Aggregate | Seeder share |
|---|---|---|---|---|
| 2 | tcp | 2.3 s | 444 MiB/s | 100% |
| 8 | tcp | 17.1 s | 411 MiB/s | 26% |
| 2 | tls | 2.5 s | 418 MiB/s | 100% |
| 8 | tls | 18.4 s | 390 MiB/s | 27% |

Every fetcher served the others, and the seeder's share fell to about a quarter. LB-Q-PRF-01 asks
eight peers on one host to finish within 2.5 times the single-stream copy; the swarm took 7.4
times, so the row fails as written. Seven fetchers receive seven times the bytes of one, and the
eight-peer aggregate rate stays near the single stream's, so on one host the swarm's time grows
with the fetcher count. The first eight-peer run found a deadlock in the TCP and TLS readers, fixed
the same day (srad/large-networks-bulk-data.md 10.4).

`PdtStress` (srad/agentspaces-pdt.md PD-O-05) runs the measures of that
document's section 5.4 on a fleet of pack node stores on disk: peer 0 writes
and the others read. `-Dmodes` is `all` or a list of `map` (a 10,000-entry map
fetched whole, then one key changed and fetched by delta; PD-Q-PRF-01 and 06),
`cold` (the blob fetched by every reader at once, timed against
`ContentSwarmStress` moving the same bytes in 1 MiB pieces on the same binding;
PD-Q-PRF-08), `blob-edit` (ten one-byte edits in place; PD-Q-PRF-02),
`blob-insert` (ten inserts of 100 bytes; PD-Q-PRF-03), `blob-append` (ten
appends of 1 MiB; PD-Q-PRF-04), `catch-up` (a reader that held nothing
fetches the final version naming a base it lacks; PD-Q-PRF-06), and
`verified-window` (srad/agentspaces-pdt2.md P2-B-03, P2-Q-PRF-05: a member that
holds nothing of the blob reads 2 MiB from its middle through the writer's Bao
outboard, and the report counts every node it stored by kind). The report
prints each delta's bytes per reader and round trips, then one PASS or FAIL
line per row against its bound. The blob modes share one blob, and
`catch-up` and `verified-window` each need a peer that the other modes leave
out. Every member advertises the PDT feature level `-Dlevel` (2 by default):
at level 2 the writer keeps an intra-chunk delta frame per edit, so
`blob-edit` moves the frame and the changed path in place of the edited chunk
(P2-Z-03, P2-Q-PRF-08); `-Dlevel=1` moves the chunk whole, as before.

`PdtStress` hands the PDT 2 phase 5 modes to `PushReconcileStress`, which runs them on a fleet of
its own, since each member needs a ref space and a set of roots: `push` (srad/agentspaces-pdt2.md
P2-P-01 to P2-P-03, P2-Q-PRF-04: every member writes small heads first so the Plumtree settles, then
peer 0 pushes a 200 KB delta to every other member, and the report counts the copies of each bundle
the writer's bus sent, the fetches the followers made, and the fallbacks, and gives each follower's
delay and the swarm's GRAFT and repair counters during the push, ROBUSTNESS.md §10.5) and `reconcile` (P2-K-02,
P2-Q-PRF-06: two members reconcile two sets of 100,000 roots that differ by 1,000 at random places,
once by negentropy and once by RIBLT). Run them as `-Dmodes=push,reconcile -Dpeers=21`. The `push`
report checks P2-Q-PRF-04 as the owner restated it on 2026-11-09: (a) the writer's answers to askers
that had heard another member announce the bundle, against 0; and (b) the eager copies of each bundle
against the writer's eager edges, its view less its lazy members, counted just before and just after
the write. For (a), a witness reads every member's wire during the push (`AskerWitness`). It notes
which members heard the bundle announced by a member other than the writer, and it splits the GRAFTs
for the bundle that reach the writer into those askers and askers that heard the writer alone, which
the check allows (ROBUSTNESS.md §10.11). The report keeps every answer visible as a figure: the
bundle copies the writer sent beyond its eager pushes. A frame the witness cannot read counts against
the check, so it never passes on what the witness did not see.

`PdtStress` hands the PDT 2 phase 8 modes to `PdtRefStress` (P2-O-04), which gives every member a
ref facade and runs two content stores: `store-cold` (P2-Q-PRF-02: peer 0 writes a blob ref, pins it
on peers 1 and 2, and leaves once both receipts are in; every other peer then fetches the blob cold,
timed against `ContentSwarmStress` moving the same bytes from one seeder to as many fetchers, and the
report gives the stores' share of the bytes), `rrb-splice` (P2-Q-PRF-07: a vector ref of
`-Delements` integers, a million by default, followed by every other peer, then ten insertions at
random places, each delta and each reader's bytes), and `text` (P2-Q-PRF-09: two replicas edit one
text of a map ref at once, 1,000 edits in one swap each; the report gives the bytes each member
received per edit, the time of the first read that merges both heads, and what one edit after the
merge ships). No run moves a value above 64 MiB, the owner's ruling of 2026-11-09
(srad/agentspaces-pdt2.md §10.16): `PdtStress` and `PdtRefStress` refuse a larger blob, and the rows
that named 1 GiB or 10 GiB read as rates and ratios.

`FastCdcBench` (PD-Q-PRF-07) is a plain harness, since the figure is a byte
rate over one large input: the best of `-Dpasses` passes of the cuts alone,
the cuts plus a SHA-256 per chunk, and the whole `PBlob` build into a store
that keeps nothing, over a seeded array of `-Dmebibytes` or a file given as
`-Dfile`; over an array it also times BLAKE3 (P2-B-05): BouncyCastle's
`Blake3Digest` over the whole input, and `BaoOutboard.of`, the identity and the
`bo` nodes in one pass, which is what a publish hashes. Recorded on 2026-11-08 on one macOS laptop (10 cores, 32 GiB) over a
1,024 MiB array, best of three passes: cuts 2,567 MiB/s, cuts plus SHA-256
1,424 MiB/s, the whole blob build 1,268 MiB/s (14,369 chunks, mean 74,726
bytes); the row asks for 300 MiB/s.

Recorded on 2026-11-08 on the same laptop (`PdtStress`, `-Dmodes=all -Dpeers=8
-Dmebibytes=1024 -Dbinding=tcp -DdeadlineSeconds=1800`, `MAVEN_OPTS=-Xmx6g`,
run beside the reactor gate; the `dtls` run of 2026-11-09 follows the phase 4 table):

| Measure | Figure | Row and bound |
| --- | --- | --- |
| Flat swarm, 1,024 MiB in 1,024 KiB pieces to 7 peers | 21,644 ms, 331 MiB/s aggregate, seeder share 26% | `ContentSwarmStress` on the same host |
| Map, 10,000 entries whole | 2,202,321 bytes per reader, 2 round trips, 241 ms | |
| Map, one key of 10,000 | 4,239 bytes to the costliest reader, 2 round trips, 101 ms | PD-Q-PRF-01: 8,192 bytes, 2 round trips, PASS |
| Cold 1 GiB blob (14,414 chunks, height 3, 1,074,425,624 tree bytes) to 6 readers | 12,820 ms, 480 MiB/s aggregate, writer share 92%, 3 round trips | PD-Q-PRF-08: ratio 0.59 against the flat swarm's 21,644 ms, bound 1.25, PASS |
| Ten one-byte edits in place | median 98,902 bytes, max 126,796 bytes, 2 round trips, 5.5 to 10.1 s | PD-Q-PRF-02: median 131,072, max 540,672, PASS |
| Ten 100-byte inserts | median 83,180 bytes, max 122,774 bytes, 2 round trips, 7.3 to 8.7 s | PD-Q-PRF-03: median 262,144, PASS |
| Ten 1 MiB appends | max 1,168,605 bytes, 2 round trips, 7.1 to 8.8 s | PD-Q-PRF-04: 1,327,104 bytes, 2 round trips, PASS |
| Catch-up without the base | 1,763,519,106 bytes received for 1,084,919,060 tree bytes in 4 round trips, 5,010 ms, complete | PD-Q-PRF-06: depth 3 + 1 round trips, PASS (the bytes over the tree size are the concurrent sources' overlap; the row bounds round trips) |

Recorded on 2026-11-09 on the same laptop for PDT 2 phase 4 (srad/agentspaces-pdt2.md, phase 4
record), with other builds sharing the host:

| Measure | Figure | Row and bound |
| --- | --- | --- |
| `FastCdcBench`, 1,024 MiB, best of 3 | cuts 2,534 MiB/s; cuts plus SHA-256 1,383 MiB/s; blob build 1,170 MiB/s; BouncyCastle BLAKE3 280 MiB/s; outboard pass 543 MiB/s | PD-Q-PRF-07 PASS; P2-B-05 (1,024 MiB/s) FAIL for both BLAKE3 figures |
| Ten one-byte edits of a 1 GiB blob, level 2 (`-Dmodes=blob-edit -Dpeers=3 -Dmebibytes=1024`) | median 5,801 bytes, max 10,593 bytes per reader, 2 round trips; each frame 67 bytes, the rest the changed `b` nodes | P2-Z-03 and P2-Q-PRF-08 (8,192 bytes): 9 of 10 edits under, max FAIL; the same edits moved median 98,902 bytes at level 1 |
| A 2 MiB verified window from the middle of a 10 GiB blob (`-Dmodes=verified-window -Dpeers=2 -Dmebibytes=10240`) | 4,245,473 bytes moved: 2,132,842 of chunks, 11,390 of `b` nodes, 2,101,241 of `bo` nodes (the window straddles two 256 MiB outboard leaves); read in 126 ms; outboard built in 22.4 s | P2-Q-PRF-05 (2 MiB + 64 KiB) FAIL |

Recorded on 2026-11-09 on the same laptop for PDT 2 phase 7 (srad/agentspaces-pdt2.md P2-X-07):
`PdtStress` on `dtls` with every mode, `-Dmodes=all -Dpeers=8 -Dmebibytes=1024 -Dbinding=dtls
-DdeadlineSeconds=1800`, run from the shaded jar with `-Xmx6g` while other builds shared the host.
The whole run took 10 min 7 s. `catch-up` and `verified-window` each take a peer, so five readers
fetch the blob.

| Measure | Figure | Row and bound |
| --- | --- | --- |
| Flat swarm, 1,024 MiB in 1,024 KiB pieces to 7 peers | 45,729 ms, 118 MiB/s aggregate, seeder share 24% | `ContentSwarmStress` on `dtls` |
| Map, 10,000 entries whole | 2,202,321 bytes per reader, 2 round trips, 263 ms | |
| Map, one key of 10,000 | 4,239 bytes to the costliest reader, 2 round trips, 82 ms | PD-Q-PRF-01: 8,192 bytes, 2 round trips, PASS |
| Cold 1 GiB blob (14,414 chunks, height 3, 1,074,425,624 tree bytes) to 5 readers | 239,803 ms, 21.4 MiB/s aggregate, writer share 100%, 3 round trips, three stalls of about 60 s | PD-Q-PRF-08: ratio 5.24 against the flat swarm's 45,729 ms, bound 1.25, FAIL |
| Ten one-byte edits in place, level 2 | median 5,801 bytes, max 10,593 bytes, 2 round trips, 4.5 to 6.8 s | PD-Q-PRF-02 PASS; P2-Z-03 and P2-Q-PRF-08 (median under 8,192, each under 16,384) PASS |
| Ten 100-byte inserts | median 4,572 bytes, max 8,636 bytes, 2 round trips, 6.1 to 8.0 s | PD-Q-PRF-03: median 262,144, PASS |
| Ten 1 MiB appends | max 1,084,793 bytes, 2 round trips, 5.6 to 9.3 s | PD-Q-PRF-04: 1,327,104 bytes, 2 round trips, PASS |
| Catch-up without the base | 1,711,508,003 bytes received for 1,084,919,060 tree bytes in 5 round trips, 16,240 ms, complete | PD-Q-PRF-06: depth 3 + 1 round trips, FAIL by one (tcp took 4 on 2026-11-08) |
| A 2 MiB verified window from the middle | 2,313,397 bytes moved: 2,237,483 of chunks, 6,378 of `b` nodes, 69,536 of `bo` nodes; read in 2,484 ms; outboard built in 721 ms | P2-Q-PRF-05 (2,162,688 bytes) FAIL: 140,331 bytes of chunks past the window's ends |

The cold fetch stalled three times, about 60 s each, with every reader at the same byte count. A
second run of `cold` alone on six peers (`-Dmodes=cold -Dpeers=6`) printed each stalled reader's
trace, which the harness now does after 30 s without progress, as `ContentSwarmStress` does. It
stalled the same way: 194,774 ms against the flat swarm's 39,641 ms (ratio 4.91), and the traces
show the cause eleven times. Every reader asked the writer, its only source, timed out three times
in a row, and dropped the writer for 60 s: "source ... timed out 3 times; dropped for 60 s". That is the rule of ROBUSTNESS.md
§10.3 row B4, which the policy table keeps. In the first run the writer served every byte, so
nothing moved until the drop lapsed; in the second the readers served a third of the bytes and
still waited at every drop. Without the stalls the transfer took
about 60 s, close to the flat swarm's 46 s. The 64 MiB run on `dtls` (`-Dpeers=8 -Dmebibytes=64`)
had no stall: cold 3,750 ms against the flat swarm's 5,586 ms (ratio 0.67), catch-up in 4 round
trips, and the window 2,211,649 bytes, so the window's overhang shows at both sizes.

Recorded on 2026-11-09 and 10 on the same laptop for PDT 2 phase 8 (srad/agentspaces-pdt2.md
§10.18, P2-O-04), every value at 64 MiB or less, run from scratch copies of the jars while other
builds held the 1-minute load between 6.9 and 17.2:

| Mode | Binding, peers | Figure | Row and bound |
| --- | --- | --- | --- |
| `push`, three runs | tcp, 21 | the writer held 1, 1, and 0 eager edges and pushed 1, 1, and 0 copies; it sent 1, 0, and 4 bundle copies beyond them in answer to GRAFTs, 2.71, 1.67, and 4.98 x the delta; the last follower held the root after 1,703, 1,622, and 2,343 ms; the second run's followers made one fetch | P2-Q-PRF-04 (b) PASS in every run; (a) as the run measures it, 0 in one run of three (the simulation splits the answers, §10.18); P2-P-03 (no fetch) in two of three |
| `push` | dtls, 8 | 2 eager edges and 2 copies, no answers, 2.06 x the delta, the last follower after 105 ms | P2-Q-PRF-04 (a) and (b) PASS |
| `reconcile` | tcp, 2 | negentropy 14 rounds, 914,557 bytes, 435 ms; RIBLT 2 rounds, 97,644 bytes, 517 ms | P2-Q-PRF-06 PASS |
| `reconcile` | dtls, 2 | negentropy 14 rounds, 914,557 bytes; RIBLT 2 rounds, 97,656 bytes | P2-Q-PRF-06 PASS |
| `store-cold`, 64 MiB to 5 readers after the writer left | tcp, 8 | 803 ms against the flat swarm's 1,604 ms for the same bytes from one seeder to 5 fetchers; the stores served 100%; 2 round trips | P2-Q-PRF-02: ratio 0.50 (bound 1.25) PASS, share (bound 90%) PASS |
| the same | dtls, 8 | 2,111 ms against 4,542 ms | ratio 0.46 PASS, share 100% PASS |
| `verified-window`, 2 MiB of 64 MiB | tcp, 2 | 2,211,649 bytes moved: 2,140,147 of chunks, 5,246 of `b` nodes, 66,256 of `bo` nodes; 1.055 x the window | P2-Q-PRF-05 as restated (the window + 128 KiB, 2,228,224) PASS |
| the same | dtls, 8 | the same bytes, read in 710 ms | PASS |
| `rrb-splice`, ten insertions in 1,000,000 elements | tcp, 8 | each insertion 3,559 to 4,880 bytes in 8 to 10 nodes, and every reader received exactly that; the vector of 6,481,568 bytes followed whole in 4.4 s | P2-Q-PRF-07 (8 KiB) PASS |
| the same | dtls, 8 | the same deltas, 800 to 925 ms each | PASS |
| `text`, 1,000 concurrent edits by two replicas | tcp, 8 | 2 heads; a member received 28,635 bytes for both, 28.6 bytes per edit; the slowest first merging read 77.5 ms; one edit after the merge shipped the 28,140-byte `t` node whole | P2-Q-PRF-09 (100 ms, 50 bytes) PASS |
| the same | dtls, 8 | the same bytes; merge 54.6 ms | PASS |
| `push` with the witness, fifteen runs in three sets of five (2026-11-10, ROBUSTNESS.md §10.11) | tcp, 21 | all answers: 1 in one run, 0 in fourteen; answers to askers that had heard another member: the same 1; answers to askers that had heard the writer alone: 0; no frame unread; 4 to 14 eager edges, 4.10 to 14.36 x the delta; the last follower after 134 to 789 ms | P2-Q-PRF-04 (a) as restated PASS in 14 of 15, (b) PASS in 15 of 15 |
| `blob-edit`, ten one-byte edits of 64 MiB | tcp, 3 | level 2: median 3,323 bytes, max 5,487; level 1, the same edits: median 76,885, max 123,574; each edit at level 2 moved 0.021 to 0.072 of its level-1 bytes | P2-Z-03 and P2-Q-PRF-08 as restated (median 8 KiB, max 16 KiB, under a tenth of level 1) PASS |

The smoke suite asserts the PD-Q-PRF-08 ratio (at most 1.25) and the
PD-Q-PRF-07 floor (300 MiB/s on cuts plus SHA-256) on every run;
`-Dperf.floors=false` skips both on a slow host.

The same harnesses run from the jar, with the properties before `-cp`:

```bash
java -Dpeers=4 -Dentries=50 -cp target/benchmarks.jar \
  ai.badmonkey.agentspaces.perf.stress.GossipPropagationStress
```

## Tests

`mvn test` runs two smoke suites: one JMH
benchmark in process to prove the generated benchmark list works, and each
stress harness at toy scale to prove fleets start and reports carry data
(`PdtStress` on tcp over an 8 MiB blob with `map`, `cold`, `blob-edit`, `blob-insert` (since PDT 2
phase 7), `blob-append`, and `catch-up`, and on dtls over a 4 MiB blob, a verified window of a
blob just under 64 MiB, `FastCdcBench` over 16 MiB, `push` and `reconcile` on seven peers, and since
PDT 2 phase 8 `store-cold` over 8 MiB on five peers, `rrb-splice` over 200,000 elements, `text`,
and the refusal of a value above 64 MiB). They verify the harnesses rather than the numbers.

## Contributing

Contributions are welcome under the Developer Certificate of Origin; see
[CONTRIBUTING.md](CONTRIBUTING.md) and the [Code of Conduct](CODE_OF_CONDUCT.md).
Report vulnerabilities privately, as [SECURITY.md](SECURITY.md) describes.

## License

Copyright 2026 Bad Monkey, Inc.

AgentSpaces Perf is an open source project from Bad Monkey, Inc., licensed
under the Apache License, Version 2.0; see `LICENSE`.
