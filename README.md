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
  -Dpeers=8 -Dmebibytes=1024 -DpieceKiB=1024 -Dbinding=tcp -DdeadlineSeconds=1800
```

`ContentSwarmStress` (srad/large-networks-bulk-data.md LB-O-08) writes the
artifact to a temporary file, publishes it from peer 0 with a file-backed
piece store on every peer, and reports per fetcher how many bytes came from
the seeder and how many from the other fetchers; with one holder the seeder
share is 100%, and it falls as the fleet grows. `-Dbinding=tcp` (the default)
or `tls` (the TLS binding with per-peer channel certificates) picks what every
peer listens on; `dtls` and `quic` live in their own modules and are not wired
into `MiniFleet`. Every ten seconds the run prints each fetcher's verified pieces, and a fetcher
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

The same harnesses run from the jar, with the properties before `-cp`:

```bash
java -Dpeers=4 -Dentries=50 -cp target/benchmarks.jar \
  ai.badmonkey.agentspaces.perf.stress.GossipPropagationStress
```

## Tests

`mvn test` runs two smoke suites: one JMH
benchmark in process to prove the generated benchmark list works, and each
stress harness at toy scale to prove fleets start and reports carry data.
They verify the harnesses rather than the numbers.

## Contributing

Contributions are welcome under the Developer Certificate of Origin; see
[CONTRIBUTING.md](CONTRIBUTING.md) and the [Code of Conduct](CODE_OF_CONDUCT.md).
Report vulnerabilities privately, as [SECURITY.md](SECURITY.md) describes.

## License

Copyright 2026 Bad Monkey, Inc.

AgentSpaces Perf is an open source project from Bad Monkey, Inc., licensed
under the Apache License, Version 2.0; see `LICENSE`.
