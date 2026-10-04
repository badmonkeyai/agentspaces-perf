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
```

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
