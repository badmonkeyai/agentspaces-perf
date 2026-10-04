# Security Policy

AgentSpaces Perf is a benchmarking and stress-testing project for the
AgentSpaces Java libraries. It ships no library and is never published to
Maven Central; its harnesses start throwaway fleets on `127.0.0.1` with fresh
identities. The security model is the core's (the AgentSpaces specification,
[SPEC §11](https://github.com/badmonkeyai/agentspaces-spec/blob/main/SPEC.md)).

## Reporting a vulnerability

Please report vulnerabilities **privately** via GitHub's security advisories:

> https://github.com/badmonkeyai/agentspaces-perf/security/advisories/new

A vulnerability in the AgentSpaces core libraries, including one a benchmark
or stress run happens to reveal, belongs in the core repository's advisories
instead:
https://github.com/badmonkeyai/AgentSpaces/security/advisories/new

Do not open public issues for suspected vulnerabilities, and do not include
exploit details in public discussions until a fix is released.

We will acknowledge your report within five business days, keep you informed of
progress, credit you in the advisory unless you prefer otherwise, and
coordinate the disclosure timeline with you. There is currently no bug bounty
program.

Questions can be sent to `oss [at] badmonkey.ai`

## Scope

In scope: anything in this repository that could harm the machine it runs on
or leak something it should not, such as a harness listening beyond loopback,
or key material written where it persists.

Performance findings, such as a slow path or a fleet that degrades under
churn, are welcome as ordinary issues, not security reports, unless they are
a denial-of-service against the core, which belongs in the core's advisories.
