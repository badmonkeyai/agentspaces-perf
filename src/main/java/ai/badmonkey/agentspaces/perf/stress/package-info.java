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
/**
 * Multi-peer stress harnesses over real TCP loopback: contended take
 * throughput, gossip propagation latency, and churn recovery. Each harness is
 * a plain {@code main} configured through system properties, sized for a
 * laptop by default, and returns a structured report so the smoke tests can
 * run it small.
 */
package ai.badmonkey.agentspaces.perf.stress;
