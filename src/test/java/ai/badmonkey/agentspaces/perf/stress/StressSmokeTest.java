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
}
