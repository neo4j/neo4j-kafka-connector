/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.connectors.kafka.utils

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldStartWith
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledOnJre
import org.junit.jupiter.api.condition.EnabledOnOs
import org.junit.jupiter.api.condition.JRE
import org.junit.jupiter.api.condition.OS
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.neo4j.connectors.kafka.utils.Telemetry.CONFLUENT_ENV
import org.neo4j.connectors.kafka.utils.Telemetry.CONFLUENT_ENV_VALUE
import org.neo4j.connectors.kafka.utils.Telemetry.connectorInformation
import org.neo4j.connectors.kafka.utils.Telemetry.jreInformation
import org.neo4j.connectors.kafka.utils.Telemetry.neo4jDriverVersion
import org.neo4j.connectors.kafka.utils.Telemetry.platform

class TelemetryTest {

  @Test
  fun `should return driver version`() {
    neo4jDriverVersion() shouldStartWith "neo4j-java"
  }

  @Test
  @EnabledOnJre(JRE.JAVA_11)
  fun `should return jre information on jre 11`() {
    jreInformation() shouldStartWith "Java/11"
  }

  @Test
  @EnabledOnJre(JRE.JAVA_17)
  fun `should return jre information on jre 17`() {
    jreInformation() shouldStartWith "Java/17"
  }

  @Test
  @EnabledOnJre(JRE.JAVA_21)
  fun `should return jre information on jre 21`() {
    jreInformation() shouldStartWith "Java/21"
  }

  @Test
  @EnabledOnOs(OS.MAC)
  fun `should return platform`() {
    platform() shouldStartWith "Mac OS X"
  }

  @Test
  fun `should return connector information with kafka`() {
    val provider = mock<EnvironmentProvider> { on { get(CONFLUENT_ENV) } doReturn null }

    connectorInformation("sink", false, "", "", provider) shouldBe "kafka-sink"
    connectorInformation("sink", false, "5.1.0", "", provider) shouldBe "kafka-sink/5.1.0"
    connectorInformation("source", false, "", "", provider) shouldBe "kafka-source"
    connectorInformation("source", false, "5.1.0", "", provider) shouldBe "kafka-source/5.1.0"
    connectorInformation("sink", true, "", "", provider) shouldBe "kafka-legacy-sink"
    connectorInformation("source", true, "", "", provider) shouldBe "kafka-legacy-source"
    connectorInformation("sink", false, "", "cypher; node-pattern", provider) shouldBe
        "kafka-sink (cypher; node-pattern)"
    connectorInformation("sink", false, "5.1.0", "cypher; node-pattern", provider) shouldBe
        "kafka-sink/5.1.0 (cypher; node-pattern)"
    connectorInformation("source", false, "", "cdc", provider) shouldBe "kafka-source (cdc)"
    connectorInformation("source", false, "5.1.0", "cdc", provider) shouldBe
        "kafka-source/5.1.0 (cdc)"
    connectorInformation("sink", true, "", "cypher", provider) shouldBe "kafka-legacy-sink (cypher)"
    connectorInformation("source", true, "", "query", provider) shouldBe
        "kafka-legacy-source (query)"
  }

  @Test
  fun `should return connector information with confluent-cloud`() {
    val provider =
        mock<EnvironmentProvider> { on { get(CONFLUENT_ENV) } doReturn CONFLUENT_ENV_VALUE }

    connectorInformation("sink", false, "", "", provider) shouldBe "confluent-cloud-sink"
    connectorInformation("sink", false, "5.1.0", "", provider) shouldBe "confluent-cloud-sink/5.1.0"
    connectorInformation("source", false, "", "", provider) shouldBe "confluent-cloud-source"
    connectorInformation("source", false, "5.1.0", "", provider) shouldBe
        "confluent-cloud-source/5.1.0"
    connectorInformation("sink", true, "", "", provider) shouldBe "confluent-cloud-legacy-sink"
    connectorInformation("source", true, "", "", provider) shouldBe "confluent-cloud-legacy-source"
    connectorInformation("sink", false, "", "cypher; node-pattern", provider) shouldBe
        "confluent-cloud-sink (cypher; node-pattern)"
    connectorInformation("sink", false, "5.1.0", "cypher; node-pattern", provider) shouldBe
        "confluent-cloud-sink/5.1.0 (cypher; node-pattern)"
    connectorInformation("source", false, "", "cdc", provider) shouldBe
        "confluent-cloud-source (cdc)"
    connectorInformation("source", false, "5.1.0", "cdc", provider) shouldBe
        "confluent-cloud-source/5.1.0 (cdc)"
    connectorInformation("sink", true, "", "cypher", provider) shouldBe
        "confluent-cloud-legacy-sink (cypher)"
    connectorInformation("source", true, "", "query", provider) shouldBe
        "confluent-cloud-legacy-source (query)"
  }
}
