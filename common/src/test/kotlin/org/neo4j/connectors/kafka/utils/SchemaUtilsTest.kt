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

import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.events.Constraint
import org.neo4j.connectors.kafka.events.RelKeyStrategy
import org.neo4j.connectors.kafka.events.StreamsConstraintType
import org.neo4j.connectors.kafka.utils.SchemaUtils.getNodeKeys

class SchemaUtilsTest {

  @Test
  fun `getNodeKeys should select the constraint with lowest properties`() {
    val props =
        mapOf(
            "LabelA" to setOf("foo", "bar"),
            "LabelB" to setOf("foo", "bar", "fooBar"),
            "LabelC" to setOf("foo"))
    val constraints =
        props.map {
          Constraint(label = it.key, properties = it.value, type = StreamsConstraintType.UNIQUE)
        }
    val keys = getNodeKeys(props.keys.toList(), setOf("prop", "foo", "bar"), constraints)
    assertEquals(setOf("foo"), keys)
  }

  @Test
  fun `getNodeKeys should return the key sorted properly`() {
    // the method getNodeKeys should select (with multiple labels) the constraint with lowest
    // properties
    // with the same size, we take the first label in alphabetical order
    // finally, with same label name, we take the first sorted properties list alphabetically

    val pair1 = "LabelX" to setOf("foo", "aaa")
    val pair2 = "LabelB" to setOf("bar", "foo")
    val pair3 = "LabelC" to setOf("baz", "bar")
    val pair4 = "LabelB" to setOf("bar", "bez")
    val pair5 = "LabelA" to setOf("bar", "baa", "xcv")
    val pair6 = "LabelC" to setOf("aaa", "baa", "xcz")
    val pair7 = "LabelA" to setOf("foo", "aac")
    val pair8 = "LabelA" to setOf("foo", "aab")
    val props = listOf(pair1, pair2, pair3, pair4, pair5, pair6, pair7, pair8)

    // we shuffle the constraints to ensure that the result doesn't depend from the ordering
    val constraints =
        props
            .map {
              Constraint(
                  label = it.first, properties = it.second, type = StreamsConstraintType.UNIQUE)
            }
            .shuffled()

    val propertyKeys =
        setOf("prop", "prop2", "foo", "bar", "baz", "bez", "aaa", "aab", "baa", "aac", "xcz", "xcv")
    val actualKeys = getNodeKeys(props.map { it.first }, propertyKeys, constraints)
    val expectedKeys = setOf("aab", "foo")

    assertEquals(expectedKeys, actualKeys)
  }

  @Test
  fun `getNodeKeys should return all keys when RelKeyStrategy is ALL`() {

    val pair1 = "LabelX" to setOf("foo", "aaa")
    val pair2 = "LabelB" to setOf("bar", "foo")
    val pair3 = "LabelC" to setOf("baz", "bar")
    val pair4 = "LabelB" to setOf("bar", "bez")
    val pair5 = "LabelA" to setOf("bar", "baa", "xcv")
    val pair6 = "LabelC" to setOf("aaa", "baa", "xcz")
    val pair7 = "LabelA" to setOf("foo", "aac")
    val pair8 = "LabelA" to setOf("foo", "aab")
    val props = listOf(pair1, pair2, pair3, pair4, pair5, pair6, pair7, pair8)

    // we shuffle the constraints to ensure that the result doesn't depend from the ordering
    val constraints =
        props
            .map {
              Constraint(
                  label = it.first, properties = it.second, type = StreamsConstraintType.UNIQUE)
            }
            .shuffled()

    val propertyKeys =
        setOf("prop", "prop2", "foo", "bar", "baz", "bez", "aaa", "aab", "baa", "aac", "xcz", "xcv")
    val actualKeys =
        getNodeKeys(props.map { it.first }, propertyKeys, constraints, RelKeyStrategy.ALL)
    val expectedKeys = setOf("aaa", "aab", "aac", "baa", "bar", "baz", "bez", "foo", "xcv", "xcz")

    assertEquals(expectedKeys, actualKeys)
  }

  @Test
  fun `getNodeKeys should return the key sorted properly (with one label)`() {
    // the method getNodeKeys should select the constraint with lowest properties
    // with the same size, we take the first sorted properties list alphabetically

    val pair1 = "LabelA" to setOf("foo", "bar")
    val pair2 = "LabelA" to setOf("bar", "foo")
    val pair3 = "LabelA" to setOf("baz", "bar")
    val pair4 = "LabelA" to setOf("bar", "bez")
    val props = listOf(pair1, pair2, pair3, pair4)

    // we shuffle the constraints to ensure that the result doesn't depend from the ordering
    val constraints =
        props
            .map {
              Constraint(
                  label = it.first, properties = it.second, type = StreamsConstraintType.UNIQUE)
            }
            .shuffled()

    val propertyKeys = setOf("prop", "foo", "bar", "baz", "bez")
    val actualKeys = getNodeKeys(listOf("LabelA"), propertyKeys, constraints)
    val expectedKeys = setOf("bar", "baz")

    assertEquals(expectedKeys, actualKeys)
  }

  @Test
  fun `getNodeKeys should return all keys when RelKeyStrategy is ALL (with one label)`() {

    val pair1 = "LabelA" to setOf("foo", "bar")
    val pair2 = "LabelA" to setOf("bar", "foo")
    val pair3 = "LabelA" to setOf("baz", "bar")
    val pair4 = "LabelA" to setOf("bar", "bez")
    val props = listOf(pair1, pair2, pair3, pair4)

    // we shuffle the constraints to ensure that the result doesn't depend from the ordering
    val constraints =
        props
            .map {
              Constraint(
                  label = it.first, properties = it.second, type = StreamsConstraintType.UNIQUE)
            }
            .shuffled()

    val propertyKeys = setOf("prop", "foo", "bar", "baz", "bez")
    val actualKeys = getNodeKeys(listOf("LabelA"), propertyKeys, constraints, RelKeyStrategy.ALL)
    val expectedKeys = setOf("bar", "baz", "bez", "foo")

    assertEquals(expectedKeys, actualKeys)
  }

  @Test
  fun `getNodeKeys should return empty in case it didn't match anything`() {
    val props =
        mapOf(
            "LabelA" to setOf("foo", "bar"),
            "LabelB" to setOf("foo", "bar", "fooBar"),
            "LabelC" to setOf("foo"))
    val constraints =
        props.map {
          Constraint(label = it.key, properties = it.value, type = StreamsConstraintType.UNIQUE)
        }
    val keys = getNodeKeys(props.keys.toList(), setOf("prop", "key"), constraints)
    assertEquals(emptySet(), keys)
  }
}
