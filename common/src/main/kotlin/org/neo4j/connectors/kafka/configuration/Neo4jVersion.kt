/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.connectors.kafka.configuration

import java.util.*
import kotlin.math.sign

class Neo4jVersion(
    private val major: Int,
    private val minor: Int,
    private val patch: Int = Int.MAX_VALUE
) : Comparable<Neo4jVersion> {

  override fun compareTo(other: Neo4jVersion): Int {
    if (major != other.major) {
      return signum(major - other.major)
    }
    if (minor != other.minor) {
      return signum(minor - other.minor)
    }
    return signum(patch - other.patch)
  }

  override fun equals(other: Any?): Boolean {
    if (other !is Neo4jVersion) {
      return false
    }
    return major == other.major && minor == other.minor && patch == other.patch
  }

  override fun hashCode(): Int {
    return Objects.hash(major, minor, patch)
  }

  override fun toString(): String {
    if (patch == Int.MAX_VALUE) {
      return String.format("%d.%d", major, minor)
    }
    return String.format("%d.%d.%d", major, minor, patch)
  }

  companion object {
    val v5 = Neo4jVersion(5, 0, 0)
    val v4_4 = Neo4jVersion(4, 4, 0)

    fun of(version: String): Neo4jVersion {
      var major = -1
      var minor = -1
      var patch = -1
      var buffer = ""
      for (c in version.toCharArray()) {
        if (c != '.') {
          buffer += c
          continue
        }
        if (major == -1) {
          major = buffer.toInt(10)
        } else if (minor == -1) {
          minor = parseMinor(buffer)
        } else {
          throw invalidVersion(version)
        }
        buffer = ""
      }
      if (buffer.isNotEmpty()) {
        if (minor == -1) {
          minor = parseMinor(buffer)
        } else {
          patch = buffer.toInt(10)
        }
      }
      if (major == -1 || minor == -1) {
        throw invalidVersion(version)
      }
      if (patch == -1) {
        return Neo4jVersion(major, minor)
      }
      return Neo4jVersion(major, minor, patch)
    }

    private fun parseMinor(buffer: String): Int {
      return buffer.replace("-aura", "").toInt(10)
    }

    private fun signum(result: Int): Int {
      return sign(result.toDouble()).toInt()
    }

    private fun invalidVersion(version: String): IllegalArgumentException {
      return IllegalArgumentException(String.format("Invalid Neo4j version: %s", version))
    }
  }
}
