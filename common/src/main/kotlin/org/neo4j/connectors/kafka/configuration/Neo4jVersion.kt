package org.neo4j.connectors.kafka

import kotlin.math.sign

class Neo4jVersion @JvmOverloads constructor(
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

  companion object {
    private val v4_4 = Neo4jVersion(4, 4, 0)
    private val v5 = Neo4jVersion(5, 0, 0)

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
        }
        buffer = ""
      }
      if (buffer.isNotEmpty()) {
        patch = buffer.toInt(10)
      }
      require(!(major == -1 || minor == -1)) { String.format("Invalid Neo4j version: %s", version) }
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
  }
}
