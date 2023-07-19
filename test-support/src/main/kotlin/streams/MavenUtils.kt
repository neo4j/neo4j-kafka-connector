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
package streams

import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import org.slf4j.Logger

object MavenUtils {
  fun mvnw(path: String = ".", logger: Logger? = null, vararg args: String) {

    val rt = Runtime.getRuntime()
    val mvnw = if (System.getProperty("os.name").startsWith("Windows")) "./mvnw.cmd" else "./mvnw"
    val commands =
      arrayOf(mvnw, "-pl", "!doc,!kafka-connect-neo4j", "-DbuildSubDirectory=containerPlugins") +
        args.let { if (it.isNullOrEmpty()) arrayOf("package", "-Dmaven.test.skip") else it }
    val proc = rt.exec(commands, null, File(path))

    val stdInput = BufferedReader(InputStreamReader(proc.inputStream))

    val stdError = BufferedReader(InputStreamReader(proc.errorStream))

    // Read the output from the command
    var s: String? = null
    while (stdInput.readLine().also { s = it } != null) {
      logger?.info(s)
    }

    // Read any errors from the attempted command
    while (stdError.readLine().also { s = it } != null) {
      logger?.error(s)
    }
  }

  fun isTravis() = System.getenv().getOrDefault("TRAVIS", "false") == "true"
}
