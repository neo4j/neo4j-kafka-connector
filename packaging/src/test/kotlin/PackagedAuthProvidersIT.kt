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
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldNotBeEmpty
import java.io.File
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.zip.ZipFile
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * Checks that every authentication provider we ship is discoverable from the built artifacts, which
 * guards against service files overwriting each other in the uber jar and against missing jars in
 * the Confluent zip.
 *
 * The artifacts are loaded in a class loader whose parent is the platform class loader, so that
 * nothing on the test classpath is visible to the service lookup.
 */
class PackagedAuthProvidersIT {

  @Test
  fun `uber jar should register all shipped authentication providers`() {
    registeredNames(listOf(File(UBER_JAR_PATH).toURI().toURL())) shouldContainAll EXPECTED_NAMES
  }

  @Test
  fun `confluent zip should register all shipped authentication providers`(@TempDir dir: Path) {
    val jars = mutableListOf<URL>()
    ZipFile(CONFLUENT_ZIP_PATH).use { zip ->
      zip.entries()
          .asSequence()
          .filter { !it.isDirectory && it.name.endsWith(".jar") }
          .filter { it.name.split('/').dropLast(1).lastOrNull() == "lib" }
          .forEach { entry ->
            val target = dir.resolve(entry.name.substringAfterLast('/'))
            zip.getInputStream(entry).use { Files.copy(it, target) }
            jars.add(target.toUri().toURL())
          }
    }
    jars.shouldNotBeEmpty()

    registeredNames(jars) shouldContainAll EXPECTED_NAMES
  }

  private fun registeredNames(urls: List<URL>): Set<String> =
      URLClassLoader(urls.toTypedArray(), ClassLoader.getPlatformClassLoader()).use { loader ->
        val registryClass = loader.loadClass(REGISTRY_CLASS)
        val registry =
            registryClass.getMethod("using", ClassLoader::class.java).invoke(null, loader)
        @Suppress("UNCHECKED_CAST")
        (registryClass.getMethod("names").invoke(registry) as Set<String>).toSet()
      }

  companion object {
    private const val REGISTRY_CLASS = "org.neo4j.connectors.driver.auth.AuthTokenManagerRegistry"
    private val EXPECTED_NAMES = listOf("basic", "bearer", "custom", "kerberos", "none", "oidc")

    private val UBER_JAR_PATH: String
    private val CONFLUENT_ZIP_PATH: String

    init {
      val properties = Properties()
      PackagedAuthProvidersIT::class.java.getResourceAsStream("/test.properties").use {
        properties.load(it)
      }
      UBER_JAR_PATH = properties.getProperty("uber.jar.path")
      CONFLUENT_ZIP_PATH = properties.getProperty("confluent.zip.path")
    }
  }
}
