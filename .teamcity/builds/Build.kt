package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId

enum class JavaPlatform(
    val javaVersion: JavaVersion = DEFAULT_JAVA_VERSION,
    val platformITVersions: List<String> = listOf(DEFAULT_CONFLUENT_PLATFORM_VERSION)
) {
  JDK_11(
      JavaVersion.V_11,
      platformITVersions = listOf("7.2.9", "7.7.0"),
  ),
  JDK_17(JavaVersion.V_17, platformITVersions = listOf("7.7.0"))
}

val DEFAULT_NEO4J_VERSION = Neo4jVersion.V_2025

class Build(
    name: String,
    forPullRequests: Boolean,
    forCompatibility: Boolean = false,
    neo4jVersion: Neo4jVersion = DEFAULT_NEO4J_VERSION,
    customizeCompletion: BuildType.() -> Unit = {}
) :
    Project(
        {
          this.id(name.toId())
          this.name = name

          val complete = Empty("${name}-complete", "complete")

          val bts = sequential {
            if (forPullRequests)
                buildType(WhiteListCheck("${name}-whitelist-check", "white-list check"))
            if (forPullRequests) dependentBuildType(PRCheck("${name}-pr-check", "pr check"))

            parallel {
              JavaPlatform.entries.forEach { java ->
                val packaging =
                    Maven(
                        "${name}-package-${java.javaVersion.version}",
                        "package (${java.javaVersion.version})",
                        "package",
                        java.javaVersion,
                        Neo4jVersion.V_NONE,
                        "-pl :packaging -am -DskipTests",
                    )

                sequential {
                  dependentBuildType(
                      Maven(
                          "${name}-build-${java.javaVersion.version}",
                          "build (${java.javaVersion.version})",
                          "test-compile",
                          java.javaVersion,
                          Neo4jVersion.V_NONE,
                      ),
                  )
                  dependentBuildType(
                      Maven(
                          "${name}-unit-tests-${java.javaVersion.version}",
                          "unit tests (${java.javaVersion.version})",
                          "test",
                          java.javaVersion,
                          neo4jVersion,
                      ),
                  )
                  dependentBuildType(collectArtifacts(packaging))

                  parallel {
                    java.platformITVersions.forEach { confluentPlatformVersion ->
                      dependentBuildType(
                          IntegrationTests(
                              "${name}-integration-tests-${java.javaVersion.version}-${confluentPlatformVersion}-${neo4jVersion.version}",
                              "integration tests (${java.javaVersion.version}, ${confluentPlatformVersion}, ${neo4jVersion.version})",
                              java.javaVersion,
                              confluentPlatformVersion,
                              neo4jVersion,
                          ) {
                            dependencies {
                              artifacts(packaging) {
                                artifactRules =
                                    """
                                    +:packages/*.jar => docker/plugins
                                    -:packages/*-kc-oss.jar
                                    """
                                        .trimIndent()
                              }
                            }
                          },
                      )
                    }
                  }
                }
              }
            }

            dependentBuildType(complete)
            if (!forPullRequests && !forCompatibility)
                dependentBuildType(Release("${name}-release", "release", DEFAULT_JAVA_VERSION))
          }

          bts.buildTypes().forEach {
            it.thisVcs()

            it.features {
              loginToECR()
              requireDiskSpace("5gb")
              if (!forCompatibility) enableCommitStatusPublisher()
              if (forPullRequests) enablePullRequests()
            }

            buildType(it)
          }

          complete.apply(customizeCompletion)
        },
    )
