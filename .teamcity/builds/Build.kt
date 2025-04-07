package builds

import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId
import jetbrains.buildServer.configs.kotlin.triggers.schedule
import jetbrains.buildServer.configs.kotlin.triggers.vcs

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
    branchFilter: String,
    forPullRequests: Boolean,
    triggerRules: String? = null
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
                        DEFAULT_NEO4J_VERSION,
                        "-pl :packaging -am -DskipTests",
                    )

                sequential {
                  dependentBuildType(
                      Maven(
                          "${name}-build-${java.javaVersion.version}",
                          "build (${java.javaVersion.version})",
                          "test-compile",
                          java.javaVersion,
                          DEFAULT_NEO4J_VERSION,
                      ),
                  )
                  dependentBuildType(
                      Maven(
                          "${name}-unit-tests-${java.javaVersion.version}",
                          "unit tests (${java.javaVersion.version})",
                          "test",
                          java.javaVersion,
                          DEFAULT_NEO4J_VERSION,
                      ),
                  )
                  dependentBuildType(collectArtifacts(packaging))

                  parallel {
                    java.platformITVersions.forEach { confluentPlatformVersion ->
                      dependentBuildType(
                          IntegrationTests(
                              "${name}-integration-tests-${java.javaVersion.version}-${confluentPlatformVersion}-${DEFAULT_JAVA_VERSION.version}",
                              "integration tests (${java.javaVersion.version}, ${confluentPlatformVersion}, ${DEFAULT_NEO4J_VERSION.version})",
                              java.javaVersion,
                              confluentPlatformVersion,
                              DEFAULT_NEO4J_VERSION,
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
            if (!forPullRequests)
                dependentBuildType(Release("${name}-release", "release", DEFAULT_JAVA_VERSION))
          }

          bts.buildTypes().forEach {
            it.thisVcs()

            it.features {
              requireDiskSpace("5gb")
              enableCommitStatusPublisher()
              if (forPullRequests) enablePullRequests()
            }

            buildType(it)
          }

          complete.triggers {
            vcs {
              this.branchFilter = branchFilter
              this.triggerRules = triggerRules
            }
          }
        },
    )

class CompatibilityBuild(name: String) :
    Project(
        {
          this.id(name.toId())
          this.name = name

          val complete = Empty("${name}-complete", "complete")

          val bts = sequential {
            parallel {
              JavaPlatform.entries.forEach { javaPlatform ->
                val packaging =
                    Maven(
                        "${name}-package-${javaPlatform.javaVersion.version}",
                        "package (${javaPlatform.javaVersion.version})",
                        "package",
                        javaPlatform.javaVersion,
                        DEFAULT_NEO4J_VERSION,
                        "-pl :packaging -am -DskipTests",
                    )

                sequential {
                  dependentBuildType(
                      Maven(
                          "${name}-build-${javaPlatform.javaVersion.version}",
                          "build (${javaPlatform.javaVersion.version})",
                          "test-compile",
                          javaPlatform.javaVersion,
                          DEFAULT_NEO4J_VERSION),
                  )

                  Neo4jVersion.entries.forEach { neo4jVersion ->
                    dependentBuildType(
                        Maven(
                            "${name}-unit-tests-${javaPlatform.javaVersion.version}-${neo4jVersion.version}",
                            "unit tests (${javaPlatform.javaVersion.version}, ${neo4jVersion.version})",
                            "test",
                            javaPlatform.javaVersion,
                            neo4jVersion,
                        ),
                    )

                    dependentBuildType(collectArtifacts(packaging))

                    parallel {
                      javaPlatform.platformITVersions.forEach { confluentPlatformVersion ->
                        dependentBuildType(
                            IntegrationTests(
                                "${name}-integration-tests-${javaPlatform.javaVersion.version}-${confluentPlatformVersion}-${neo4jVersion.version}",
                                "integration tests (${javaPlatform.javaVersion.version}, ${confluentPlatformVersion}, ${neo4jVersion.version})",
                                javaPlatform.javaVersion,
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
            }

            dependentBuildType(complete)
          }

          bts.buildTypes().forEach {
            it.thisVcs()

            it.features {
              requireDiskSpace("5gb")
              enableCommitStatusPublisher()
            }

            buildType(it)
          }

          complete.triggers {
            schedule {
              daily {
                hour = 8
                minute = 0
              }
              triggerBuild = always()
            }
          }
        },
    )
