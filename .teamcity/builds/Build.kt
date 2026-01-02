package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.ReuseBuilds
import jetbrains.buildServer.configs.kotlin.buildFeatures.notifications
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

class Build(
    name: String,
    forPullRequests: Boolean,
    neo4jVersions: Set<Neo4jVersion>,
    forCompatibility: Boolean = false,
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
              dependentBuildType(SemgrepCheck("${name}-semgrep-check", "semgrep check"))

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

                  dependentBuildType(collectArtifacts(packaging))

                  parallel {
                    neo4jVersions.forEach { neo4jVersion ->
                      sequential {
                        dependentBuildType(
                            Maven(
                                "${name}-unit-tests-${java.javaVersion.version}-${neo4jVersion.version}",
                                "unit tests (${java.javaVersion.version}, ${neo4jVersion.version})",
                                "test",
                                java.javaVersion,
                                neo4jVersion,
                            ),
                        )

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
                }
              }
            }

            dependentBuildType(
                complete, reuse = if (forCompatibility) ReuseBuilds.NO else ReuseBuilds.SUCCESSFUL)
            if (!forPullRequests && !forCompatibility)
                dependentBuildType(Release("${name}-release", "release", DEFAULT_JAVA_VERSION))
          }

          bts.buildTypes().forEach {
            it.thisVcs()

            it.features {
              loginToECR()
              requireDiskSpace("15gb")
              if (!forCompatibility) enableCommitStatusPublisher()
              if (forPullRequests) enablePullRequests()
            }

            buildType(it)
          }

          complete.features {
            notifications {
              branchFilter =
                  """
                  +:$DEFAULT_BRANCH
                  ${if (forPullRequests) "+:pull/*" else ""}
                  """
                      .trimIndent()

              queuedBuildRequiresApproval = forPullRequests
              buildFailedToStart = !forPullRequests
              buildFailed = !forPullRequests
              buildFinishedSuccessfully = !forPullRequests
              buildProbablyHanging = !forPullRequests

              notifierSettings = slackNotifier {
                connection = SLACK_CONNECTION_ID
                sendTo = SLACK_CHANNEL
                messageFormat = simpleMessageFormat()
              }
            }
          }

          complete.apply(customizeCompletion)
        },
    )
