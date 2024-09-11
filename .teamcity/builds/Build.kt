package builds

import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId
import jetbrains.buildServer.configs.kotlin.triggers.vcs

enum class JavaPlatform(
    val javaVersion: String = DEFAULT_JAVA_VERSION,
    val platformVersions: List<String> = listOf(DEFAULT_CONFLUENT_PLATFORM_VERSION)
) {
  JDK_11("11", platformVersions = listOf("7.2.9", "7.7.0")),
  JDK_17("17", platformVersions = listOf("7.7.0"))
}

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
              JavaPlatform.entries.forEach { p ->
                val packaging =
                    Maven(
                        "${name}-package",
                        "package",
                        "package",
                        p.javaVersion,
                        "-pl :packaging -am -DskipTests")

                dependentBuildType(Maven("${name}-build", "build", "test-compile", "11"))
                dependentBuildType(Maven("${name}-unit-tests", "unit tests", "test", "11"))
                dependentBuildType(collectArtifacts(packaging))

                parallel {
                  p.platformVersions.forEach { version ->
                    dependentBuildType(
                        IntegrationTests(
                            "${name}-integration-tests",
                            "integration tests",
                            p.javaVersion,
                            version,
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
