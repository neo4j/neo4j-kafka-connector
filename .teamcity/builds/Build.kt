package builds

import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId
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
              JavaPlatform.entries.forEach {
                val packaging =
                    Maven(
                        "${name}-package",
                        "package",
                        "package",
                        it.javaVersion,
                        "-pl :packaging -am -DskipTests",
                    )

                sequential {
                  dependentBuildType(
                      Maven(
                          "${name}-build",
                          "build",
                          "test-compile",
                          it.javaVersion,
                      ),
                  )
                  dependentBuildType(
                      Maven(
                          "${name}-unit-tests",
                          "unit tests",
                          "test",
                          it.javaVersion,
                      ),
                  )
                  dependentBuildType(collectArtifacts(packaging))

                  parallel {
                    it.platformITVersions.forEach { platformVersion ->
                      dependentBuildType(
                          IntegrationTests(
                              "${name}-integration-tests",
                              "integration tests",
                              it.javaVersion,
                              platformVersion,
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
