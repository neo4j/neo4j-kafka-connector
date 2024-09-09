package builds

import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.StageFactory.parallel
import jetbrains.buildServer.configs.kotlin.StageFactory.sequential
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId
import jetbrains.buildServer.configs.kotlin.triggers.vcs

class Build(
    name: String,
    branchFilter: String,
    forPullRequests: Boolean,
    triggerRules: String? = null
) :
    Project({
      this.id(name.toId())
      this.name = name

      val complete = Empty("${name}-complete", "complete")

      val bts = sequential {
        if (forPullRequests)
            buildType(WhiteListCheck("${name}-whitelist-check", "white-list check"))
        if (forPullRequests) dependentBuildType(PRCheck("${name}-pr-check", "pr check"))

        val packaging11 =
            Maven("${name}-package", "package", "package", "11", "-pl :packaging -am -DskipTests")

        val packaging17 =
            Maven("${name}-package", "package", "package", "17", "-pl :packaging -am -DskipTests")

        parallel {
          // JDK 11
          sequential {
            dependentBuildType(Maven("${name}-build", "build", "test-compile", "11"))
            dependentBuildType(Maven("${name}-unit-tests", "unit tests", "test", "11"))
            dependentBuildType(collectArtifacts(packaging11))

            parallel {
              dependentBuildType(
                  IntegrationTests(
                      "${name}-integration-tests", "integration tests", "11", "7.2.9") {
                        dependencies {
                          artifacts(packaging11) {
                            artifactRules =
                                """
                        +:packages/*.jar => docker/plugins
                        -:packages/*-kc-oss.jar
                      """
                                    .trimIndent()
                          }
                        }
                      })
              dependentBuildType(
                  IntegrationTests(
                      "${name}-integration-tests", "integration tests", "11", "7.7.0") {
                        dependencies {
                          artifacts(packaging11) {
                            artifactRules =
                                """
                        +:packages/*.jar => docker/plugins
                        -:packages/*-kc-oss.jar
                      """
                                    .trimIndent()
                          }
                        }
                      })
            }
          }

          // JDK 17
          sequential {
            dependentBuildType(Maven("${name}-build", "build", "test-compile", "17"))
            dependentBuildType(Maven("${name}-unit-tests", "unit tests", "test", "17"))
            dependentBuildType(collectArtifacts(packaging17))

            dependentBuildType(
                IntegrationTests("${name}-integration-tests", "integration tests", "17", "7.7.0") {
                  dependencies {
                    artifacts(packaging17) {
                      artifactRules =
                          """
                        +:packages/*.jar => docker/plugins
                        -:packages/*-kc-oss.jar
                      """
                              .trimIndent()
                    }
                  }
                })
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
    })
