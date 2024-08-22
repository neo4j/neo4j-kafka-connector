package builds

import jetbrains.buildServer.configs.kotlin.Project
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

        parallel {
          listOf("11", "17").forEach { javaVersion ->
            val packaging =
                Maven(
                    "${name}-package",
                    "package",
                    "package",
                    javaVersion,
                    "-pl :packaging -am -DskipTests")

            sequential {
              dependentBuildType(Maven("${name}-build", "build", "test-compile", javaVersion))
              dependentBuildType(Maven("${name}-unit-tests", "unit tests", "test", javaVersion))
              dependentBuildType(collectArtifacts(packaging))
              dependentBuildType(
                  IntegrationTests("${name}-integration-tests", "integration tests", javaVersion) {
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
                  })
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
    })
