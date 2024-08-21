package builds

import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId
import jetbrains.buildServer.configs.kotlin.triggers.vcs

class Build(
    name: String,
    branchFilter: String,
    forPullRequests: Boolean,
    javaVersion: String,
    triggerRules: String? = null
) :
    Project({
      this.id("${name}-${javaVersion}".toId())
      this.name = "$name (Java $javaVersion)"

      val packaging =
          Maven(
              "${name}-package",
              "package",
              "package",
              javaVersion,
              "-pl :packaging -am -DskipTests")

      val complete = Empty("${name}-complete", "complete", javaVersion)

      val bts = sequential {
        if (forPullRequests)
            buildType(WhiteListCheck("${name}-whitelist-check", "white-list check", javaVersion))
        if (forPullRequests)
            dependentBuildType(PRCheck("${name}-pr-check", "pr check", javaVersion))
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
        dependentBuildType(complete)
        if (!forPullRequests) dependentBuildType(Release("${name}-release", "release", javaVersion))
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
