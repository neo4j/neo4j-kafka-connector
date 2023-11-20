package builds

import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId
import jetbrains.buildServer.configs.kotlin.triggers.vcs

class Build(name: String, branchFilter: String, forPullRequests: Boolean) :
    Project({
      this.id(name.toId())
      this.name = name

      val packaging =
          Maven("${name}-package", "package", "package", "-pl :packaging -am -DskipTests")

      val bts = sequential {
        if (forPullRequests)
            buildType(WhiteListCheck("${name}-whitelist-check", "white-list check"))
        if (forPullRequests) dependentBuildType(PRCheck("${name}-pr-check", "pr check"))
        dependentBuildType(Maven("${name}-build", "build", "test-compile"))
        dependentBuildType(Maven("${name}-unit-tests", "unit tests", "test"))
        dependentBuildType(collectArtifacts(packaging))
        dependentBuildType(
            IntegrationTests("${name}-integration-tests", "integration tests") {
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
        dependentBuildType(Empty("${name}-complete", "complete"))
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

      bts.buildTypes().last().triggers { vcs { this.branchFilter = branchFilter } }

      buildType(Release("${name}-release", "release", packaging))
    })
