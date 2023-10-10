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
        parallel {
          dependentBuildType(Maven("${name}-unit-tests", "unit tests", "test"))
          dependentBuildType(
              IntegrationTests("${name}-integration-tests", "integration tests"))
        }
        if (forPullRequests) dependentBuildType(packaging)
        else dependentBuildType(collectArtifacts(packaging))
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
    })
