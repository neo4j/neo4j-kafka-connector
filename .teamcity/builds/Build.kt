package builds

import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId
import jetbrains.buildServer.configs.kotlin.triggers.vcs

class Build(name: String, branchFilter: String, forPullRequests: Boolean) :
    Project({
      this.id(name.toId())
      this.name = name

      val packaging = Maven("${name}-package", "package", "package", "-DskipTests") {}

      val bts = sequential {
        if (forPullRequests)
            buildType(WhiteListCheck("${name}-whitelist-check", "white-list check"))
        if (forPullRequests) buildType(PRCheck("${name}-pr-check", "pr check"))
        buildType(
            Maven("${name}-prepare-build", "prepare-build", "install") {
              pomLocation = "build-resources/pom.xml"
            })
        buildType(Maven("${name}-build", "build", "test-compile") {})
        parallel {
          buildType(Maven("${name}-unit-tests", "unit tests", "test") {})
          buildType(
              Maven("${name}-integration-tests", "integration tests", "verify", "-DskipUnitTests") {
              })
        }
        if (forPullRequests) buildType(packaging) else buildType(collectArtifacts(packaging))
        buildType(Empty("${name}-complete", "complete"))
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
