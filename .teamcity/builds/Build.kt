package builds

import jetbrains.buildServer.configs.kotlin.v10.toExtId
import jetbrains.buildServer.configs.kotlin.v2019_2.Project
import jetbrains.buildServer.configs.kotlin.v2019_2.sequential
import jetbrains.buildServer.configs.kotlin.v2019_2.triggers.vcs

class Build(name: String, branchFilter: String, forPullRequests: Boolean) : Project({
    this.id(name.toExtId())
    this.name = name

    var packaging = Maven("${name}-package", "package", "clean package -pl :packaging -am")

    val bts = sequential {
        if (forPullRequests) buildType(WhiteListCheck("${name}-whitelist-check", "white-list check"))
        if (forPullRequests) buildType(PRCheck("${name}-pr-check", "pr check"))
        buildType(Maven("${name}-build", "build", "clean compile"))
        parallel {
            buildType(Maven("${name}-unit-tests", "unit tests", "clean test"))
            buildType(Maven("${name}-integration-tests", "integration tests", "clean verify", "-DskipUnitTests"))
        }
        if (forPullRequests) buildType(packaging)
        else buildType(collectArtifacts(packaging))
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

    bts.buildTypes().last().triggers {
        vcs {
            this.branchFilter = branchFilter
        }
    }
})

