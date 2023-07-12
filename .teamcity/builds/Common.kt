package builds

import jetbrains.buildServer.configs.kotlin.BuildFeatures
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.DslContext
import jetbrains.buildServer.configs.kotlin.Requirements
import jetbrains.buildServer.configs.kotlin.buildFeatures.PullRequests
import jetbrains.buildServer.configs.kotlin.buildFeatures.commitStatusPublisher
import jetbrains.buildServer.configs.kotlin.buildFeatures.freeDiskSpace
import jetbrains.buildServer.configs.kotlin.buildFeatures.pullRequests

const val GITHUB_OWNER = "neo4j"
const val GITHUB_REPOSITORY = "neo4j-kafka-connector"

enum class LinuxSize(val value: String) {
    SMALL("small"),
    LARGE("large")
}

fun Requirements.runOnLinux(size: LinuxSize = LinuxSize.SMALL) {
    startsWith("cloud.amazon.agent-name-prefix", "linux-${size.value}")
}

fun BuildType.thisVcs() = vcs {
    root(DslContext.settingsRoot)
    cleanCheckout = true
}

fun BuildFeatures.enableCommitStatusPublisher() = commitStatusPublisher {
    vcsRootExtId = DslContext.settingsRootId.toString()
    publisher = github {
        githubUrl = "https://api.github.com"
        authType = personalToken {
            token = "%github-commit-status-token%"
        }
    }
}

fun BuildFeatures.enablePullRequests() = pullRequests {
    vcsRootExtId = DslContext.settingsRootId.toString()
    provider = github {
        authType = token {
            token = "%github-pull-request-token%"
        }
        filterAuthorRole = PullRequests.GitHubRoleFilter.MEMBER
    }
}

fun BuildFeatures.requireDiskSpace(size: String = "3gb") = freeDiskSpace {
    requiredSpace = size
    failBuild = true
}

fun collectArtifacts(buildType: BuildType): BuildType {
    buildType.artifactRules = """
        +:packaging/target/*.jar => packages
        +:packaging/target/*.zip => packages
    """.trimIndent()

    return buildType
}
