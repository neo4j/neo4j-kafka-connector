package builds

import jetbrains.buildServer.configs.kotlin.BuildFeatures
import jetbrains.buildServer.configs.kotlin.BuildSteps
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.CompoundStage
import jetbrains.buildServer.configs.kotlin.FailureAction
import jetbrains.buildServer.configs.kotlin.Requirements
import jetbrains.buildServer.configs.kotlin.buildFeatures.PullRequests
import jetbrains.buildServer.configs.kotlin.buildFeatures.commitStatusPublisher
import jetbrains.buildServer.configs.kotlin.buildFeatures.freeDiskSpace
import jetbrains.buildServer.configs.kotlin.buildFeatures.pullRequests
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.maven
import jetbrains.buildServer.configs.kotlin.vcs.GitVcsRoot

const val GITHUB_OWNER = "neo4j"
const val GITHUB_REPOSITORY = "neo4j-kafka-connector"
const val MAVEN_DEFAULT_ARGS = "--no-transfer-progress --batch-mode"

enum class LinuxSize(val value: String) {
  SMALL("small"),
  LARGE("large")
}

object Neo4jKafkaConnectorVcs :
    GitVcsRoot({
      id("Connectors_Neo4jKafkaConnector_Build")

      name = "neo4j-kafka-connector"
      url = "git@github.com:neo4j/neo4j-kafka-connector.git"
      branch = "refs/heads/main"
      branchSpec = "refs/heads/*"

      authMethod = defaultPrivateKey { userName = "git" }
    })

fun Requirements.runOnLinux(size: LinuxSize = LinuxSize.SMALL) {
  startsWith("cloud.amazon.agent-name-prefix", "linux-${size.value}")
}

fun BuildType.thisVcs() = vcs {
  root(Neo4jKafkaConnectorVcs)

  cleanCheckout = true
}

fun BuildFeatures.enableCommitStatusPublisher() = commitStatusPublisher {
  vcsRootExtId = Neo4jKafkaConnectorVcs.id.toString()
  publisher = github {
    githubUrl = "https://api.github.com"
    authType = personalToken { token = "%github-commit-status-token%" }
  }
}

fun BuildFeatures.enablePullRequests() = pullRequests {
  vcsRootExtId = Neo4jKafkaConnectorVcs.id.toString()
  provider = github {
    authType = token { token = "%github-pull-request-token%" }
    filterAuthorRole = PullRequests.GitHubRoleFilter.MEMBER
  }
}

fun BuildFeatures.requireDiskSpace(size: String = "3gb") = freeDiskSpace {
  requiredSpace = size
  failBuild = true
}

fun CompoundStage.dependentBuildType(bt: BuildType) =
    buildType(bt) {
      onDependencyCancel = FailureAction.CANCEL
      onDependencyFailure = FailureAction.FAIL_TO_START
    }

fun collectArtifacts(buildType: BuildType): BuildType {
  buildType.artifactRules =
      """
        +:packaging/target/*.jar => packages
        +:packaging/target/*.zip => packages
    """
          .trimIndent()

  return buildType
}

fun BuildSteps.commonMaven(init: MavenBuildStep.() -> Unit): MavenBuildStep {
  val maven = this.maven {
    // this is the settings name we uploaded to Connectors project
    userSettingsSelection = "github"
    localRepoScope = MavenBuildStep.RepositoryScope.MAVEN_DEFAULT

    dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
    dockerImage = "eclipse-temurin:11-jdk"
    dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
  }

  init(maven)
  return maven
}
