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
const val MAVEN_DEFAULT_ARGS =
    "--no-transfer-progress --batch-mode -Dmaven.repo.local=%teamcity.build.checkoutDir%/.m2/repository"

val DEFAULT_JAVA_VERSION = JavaVersion.V_11
const val DEFAULT_CONFLUENT_PLATFORM_VERSION = "7.2.9"

enum class LinuxSize(val value: String) {
  SMALL("small"),
  LARGE("large")
}

enum class JavaVersion(val version: String, val dockerImage: String) {
  V_11(version = "11", dockerImage = "eclipse-temurin:11-jdk"),
  V_17(version = "17", dockerImage = "eclipse-temurin:17-jdk"),
}

enum class Neo4jVersion(
    val version: String,
    val dockerImage: String,
    val activationProperty: String
) {
  V_4_4("4.4", "neo4j:4.4-enterprise", "neo4j-44"),
  V_4_4_DEV(
      "4.4-dev",
      "535893049302.dkr.ecr.eu-west-1.amazonaws.com/build-service/neo4j:4.4-enterprise-debian-nightly",
      "neo4j-44"),
  V_5("5", "neo4j:5-enterprise", "neo4j-5"),
  V_5_DEV(
      "5-dev",
      "535893049302.dkr.ecr.eu-west-1.amazonaws.com/build-service/neo4j:5-enterprise-debian-nightly",
      "neo4j-5"),
  V_2025("2025", "neo4j:2025-enterprise", "neo4j-2025"),
  V_2025_DEV(
      "2025-dev",
      "535893049302.dkr.ecr.eu-west-1.amazonaws.com/build-service/neo4j:2025-enterprise-debian-nightly",
      "neo4j-2025"),
}

object Neo4jKafkaConnectorVcs :
    GitVcsRoot(
        {
          id("Connectors_Neo4jKafkaConnector_Build")

          name = "neo4j-kafka-connector"
          url = "git@github.com:neo4j/neo4j-kafka-connector.git"
          branch = "refs/heads/main"
          branchSpec = "refs/heads/*"

          authMethod = defaultPrivateKey { userName = "git" }
        },
    )

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
    filterAuthorRole = PullRequests.GitHubRoleFilter.EVERYBODY
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

fun BuildSteps.commonMaven(
    javaVersion: JavaVersion,
    init: MavenBuildStep.() -> Unit
): MavenBuildStep {
  val maven =
      this.maven {
        // this is the settings name we uploaded to Connectors project
        userSettingsSelection = "github"
        localRepoScope = MavenBuildStep.RepositoryScope.MAVEN_DEFAULT

        dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
        dockerImage = javaVersion.dockerImage
        dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
      }

  init(maven)
  return maven
}
