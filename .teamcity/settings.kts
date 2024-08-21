import builds.Build
import builds.Neo4jKafkaConnectorVcs
import jetbrains.buildServer.configs.kotlin.project
import jetbrains.buildServer.configs.kotlin.version

version = "2024.03"

project {
  params {
    password("github-commit-status-token", "%github-token%")
    password("github-pull-request-token", "%github-token%")
  }

  vcsRoot(Neo4jKafkaConnectorVcs)

  val javaVersions = listOf("11", "17")

  javaVersions.forEach { javaVersion ->
    subProject(
        Build(
            name = "main",
            branchFilter =
                """
                      +:main
                    """
                    .trimIndent(),
            forPullRequests = false,
            javaVersion = javaVersion))

    subProject(
        Build(
            name = "pull-request",
            branchFilter =
                """
                      +:pull/*
                    """
                    .trimIndent(),
            forPullRequests = true,
            javaVersion = javaVersion))
  }
}
