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

  subProject(
      Build(
          name = "main",
          branchFilter =
              """
                      +:main
                    """
                  .trimIndent(),
          forPullRequests = false))

  subProject(
      Build(
          name = "pull-request",
          branchFilter =
              """
                      +:pull/174
                    """
                  .trimIndent(),
          forPullRequests = true))
}
