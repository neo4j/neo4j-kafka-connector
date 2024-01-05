import builds.Build
import builds.Neo4jKafkaConnectorVcs
import jetbrains.buildServer.configs.kotlin.project
import jetbrains.buildServer.configs.kotlin.version

version = "2023.11"

project {
  params {
    password("github-commit-status-token", "credentialsJSON:23b1c78f-22be-4c3e-9efc-3e7ead3238ed")
    password("github-pull-request-token", "credentialsJSON:23b1c78f-22be-4c3e-9efc-3e7ead3238ed")
    text("github-packages-user", "neo4j-build-service")
    password("github-packages-token", "credentialsJSON:b43ccf66-3394-496f-8c22-be161097c2df")
  }

  vcsRoot(Neo4jKafkaConnectorVcs)

  subProject(
      Build(
          name = "main",
          branchFilter = """
                +:main               
            """
              .trimIndent(),
          triggerRules = """
                -:comment=^build.*release version.*:**
                -:comment=^build.*update version.*:**
            """.trimIndent(),
          forPullRequests = false))
  subProject(
      Build(
          name = "pull-request",
          branchFilter = """
                +:pull/*
            """
              .trimIndent(),
          forPullRequests = true))
}
