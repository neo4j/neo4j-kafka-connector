import builds.Build
import builds.Neo4jKafkaConnectorVcs
import jetbrains.buildServer.configs.kotlin.project
import jetbrains.buildServer.configs.kotlin.version

version = "2023.05"

project {
  params {
    password("github-commit-status-token", "credentialsJSON:be6ac011-ba27-4f2e-a628-edce6708504e")
    password("github-pull-request-token", "credentialsJSON:be6ac011-ba27-4f2e-a628-edce6708504e")
    text("github-packages-user", "neo4j-build-service")
    password("github-packages-token", "credentialsJSON:d5ea2df2-7a81-41d4-98bf-cb7ebd607493")
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
