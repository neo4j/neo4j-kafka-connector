import builds.Build
import builds.Neo4jKafkaConnectorVcs
import builds.Neo4jVersion
import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.project
import jetbrains.buildServer.configs.kotlin.triggers.schedule
import jetbrains.buildServer.configs.kotlin.triggers.vcs
import jetbrains.buildServer.configs.kotlin.version

version = "2025.03"

project {
  params {
    password("github-commit-status-token", "%github-token%")
    password("github-pull-request-token", "%github-token%")
  }

  vcsRoot(Neo4jKafkaConnectorVcs)

  subProject(
      Build(name = "main", forPullRequests = false) {
        vcs {
          this.branchFilter = "+:main"
          this.triggerRules =
              """
              -:comment=^build.*release version.*:**
              -:comment=^build.*update version.*:**
              """
                  .trimIndent()
        }
      })

  subProject(
      Build(name = "pull-request", forPullRequests = true) {
        vcs { this.branchFilter = "+:pull/*" }
      })

  subProject(
      Project {
        this.id("compatibility")
        name = "compatibility"

        Neo4jVersion.entries.forEach { neo4j ->
          Build(name = "${neo4j.version}", forPullRequests = false, neo4jVersion = neo4j) {
            schedule {
              daily {
                hour = 8
                minute = 0
              }
              triggerBuild = always()
            }
          }
        }
      })
}
