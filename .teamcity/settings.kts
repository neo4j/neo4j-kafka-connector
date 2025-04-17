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
      Build(
          name = "main",
          neo4jVersions = setOf(Neo4jVersion.V_4_4, Neo4jVersion.V_5, Neo4jVersion.V_2025),
          forPullRequests = false) {
            triggers {
              vcs {
                this.branchFilter = "+:update-ci"
                this.triggerRules =
                    """
              -:comment=^build.*release version.*:**
              -:comment=^build.*update version.*:**
              """
                        .trimIndent()
              }
            }
          })

  subProject(
      Build(
          name = "pull-request",
          neo4jVersions = setOf(Neo4jVersion.V_4_4, Neo4jVersion.V_5, Neo4jVersion.V_2025),
          forPullRequests = true) {
            triggers { vcs { this.branchFilter = "+:pull/*" } }
          })

  subProject(
      Project {
        this.id("compatibility")
        name = "compatibility"

        Neo4jVersion.entries.minus(Neo4jVersion.V_NONE).forEach { neo4j ->
          subProject(
              Build(
                  name = "${neo4j.version}",
                  forPullRequests = false,
                  forCompatibility = true,
                  neo4jVersions = setOf(neo4j)) {
                    triggers {
                      vcs { enabled = false }

                      schedule {
                        branchFilter = "+:main"
                        schedulingPolicy = daily {
                          hour = 8
                          minute = 0
                        }
                        triggerBuild = always()
                        dependencies { triggerBuild = always() }
                      }
                    }
                  })
        }
      })
}
