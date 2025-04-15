import builds.Build
import builds.Neo4jKafkaConnectorVcs
import builds.Neo4jVersion
import builds.SLACK_CHANNEL
import builds.SLACK_CONNECTION_ID
import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.buildFeatures.notifications
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
        triggers {
          vcs {
            this.branchFilter = "+:fix-test-containers"
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
      Build(name = "pull-request", forPullRequests = true) {
        triggers { vcs { this.branchFilter = "+:pull/*" } }
      })

  subProject(
      Project {
        this.id("compatibility")
        name = "compatibility"

        Neo4jVersion.entries.forEach { neo4j ->
          subProject(
              Build(
                  name = "${neo4j.version}",
                  forPullRequests = false,
                  forCompatibility = true,
                  neo4jVersion = neo4j) {
                    triggers {
                      vcs { enabled = false }

                      schedule {
                        branchFilter = "+:fix-test-containers"
                        schedulingPolicy = daily {
                          hour = 8
                          minute = 0
                        }
                        triggerBuild = always()
                      }
                    }

                    features {
                      notifications {
                        buildFailedToStart = true
                        buildFailed = true
                        firstFailureAfterSuccess = true
                        firstSuccessAfterFailure = true
                        buildProbablyHanging = true

                        branchFilter = "+:fix-test-containers"

                        notifierSettings = slackNotifier {
                          connection = SLACK_CONNECTION_ID
                          sendTo = SLACK_CHANNEL
                          messageFormat = simpleMessageFormat()
                        }
                      }
                    }
                  })
        }
      })
}
