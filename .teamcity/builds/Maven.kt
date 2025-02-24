package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.buildCache
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.toId

class Maven(
    id: String,
    name: String,
    goals: String,
    javaVersion: JavaVersion,
    args: String? = null
) :
    BuildType({
      this.id("${id}-${javaVersion.version}".toId())
      this.name = "$name (Java ${javaVersion.version})"

      params { text("env.JAVA_VERSION", javaVersion.version) }

      steps {
        commonMaven(javaVersion) {
          this.goals = goals
          this.runnerArgs =
              "$MAVEN_DEFAULT_ARGS -Djava.version=${javaVersion.version} ${args ?: ""}"
        }
      }

      features {
        dockerSupport {}

        buildCache {
          this.name = "neo4j-kafka-connector"
          publish = true
          use = true
          publishOnlyChanged = true
          rules = ".m2/repository"
        }
      }

      requirements { runOnLinux(LinuxSize.SMALL) }
    })
