package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.buildCache
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.toId

class Maven(id: String, name: String, goals: String, args: String? = null) :
    BuildType({
      this.id(id.toId())
      this.name = name

      // we uploaded a custom settings.xml file in Teamcity UI, under Connectors project
      // with the following content, so we set the relevant environment variables here.

      /*
      <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                                    http://maven.apache.org/xsd/settings-1.0.0.xsd">
          <servers>
            <server>
              <id>github</id>
              <username>${env.PACKAGES_USERNAME}</username>
              <password>${env.PACKAGES_PASSWORD}</password>
            </server>
          </servers>
      </settings>
       */

      params {
        text("env.PACKAGES_USERNAME", "%github-packages-user%")
        password("env.PACKAGES_PASSWORD", "%github-packages-token%")
      }

      steps {
        commonMaven {
          this.goals = goals
          this.runnerArgs = "$MAVEN_DEFAULT_ARGS ${args ?: ""}"
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
