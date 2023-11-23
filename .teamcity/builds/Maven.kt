package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.buildSteps.script
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
        script {
          this.name = "Ensure maven settings"
          scriptContent =
              """
                 #!/bin/bash -eu
                 mkdir -p .m2
                 settings.xml < EOF
                  <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                                            http://maven.apache.org/xsd/settings-1.0.0.xsd">
                      <servers>
                        <server>
                          <id>github</id>
                          <username>${'$'}{env.PACKAGES_USERNAME}</username>
                          <password>${'$'}{env.PACKAGES_PASSWORD}</password>
                        </server>
                      </servers>
                  </settings>
                 EOF              
              """.trimIndent()
        }

        commonMaven {
          this.goals = goals
          this.runnerArgs = "$MAVEN_DEFAULT_ARGS ${args ?: ""}"
        }
      }

      features {
        dockerSupport {}
        mavenBuildCache {}
      }

      requirements { runOnLinux(LinuxSize.SMALL) }
    })
