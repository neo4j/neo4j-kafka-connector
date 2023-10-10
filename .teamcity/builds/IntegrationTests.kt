package builds

import jetbrains.buildServer.configs.kotlin.BuildStep
import jetbrains.buildServer.configs.kotlin.BuildStepConditions
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.maven
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class IntegrationTests(id: String, name: String) :
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
          scriptContent = """
                #!/bin/bash -eu
                
                gem install dip
                curl -fsSL https://get.docker.com | sh
                dip provision
            """.trimIndent()
          formatStderrAsError = true


          dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
          dockerImage = "eclipse-temurin:11-jdk"
          dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
        }
        maven {
          this.goals = "verify"
          this.runnerArgs = "-DskipUnitTests"

          // this is the settings name we uploaded to Connectors project
          userSettingsSelection = "github"

          dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
          dockerImage = "eclipse-temurin:11-jdk"
          dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
        }
        script {
          scriptContent = """
                #!/bin/bash -eu
                
                # TODO: publish custom image instead
                gem install dip
                curl -fsSL https://get.docker.com | sh
                dip compose down --rmi local
            """.trimIndent()
          formatStderrAsError = true

          executionMode = BuildStep.ExecutionMode.ALWAYS
          dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
          dockerImage = "ruby:3"
          dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
        }
      }

      features { dockerSupport {} }

      requirements { runOnLinux(LinuxSize.LARGE) }
    })
