package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.maven
import jetbrains.buildServer.configs.kotlin.toId

class Maven(id: String, name: String, goals: String, args: String? = null) :
    BuildType({
      this.id(id.toId())
      this.name = name

      params {
        text("env.PACKAGES_USERNAME", "%github-packages-user%")
        password("env.PACKAGES_PASSWORD", "%github-packages-token%")
      }

      steps {
        maven {
          this.goals = goals
          this.runnerArgs = args

          dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
          dockerImage = "eclipse-temurin:11-jdk"
          dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
        }
      }

      features { dockerSupport {} }

      requirements { runOnLinux(LinuxSize.SMALL) }
    })
