package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class Release(id: String, name: String) :
  BuildType({
    this.id(id.toId())
    this.name = name

    params {
      text("version", "", allowEmpty = false)

      text("env.PACKAGES_USERNAME", "%github-packages-user%")
      password("env.PACKAGES_PASSWORD", "%github-packages-token%")
      password("env.JRELEASER_GITHUB_TOKEN", "%github-pull-request-token%")
    }

    steps {
      script {
        this.name = "Validate version"
        scriptContent =
            """
              #!/bin/bash -eu
               
              if [ -z "%version%" ]; then
                echo "Version is not set. Please run as a custom build and specify 'version' parameter."
                exit 1 
              fi 
            """.trimIndent()
      }

      commonMaven {
        this.name = "Set version"
        goals = "versions:set"
        runnerArgs = "$MAVEN_DEFAULT_ARGS -DnewVersion=%version% -DgenerateBackupPoms=false"
      }

      commonMaven {
        this.name = "Build versionalised package"
        goals = "package"
        runnerArgs = "$MAVEN_DEFAULT_ARGS -DskipTests"
      }

      script {
        this.name = "Push version"
        scriptContent =
            """
              #!/bin/bash -eu
              
              apt-get update
              apt-get install --yes git 
              
              USER_NAME=`echo %teamcity.build.triggeredBy.username% | sed 's/@.*//g' | sed 's/\./ /g' |  sed 's/\w\+/\L\u&/g'`
      
              git config --global user.email %teamcity.build.triggeredBy.username%
              git config --global user.name "${'$'}USER_NAME"
              git config --global --add safe.directory %teamcity.build.checkoutDir%
              
              git add .
              git commit -m "build: release version %version%"
              git push 
            """.trimIndent()

        formatStderrAsError = true

        dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
        dockerImage = "eclipse-temurin:11-jdk"
        dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
      }

      commonMaven {
        this.name = "Release to Github"
        goals = "jreleaser:full-release"
        runnerArgs = "$MAVEN_DEFAULT_ARGS -Prelease -pl :packaging"
      }
    }

    features {
      dockerSupport {}
      requireDiskSpace("5gb")
    }

    requirements { runOnLinux(LinuxSize.SMALL) }

})

