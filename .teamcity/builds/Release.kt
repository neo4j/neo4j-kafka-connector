package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
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
      commonMaven {
        goals = "versions:set"
        runnerArgs = "$MAVEN_DEFAULT_ARGS -DnewVersion=%version% -DgenerateBackupPoms=false"
      }

      commonMaven {
        goals = "package"
        runnerArgs = "$MAVEN_DEFAULT_ARGS -DskipTests"
      }

      script {
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
              git commit -m "Release version %version%"
              git push 
            """.trimIndent()
      }

      commonMaven {
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

