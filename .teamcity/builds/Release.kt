package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class Release(id: String, name: String) :
  BuildType({
    this.id(id.toId())
    this.name = name

    params {
      text("version", "", allowEmpty = false, display = ParameterDisplay.PROMPT, label = "Version to release")

      text("env.PACKAGES_USERNAME", "%github-packages-user%")
      password("env.PACKAGES_PASSWORD", "%github-packages-token%")
      password("env.JRELEASER_GITHUB_TOKEN", "%github-pull-request-token%")
    }

    steps {
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
             
              git status
              git add \*pom.xml
              git status
              git push origin HEAD:test-releases
            """.trimIndent()
      }

      commonMaven {
        this.name = "Release to Github"
        goals = "jreleaser:full-release"
        runnerArgs = "$MAVEN_DEFAULT_ARGS -Prelease -Djreleaser.dry.run=true  -pl :packaging"
      }
    }

    features {
      dockerSupport {}
    }

    requirements { runOnLinux(LinuxSize.SMALL) }

})

