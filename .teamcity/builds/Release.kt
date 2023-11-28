package builds

import jetbrains.buildServer.configs.kotlin.BuildSteps
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class Release(id: String, name: String) :
  BuildType({
    this.id(id.toId())
    this.name = name

    params {
      text("releaseVersion", "", allowEmpty = false,
          display = ParameterDisplay.PROMPT, label = "Version to release")
      text("nextSnapshotVersion", "", allowEmpty = false,
          display = ParameterDisplay.PROMPT, label = "Version on the next snapshot after the release")

      text("env.PACKAGES_USERNAME", "%github-packages-user%")
      password("env.PACKAGES_PASSWORD", "%github-packages-token%")
      password("env.JRELEASER_GITHUB_TOKEN", "%github-pull-request-token%")
    }

    steps {
      setVersion("Set release version", "%releaseVersion%")

      commonMaven {
        this.name = "Build versionalised package"
        goals = "package"
        runnerArgs = "$MAVEN_DEFAULT_ARGS -DskipTests"
      }

      commitAndPush("Push release version", "build: release version %releaseVersion%")

      commonMaven {
        this.name = "Release to Github"
        goals = "jreleaser:full-release"
        runnerArgs = "$MAVEN_DEFAULT_ARGS -Prelease -pl :packaging"
      }

      setVersion("Set next snapshot version", "%nextSnapshotVersion%")

      commitAndPush("Push next snapshot version", "build: update version to %nextSnapshotVersion%")
    }

    features {
      dockerSupport {}
    }

    requirements { runOnLinux(LinuxSize.SMALL) }

})

fun BuildSteps.setVersion(name: String, version: String): MavenBuildStep {
  return this.commonMaven {
    this.name = name
    goals = "versions:set"
    runnerArgs = "$MAVEN_DEFAULT_ARGS -DnewVersion=$version -DgenerateBackupPoms=false"
  }
}

fun BuildSteps.commitAndPush(name: String, commitMessage: String, includeFiles: String = "\\*pom.xml"): ScriptBuildStep {
  return this.script {
    this.name = name
    scriptContent =
        """
          #!/bin/bash -eu              
         
          git add $includeFiles
          git commit -m "$commitMessage"
          git push
        """.trimIndent()
  }
}

