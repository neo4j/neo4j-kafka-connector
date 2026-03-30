package builds

import jetbrains.buildServer.configs.kotlin.BuildSteps
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

private const val DRY_RUN = "dry-run"

class Release(id: String, name: String, javaVersion: JavaVersion) :
    BuildType(
        {
          this.id(id.toId())
          this.name = name

          params {
            text(
                "releaseVersion",
                "",
                allowEmpty = false,
                display = ParameterDisplay.PROMPT,
                label = "Version to release",
            )
            text(
                "nextSnapshotVersion",
                "",
                allowEmpty = false,
                display = ParameterDisplay.PROMPT,
                label = "Version on the next snapshot after the release",
            )
            checkbox(
                DRY_RUN,
                "true",
                "Dry run?",
                description =
                    "Whether to perform a dry run where nothing is published and released",
                display = ParameterDisplay.PROMPT,
                checked = "true",
                unchecked = "false",
            )

            password("env.JRELEASER_GITHUB_TOKEN", "%github-pull-request-token%")

            text("env.JRELEASER_DRY_RUN", "%$DRY_RUN%")
            text("env.JRELEASER_PROJECT_VERSION", "%releaseVersion%")

            text("env.JRELEASER_ANNOUNCE_SLACK_ACTIVE", "NEVER")
            text("env.JRELEASER_ANNOUNCE_SLACK_TOKEN", "%slack-token%")
          }

          steps {
            setVersion("Set release version", "%releaseVersion%", javaVersion)

            commitAndPush(
                "Push release version",
                "build: release version %releaseVersion%",
                dryRunParameter = DRY_RUN,
            )

            script {
              scriptContent =
                  """
                  #!/bin/bash

                  set -eux

                  if [ "%dry-run%" = "true" ]; then
                    echo "we are on a dry run"
                    export JRELEASER_ANNOUNCE_SLACK_ACTIVE=NEVER
                  else
                    echo "we will do a full release"
                    export JRELEASER_ANNOUNCE_SLACK_ACTIVE=ALWAYS
                  fi
                  export MAVEN_ARGS="$MAVEN_DEFAULT_ARGS"

                  jreleaser assemble
                  jreleaser full-release
                  """
                      .trimIndent()

              dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
              dockerImage = javaVersion.dockerImage
              dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
            }

            setVersion("Set next snapshot version", "%nextSnapshotVersion%", javaVersion)

            commitAndPush(
                "Push next snapshot version",
                "build: update version to %nextSnapshotVersion%",
                dryRunParameter = DRY_RUN,
            )
          }

          features { buildCache(javaVersion) }

          requirements { runOnLinux(LinuxSize.SMALL) }
        },
    )

fun BuildSteps.setVersion(name: String, version: String, javaVersion: JavaVersion): MavenBuildStep {
  return this.commonMaven(javaVersion) {
    this.name = name
    goals = "versions:set"
    runnerArgs =
        "$MAVEN_DEFAULT_ARGS -Djava.version=${javaVersion.version} -DnewVersion=$version -DgenerateBackupPoms=false"
  }
}

fun BuildSteps.commitAndPush(
    name: String,
    commitMessage: String,
    includeFiles: String = "\\*pom.xml",
    dryRunParameter: String = "dry-run",
): ScriptBuildStep {
  return this.script {
    this.name = name
    scriptContent =
        """
          #!/bin/bash -eu              
         
          git add $includeFiles
          git commit -m "$commitMessage"
          git push
        """
            .trimIndent()

    conditions { doesNotMatch(dryRunParameter, "true") }
  }
}
