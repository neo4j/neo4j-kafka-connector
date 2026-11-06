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

            script {
              this.name = "Upload artifacts to S3"
              scriptContent =
                  """
                  #!/bin/bash

                  # The release is already published at this point, so a problem here must not
                  # fail the build. We print a TeamCity warning instead and exit successfully.
                  set -u

                  warn() {
                    echo "##teamcity[message text='${'$'}1' status='WARNING']"
                  }

                  MANUAL="Run ./scripts/upload-release-to-s3.sh %releaseVersion% manually"

                  for tool in curl aws; do
                    if ! command -v ${'$'}tool >/dev/null 2>&1; then
                      warn "S3 upload skipped, ${'$'}tool is not available in this image. ${'$'}MANUAL"
                      exit 0
                    fi
                  done

                  if [ "%dry-run%" = "true" ]; then
                    echo "dry run: only downloads and verifies, nothing is uploaded"
                    export AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy AWS_DEFAULT_REGION=us-east-1
                    export AWS_EXTRA_ARGS=--dryrun
                  fi

                  if ! bash ./scripts/upload-release-to-s3.sh "%releaseVersion%"; then
                    warn "S3 upload FAILED, the release itself is fine. ${'$'}MANUAL"
                  fi

                  exit 0
                  """
                      .trimIndent()

              dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
              dockerImage = javaVersion.dockerImage
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
