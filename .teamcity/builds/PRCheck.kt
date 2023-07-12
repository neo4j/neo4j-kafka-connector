package builds

import jetbrains.buildServer.configs.kotlin.v10.toExtId
import jetbrains.buildServer.configs.kotlin.v2019_2.BuildType
import jetbrains.buildServer.configs.kotlin.v2019_2.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.v2019_2.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.v2019_2.buildSteps.script

class PRCheck(id: String, name: String) : BuildType({
    this.id(id.toExtId())
    this.name = name

    steps {
        script {
            scriptContent = """
                #!/bin/bash
                
                set -eu
                
                export DANGER_GITHUB_API_TOKEN=%github-pull-request-token%
                export PULL_REQUEST_URL=https://github.com/$GITHUB_OWNER/$GITHUB_REPOSITORY/%teamcity.build.branch%
                
                # process pull request
                npm ci
                npx danger ci --verbose --failOnErrors
            """.trimIndent()

            dockerImage = "node:18.4"
            dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
        }
    }

    features {
        dockerSupport {
        }
    }

    requirements {
        runOnLinux(LinuxSize.SMALL)
    }
})
