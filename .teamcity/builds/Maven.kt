package builds

import jetbrains.buildServer.configs.kotlin.v10.toExtId
import jetbrains.buildServer.configs.kotlin.v2019_2.BuildType
import jetbrains.buildServer.configs.kotlin.v2019_2.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.v2019_2.buildFeatures.freeDiskSpace
import jetbrains.buildServer.configs.kotlin.v2019_2.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.v2019_2.buildSteps.maven

class Maven(id: String, name: String, goals: String, args: String? = null) : BuildType({
    this.id(id.toExtId())
    this.name = name

    steps {
        maven {
            this.goals = goals
            this.runnerArgs = args

            dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
            dockerImage = "eclipse-temurin:11-jdk"
            dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
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
