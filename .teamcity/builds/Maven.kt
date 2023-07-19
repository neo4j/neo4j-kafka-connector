package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep.RepositoryScope
import jetbrains.buildServer.configs.kotlin.buildSteps.maven
import jetbrains.buildServer.configs.kotlin.toId

class Maven(id: String, name: String, goals: String, args: String? = null) : BuildType({
    this.id(id.toId())
    this.name = name

    steps {
        maven {
            this.goals = "install"
            this.pomLocation = "build-resources/pom.xml"

            dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
            dockerImage = "eclipse-temurin:11-jdk"
            dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
        }
        maven {
            this.goals = goals
            this.runnerArgs = args

            dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
            dockerImage = "eclipse-temurin:11-jdk"
            dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"

            localRepoScope = RepositoryScope.BUILD_CONFIGURATION
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
