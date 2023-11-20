package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.maven
import jetbrains.buildServer.configs.kotlin.toId

class Release(id: String, name: String, artifactSource: BuildType) : BuildType({
  this.id(id.toId())
  this.name = name

  params {
    text("version", "", allowEmpty = false)

    text("env.PACKAGES_USERNAME", "%github-packages-user%")
    password("env.PACKAGES_PASSWORD", "%github-packages-token%")
    password("env.JRELEASER_GITHUB_TOKEN", "%github-packages-token%")
  }

  dependencies {
    artifacts(artifactSource) {
      artifactRules = """
        +:packages/target/*.jar => packaging/target/*.jar
        +:packages/target/*.zip => packaging/target/*.zip
      """.trimIndent()
    }
  }

  steps {
    maven {
      goals = "versions:set"
      runnerArgs = "$MAVEN_DEFAULT_ARGS -DnewVersion=%version%"

      // this is the settings name we uploaded to Connectors project
      userSettingsSelection = "github"
      localRepoScope = MavenBuildStep.RepositoryScope.MAVEN_DEFAULT

      dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
      dockerImage = "eclipse-temurin:11-jdk"
      dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
    }

    maven {
      goals = "jreleaser:full-release"
      runnerArgs = "$MAVEN_DEFAULT_ARGS -Prelease -pl :packaging"

      // this is the settings name we uploaded to Connectors project
      userSettingsSelection = "github"
      localRepoScope = MavenBuildStep.RepositoryScope.MAVEN_DEFAULT

      dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
      dockerImage = "eclipse-temurin:11-jdk"
      dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
    }
  }

  features { dockerSupport {} }

  requirements { runOnLinux(LinuxSize.SMALL) }

})

