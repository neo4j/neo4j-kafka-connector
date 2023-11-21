package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
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
        runnerArgs = "$MAVEN_DEFAULT_ARGS -DnewVersion=%version%"
      }

      commonMaven {
        goals = "package"
        runnerArgs = "$MAVEN_DEFAULT_ARGS -DskipTests"
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

