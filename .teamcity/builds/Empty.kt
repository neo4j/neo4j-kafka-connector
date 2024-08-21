package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.toId

class Empty(id: String, name: String, javaVersion: String) :
    BuildType({
      this.id("${id}-${javaVersion}".toId())
      this.name = "$name (Java $javaVersion)"

      requirements { runOnLinux(LinuxSize.SMALL) }
    })
