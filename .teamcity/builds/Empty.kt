package builds

import jetbrains.buildServer.configs.kotlin.v10.toExtId
import jetbrains.buildServer.configs.kotlin.v2019_2.BuildType

class Empty(id: String, name: String) : BuildType({
    this.id(id.toExtId())
    this.name = name

    requirements {
        runOnLinux(LinuxSize.SMALL)
    }
})