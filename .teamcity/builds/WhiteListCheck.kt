package builds

import jetbrains.buildServer.configs.kotlin.v10.toExtId
import jetbrains.buildServer.configs.kotlin.v2019_2.AbsoluteId
import jetbrains.buildServer.configs.kotlin.v2019_2.BuildType
import jetbrains.buildServer.configs.kotlin.v2019_2.buildSteps.script

class WhiteListCheck(id: String, name: String) : BuildType({
    this.id(id.toExtId())
    this.name = name

    dependencies {
        artifacts(AbsoluteId("Tools_WhitelistCheck")) {
            buildRule = lastSuccessful()
            cleanDestination = true
            artifactRules = "whitelist-check.tar.gz!** => whitelist-check/"
        }
    }

    steps {
        script {
            scriptContent = """
                #!/bin/bash -eu
                
                token="%github-pull-request-token%"
                
                echo "Checking committers on PR %teamcity.build.branch%"
                
                # process pull request
                ./whitelist-check/bin/examine-pull-request $GITHUB_OWNER $GITHUB_REPOSITORY "${'$'}{token}" %teamcity.build.branch% whitelist-check/cla-database.csv
            """.trimIndent()
            formatStderrAsError = true
        }
    }

    requirements {
        runOnLinux(LinuxSize.SMALL)
    }
})
