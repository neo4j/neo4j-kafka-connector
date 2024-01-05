package builds

import jetbrains.buildServer.configs.kotlin.AbsoluteId
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class WhiteListCheck(id: String, name: String) :
    BuildType({
      this.id(id.toId())
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
          scriptContent =
              """
                #!/bin/bash -eu
                
                token="%github-pull-request-token%"
                
                echo "Checking committers on PR %teamcity.build.branch%"
                
                # process pull request
                ./whitelist-check/bin/examine-pull-request $GITHUB_OWNER $GITHUB_REPOSITORY "${'$'}{token}" %teamcity.build.branch% whitelist-check/cla-database.csv
            """
                  .trimIndent()
          formatStderrAsError = true
        }
      }

      requirements { runOnLinux(LinuxSize.SMALL) }
    })
