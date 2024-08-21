package builds

import jetbrains.buildServer.configs.kotlin.BuildStep
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.buildCache
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.maven
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class IntegrationTests(id: String, name: String, javaVersion: String, init: BuildType.() -> Unit) :
    BuildType({
      this.id("${id}-${javaVersion}".toId())
      this.name = "$name (Java $javaVersion)"
      init()

      // we uploaded a custom settings.xml file in Teamcity UI, under Connectors project
      // with the following content, so we set the relevant environment variables here.

      /*
      <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                                    http://maven.apache.org/xsd/settings-1.0.0.xsd">
          <servers>
            <server>
              <id>github</id>
              <username>${env.PACKAGES_USERNAME}</username>
              <password>${env.PACKAGES_PASSWORD}</password>
            </server>
          </servers>
      </settings>
       */

      val javaDockerImage =
          when (javaVersion) {
            "11" -> "eclipse-temurin:11-jdk"
            "17" -> "eclipse-temurin:17-jdk"
            else -> error("Unsupported Java version: $javaVersion")
          }

      params {
        text("env.PACKAGES_USERNAME", "%github-packages-user%")
        password("env.PACKAGES_PASSWORD", "%github-packages-token%")

        text("env.BROKER_EXTERNAL_HOST", "broker:29092")
        text("env.SCHEMA_CONTROL_REGISTRY_URI", "http://schema-registry:8081")
        text("env.SCHEMA_CONTROL_REGISTRY_EXTERNAL_URI", "http://schema-registry:8081")
        text("env.KAFKA_CONNECT_EXTERNAL_URI", "http://connect:8083")
        text("env.NEO4J_URI", "neo4j://neo4j")
        text("env.NEO4J_EXTERNAL_URI", "neo4j://neo4j")
        text("env.NEO4J_USER", "neo4j")
        text("env.NEO4J_PASSWORD", "password")
      }

      steps {
        script {
          scriptContent =
              """
                #!/bin/bash -eu
                # TODO: publish custom image instead
                apt-get update
                apt-get install --yes ruby-full
                gem install dip
                curl -fsSL https://get.docker.com | sh
                dip compose up -d neo4j zookeeper broker schema-registry control-center
                until [ "`docker inspect -f {{.State.Health.Status}} control-center`"=="healthy" ]; do
                    sleep 0.1;
                done;
            """
                  .trimIndent()
          formatStderrAsError = true

          dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
          dockerImage = javaDockerImage
          dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
        }
        maven {
          this.goals = "verify"
          this.runnerArgs = "$MAVEN_DEFAULT_ARGS -Djava.version=$javaVersion -DskipUnitTests"

          // this is the settings name we uploaded to Connectors project
          userSettingsSelection = "github"
          localRepoScope = MavenBuildStep.RepositoryScope.MAVEN_DEFAULT

          dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
          dockerImage = javaDockerImage
          dockerRunParameters =
              "--volume /var/run/docker.sock:/var/run/docker.sock --network neo4j-kafka-connector_default"
        }
        script {
          scriptContent =
              """
                #!/bin/bash -eu
                # TODO: publish custom image instead
                apt-get update
                apt-get install --yes ruby-full
                gem install dip
                curl -fsSL https://get.docker.com | sh
                dip compose logs --no-color
                dip compose down --rmi local
            """
                  .trimIndent()
          formatStderrAsError = true

          executionMode = BuildStep.ExecutionMode.ALWAYS
          dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
          dockerImage = javaDockerImage
          dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
        }
      }

      features {
        dockerSupport {}

        buildCache {
          this.name = "neo4j-kafka-connector"
          publish = true
          use = true
          publishOnlyChanged = true
          rules = ".m2/repository"
        }
      }

      requirements { runOnLinux(LinuxSize.LARGE) }
    })
