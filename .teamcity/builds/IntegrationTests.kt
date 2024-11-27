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

class IntegrationTests(
    id: String,
    name: String,
    javaVersion: JavaVersion,
    platformVersion: String,
    init: BuildType.() -> Unit
) :
    BuildType(
        {
          this.id("${id}-${javaVersion.version}-${platformVersion}".toId())
          this.name = "$name (Java ${javaVersion.version}) (Confluent Platform $platformVersion)"
          init()

          artifactRules =
              """
              +:diagnostics => diagnostics.zip
              """
                  .trimIndent()

          params {
            text("env.BROKER_EXTERNAL_HOST", "broker:29092")
            text("env.SCHEMA_CONTROL_REGISTRY_URI", "http://schema-registry:8081")
            text("env.SCHEMA_CONTROL_REGISTRY_EXTERNAL_URI", "http://schema-registry:8081")
            text("env.KAFKA_CONNECT_EXTERNAL_URI", "http://connect:8083")
            text("env.NEO4J_URI", "neo4j://neo4j")
            text("env.NEO4J_EXTERNAL_URI", "neo4j://neo4j")
            text("env.NEO4J_USER", "neo4j")
            text("env.NEO4J_PASSWORD", "password")
            text("env.CONFLUENT_PLATFORM_VERSION", platformVersion)
          }

          steps {
            script {
              scriptContent =
                  """
                #!/bin/bash -eu
                # TODO: publish custom image instead
                apt-get update
                apt-get install --yes ruby-full ruby-bundler
                bundle install
                curl -fsSL https://get.docker.com | sh
                dip compose up -d neo4j zookeeper broker schema-registry control-center
                until [ "`docker inspect -f {{.State.Health.Status}} control-center`"=="healthy" ]; do
                    sleep 0.1;
                done;
            """
                      .trimIndent()
              formatStderrAsError = true
              dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
              dockerImage = javaVersion.dockerImage
              dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
            }
            maven {
              this.goals = "verify"
              this.runnerArgs =
                  "$MAVEN_DEFAULT_ARGS -Djava.version=${javaVersion.version} -Dkafka-schema-registry.version=$platformVersion -DskipUnitTests"

              // this is the settings name we uploaded to Connectors project
              userSettingsSelection = "github"
              localRepoScope = MavenBuildStep.RepositoryScope.MAVEN_DEFAULT
              dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
              dockerImage = javaVersion.dockerImage
              dockerRunParameters =
                  "--volume /var/run/docker.sock:/var/run/docker.sock --network neo4j-kafka-connector_default"
            }
            script {
              scriptContent =
                  """
                #!/bin/bash -eu
                # TODO: publish custom image instead
                apt-get update
                apt-get install --yes ruby-full ruby-bundler
                bundle install
                curl -fsSL https://get.docker.com | sh
                mkdir diagnostics
                dip compose cp neo4j:/data diagnostics/data
                dip compose cp neo4j:/logs diagnostics/logs
                dip compose logs --no-color > ./diagnostics/docker-compose.logs
            """
                      .trimIndent()
              formatStderrAsError = true

              executionMode = BuildStep.ExecutionMode.RUN_ON_FAILURE
              dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
              dockerImage = javaVersion.dockerImage
              dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
            }
            script {
              scriptContent =
                  """
                #!/bin/bash -eu
                # TODO: publish custom image instead
                apt-get update
                apt-get install --yes ruby-full ruby-bundler
                bundle install
                curl -fsSL https://get.docker.com | sh
                dip compose down --rmi local
            """
                      .trimIndent()
              formatStderrAsError = true
              executionMode = BuildStep.ExecutionMode.ALWAYS
              dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
              dockerImage = javaVersion.dockerImage
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
        },
    )
