import builds.Build
import jetbrains.buildServer.configs.kotlin.project
import jetbrains.buildServer.configs.kotlin.version

version = "2023.05"

project {
    params {
        password("github-commit-status-token", "credentialsJSON:be6ac011-ba27-4f2e-a628-edce6708504e")
        password("github-pull-request-token", "credentialsJSON:be6ac011-ba27-4f2e-a628-edce6708504e")
    }

    subProject(
        Build(
            "main", """
                +:main
            """.trimIndent(), false
        )
    )
    subProject(
        Build(
            "pull-request", """
                +:pull/*
            """.trimIndent(), true
        )
    )
}

