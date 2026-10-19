rootProject.name = "openkache"

dependencyResolutionManagement {
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
    }
}

include("cache-core", "cache-server", "experiment-runner")
