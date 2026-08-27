pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}

rootProject.name = "durable-execution-kt"

include("java-core")
include("java-testing")
include("core")
include("testing")
include("sdk")
include("conformance-tests")
