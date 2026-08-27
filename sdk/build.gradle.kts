plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    `java-library`
}

kotlin {
    jvmToolchain(21)
    explicitApi()
}

dependencies {
    api(project(":java-core"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.11.0")
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")

    testImplementation(kotlin("test"))
    testImplementation(project(":java-testing"))
    testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.1.3")
}

tasks.test {
    useJUnitPlatform()
}
