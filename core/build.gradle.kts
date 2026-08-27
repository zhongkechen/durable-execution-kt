plugins {
    kotlin("jvm")
}

kotlin {
    jvmToolchain(21)
    explicitApi()
}

dependencies {
    api(platform("software.amazon.awssdk:bom:2.53.1"))
    api(platform("com.fasterxml.jackson:jackson-bom:2.22.1"))
    api("software.amazon.awssdk:lambda")
    api("com.fasterxml.jackson.core:jackson-databind")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.1.3")
}

tasks.test {
    useJUnitPlatform()
}
