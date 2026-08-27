plugins {
    kotlin("jvm")
}

kotlin {
    jvmToolchain(21)
    explicitApi()
}

dependencies {
    api(platform("com.fasterxml.jackson:jackson-bom:2.22.1"))
    api("com.fasterxml.jackson.core:jackson-databind")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.1.3")
}

tasks.test {
    useJUnitPlatform()
}
