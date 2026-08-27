plugins {
    `java-library`
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
    withSourcesJar()
}

dependencies {
    api(platform("software.amazon.awssdk:bom:2.53.1"))
    api(platform("com.fasterxml.jackson:jackson-bom:2.22.1"))

    api("com.amazonaws:aws-lambda-java-core:1.4.0")
    api("software.amazon.awssdk:lambda")
    api("com.fasterxml.jackson.core:jackson-databind")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    api("org.slf4j:slf4j-api:2.0.18")

    testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
    testImplementation("org.mockito:mockito-core:5.23.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.1.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.18")
}

tasks.test {
    useJUnitPlatform()
}
