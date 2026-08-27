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
    api(project(":java-core"))
    api(platform("software.amazon.awssdk:bom:2.53.1"))
    api("com.amazonaws:aws-lambda-java-core:1.4.0")
    api("software.amazon.awssdk:lambda")

    testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
    testImplementation("org.mockito:mockito-core:5.23.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.1.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.18")
}

tasks.test {
    useJUnitPlatform()
}
