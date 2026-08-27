plugins {
    kotlin("jvm") version "2.4.10" apply false
    kotlin("plugin.serialization") version "2.4.10" apply false
    id("com.gradleup.shadow") version "9.6.1" apply false
}

allprojects {
    group = "io.github.zhongkechen"
    version = "0.1.0-SNAPSHOT"
}
