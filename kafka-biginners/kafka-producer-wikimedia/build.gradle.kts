plugins {
    id("java")
}

group = "org.cardoza.kafka"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation ("org.apache.kafka:kafka-clients:3.7.0")

    implementation ("org.slf4j:slf4j-api:2.0.13")

    implementation ("org.slf4j:slf4j-simple:2.0.9")

    implementation("com.squareup.okhttp3:okhttp:4.12.0")

    implementation("com.launchdarkly:okhttp-eventsource:4.1.1")
}

tasks.test {
    useJUnitPlatform()
}