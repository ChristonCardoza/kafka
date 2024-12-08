plugins {
    id("java")
}

group = "org.cardoza.kafka"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation ("org.apache.kafka:kafka-clients:3.7.0")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation ("org.slf4j:slf4j-api:2.0.13")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation ("org.slf4j:slf4j-simple:2.0.9")

    // https://mvnrepository.com/artifact/org.opensearch.client/opensearch-rest-high-level-client
//    implementation("org.opensearch.client:opensearch-rest-high-level-client:1.2.4")
    implementation ("org.opensearch.client:opensearch-java:2.6.0")

    // Apache HttpClient (transport for OpenSearch)
    implementation ("org.apache.httpcomponents.client5:httpclient5:5.2")
    implementation ("org.apache.httpcomponents.core5:httpcore5:5.2")
    implementation ("org.apache.httpcomponents.client5:httpclient5-fluent:5.2")

    // https://mvnrepository.com/artifact/com.google.code.gson/gson
    implementation("com.google.code.gson:gson:2.9.0")


}

tasks.test {
    useJUnitPlatform()
}