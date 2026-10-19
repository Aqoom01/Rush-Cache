plugins {
    application
}

dependencies {
    implementation(project(":cache-core"))

    implementation(libs.netty.handler)
    implementation(libs.netty.codec.base)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)
    implementation(libs.prometheus.metrics.core)
    implementation(libs.prometheus.metrics.exporter.httpserver)
    implementation(libs.prometheus.metrics.instrumentation.jvm)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass = "io.openkache.server.Main"
    applicationName = "openkache-server"
}
