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

/* O00 실험 adapter만 추가한다. 기존 main/test/application 의존성과 wire는 보존한다. */
val benchmark = sourceSets.create("benchmark")
benchmark.compileClasspath += sourceSets.main.get().output
benchmark.runtimeClasspath += sourceSets.main.get().output
configurations[benchmark.implementationConfigurationName]
    .extendsFrom(configurations.implementation.get())
dependencies {
    add(benchmark.implementationConfigurationName,
        project(mapOf("path" to ":experiment-runner", "configuration" to "benchmarkElements")))
}

val benchmarkTest = sourceSets.create("benchmarkTest")
benchmarkTest.compileClasspath += benchmark.output + sourceSets.main.get().output
benchmarkTest.runtimeClasspath += benchmark.output + sourceSets.main.get().output
configurations[benchmarkTest.implementationConfigurationName]
    .extendsFrom(configurations.testImplementation.get(),
        configurations[benchmark.implementationConfigurationName])
configurations[benchmarkTest.runtimeOnlyConfigurationName]
    .extendsFrom(configurations.testRuntimeOnly.get(), configurations.runtimeOnly.get())

val benchmarkTestTask = tasks.register<Test>("benchmarkTest") {
    description = "O00 장기 client adapter의 계수·deadline·로컬 TCP 연결을 검증한다."
    group = "verification"
    testClassesDirs = benchmarkTest.output.classesDirs
    classpath = benchmarkTest.runtimeClasspath
    // 새 TCP fixture만 기존 회귀 뒤에 배치한다. 기존 회귀의 병렬/timeout은 변경하지 않는다.
    mustRunAfter(":cache-core:test", ":cache-server:test", ":experiment-runner:test",
        ":experiment-runner:benchmarkTest")
}
tasks.named("check") {
    dependsOn(benchmarkTestTask)
}

