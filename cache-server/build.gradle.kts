import java.security.MessageDigest

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
    exclude("**/benchmark/variants/**")
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


// V02–V07 신규 검사만 분리한다. 기존 회귀/benchmark의 병렬·timeout·대상은 그대로 유지한다.
val variantBenchmarkTestTask = tasks.register<Test>("variantBenchmarkTest") {
    description = "V02–V07 공통 adapter·프로필·raw drain·로컬 TCP 연결을 검증한다."
    group = "verification"
    testClassesDirs = benchmarkTest.output.classesDirs
    classpath = benchmarkTest.runtimeClasspath
    include("**/benchmark/variants/**")
    exclude("**/benchmark/variants/Correctness*Test.class")
    exclude("**/benchmark/variants/Jvm*Test.class")
    exclude("**/benchmark/variants/Network*Test.class")
    mustRunAfter(benchmarkTestTask)
}
tasks.named("check") { dependsOn(variantBenchmarkTestTask) }

// 새 correctness TCP 검사만 별도 그룹으로 실행한다. 기존 99개 변형 검사는 그대로 둔다.
val correctnessBenchmarkTestTask = tasks.register<Test>("correctnessBenchmarkTest") {
    description = "O00/V01–V07 전수 이력과 예약 ACK 키의 로컬 재기동 감사를 검증한다."
    group = "verification"
    testClassesDirs = benchmarkTest.output.classesDirs
    classpath = benchmarkTest.runtimeClasspath
    include("**/benchmark/variants/Correctness*Test.class")
    mustRunAfter(variantBenchmarkTestTask)
}
tasks.named("check") { dependsOn(correctnessBenchmarkTestTask) }

// 신규 자원 관찰 TCP 검사만 마지막 그룹에 배치한다. 기존 기능 검사 대상을 유지한다.
val jvmBenchmarkTestTask = tasks.register<Test>("jvmBenchmarkTest") {
    description = "O00/V01–V07의 JVM 자원·1초 요청 창과 수집기 종료를 로컬 TCP로 검증한다."
    group = "verification"
    testClassesDirs = benchmarkTest.output.classesDirs
    classpath = benchmarkTest.runtimeClasspath
    include("**/benchmark/variants/Jvm*Test.class")
    mustRunAfter(correctnessBenchmarkTestTask)
}
tasks.named("check") { dependsOn(jvmBenchmarkTestTask) }

// Network observation tests use their own preparation gate; original populations remain unchanged.
val networkBenchmarkTestTask = tasks.register<Test>("networkBenchmarkTest") {
    description = "O00/V01–V07 sender frames, stream bytes and connection accounting."
    group = "verification"
    testClassesDirs = benchmarkTest.output.classesDirs
    classpath = benchmarkTest.runtimeClasspath
    include("**/benchmark/variants/Network*Test.class")
    mustRunAfter(jvmBenchmarkTestTask)
}
tasks.named("check") { dependsOn(networkBenchmarkTestTask) }

// Portable pilot is opt-in; production/application JAR and dependencies stay unchanged.
val pilotJar = tasks.register<Jar>("pilotJar") {
    archiveClassifier.set("pilot")
    from(benchmark.output)
}
val pilotLibraries = providers.provider {
    listOf(tasks.named<Jar>("jar").get().archiveFile.get().asFile,
        pilotJar.get().archiveFile.get().asFile) +
        configurations[benchmark.runtimeClasspathConfigurationName].files.sortedBy { it.name }
}
val pilotHashes = tasks.register("pilotHashes") {
    dependsOn(tasks.named("jar"), pilotJar, configurations[benchmark.runtimeClasspathConfigurationName])
    inputs.files(pilotLibraries)
    val hashFile = layout.buildDirectory.file("pilot/BUNDLE_SHA256SUMS")
    outputs.file(hashFile)
    doLast {
        val files = pilotLibraries.get()
        check(files.all { it.isFile && it.extension == "jar" })
        check(files.map { it.name }.distinct().size == files.size) { "Duplicate pilot library names" }
        val content = files.sortedBy { it.name }.joinToString("") { file ->
            val digest = MessageDigest.getInstance("SHA-256")
            file.inputStream().use { input ->
                val buffer = ByteArray(65536)
                while (true) { val n = input.read(buffer); if (n < 0) break; digest.update(buffer, 0, n) }
            }
            digest.digest().joinToString("") { "%02x".format(it.toInt() and 255) } + "  lib/" + file.name + "\n"
        }
        hashFile.get().asFile.apply { parentFile.mkdirs(); writeText(content, Charsets.UTF_8) }
    }
}
tasks.register<Zip>("pilotZip") {
    description = "O00 functional pilot bundle; no EC2 access or performance qualification."
    group = "distribution"
    dependsOn(pilotHashes)
    archiveFileName.set("ionia-pilot.zip")
    destinationDirectory.set(layout.buildDirectory.dir("distributions"))
    into("lib") { from(pilotLibraries) }
    from(layout.buildDirectory.file("pilot/BUNDLE_SHA256SUMS"))
    into("tools") { from(rootProject.file("experiments/ionia-pilot")) }
    duplicatesStrategy = DuplicatesStrategy.FAIL
}
