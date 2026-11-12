plugins {
    application
}

dependencies {
    implementation(project(":cache-core"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    // 통합 테스트에서만 실제 서버를 띄워 end-to-end로 검증한다. production 코드는 cache-server를 참조하지 않는다.
    testImplementation(project(":cache-server"))
}

application {
    mainClass = "io.openkache.runner.Main"
    applicationName = "openkache-experiment-runner"
    applicationDefaultJvmArgs = listOf("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
}

/*
 * 공통 benchmark v1을 기존 단일 노드 Runner와 분리한다.
 * main/test/application 배포본과 공통 API는 그대로 유지한다.
 */
val benchmark = sourceSets.create("benchmark")
configurations[benchmark.implementationConfigurationName]
    .extendsFrom(configurations.implementation.get())

val benchmarkTest = sourceSets.create("benchmarkTest")
benchmarkTest.compileClasspath += benchmark.output
benchmarkTest.runtimeClasspath += benchmark.output
configurations[benchmarkTest.implementationConfigurationName]
    .extendsFrom(configurations.testImplementation.get(),
        configurations[benchmark.implementationConfigurationName])
configurations[benchmarkTest.runtimeOnlyConfigurationName]
    .extendsFrom(configurations.testRuntimeOnly.get())

val benchmarkTestTask = tasks.register<Test>("benchmarkTest") {
    description = "공통 benchmark v1의 논리 trace 생성기를 검증한다."
    group = "verification"
    testClassesDirs = benchmarkTest.output.classesDirs
    classpath = benchmarkTest.runtimeClasspath
}

tasks.named("check") {
    dependsOn(benchmarkTestTask)
}

tasks.register<JavaExec>("benchmarkTrace") {
    description = "논리 trace/hash만 생성한다. 서버로 요청을 보내지 않는다."
    group = "verification"
    dependsOn(benchmark.classesTaskName)
    classpath = benchmark.runtimeClasspath
    mainClass = "io.openkache.runner.benchmark.TraceTool"
    val traceConfig = providers.gradleProperty("traceConfig")
    val traceOutput = providers.gradleProperty("traceOutput")
    val emitTraces = providers.gradleProperty("emitTraces")
    val traceRoot = rootProject.layout.projectDirectory
    doFirst {
        require(traceConfig.isPresent && traceOutput.isPresent) {
            "Use -PtraceConfig=PATH -PtraceOutput=NEW_DIRECTORY [-PemitTraces=true]"
        }
        val emit = emitTraces.orNull
        require(emit == null || emit == "true" || emit == "false") {
            "emitTraces must be true or false"
        }
        args(traceRoot.file(traceConfig.get()).asFile.absolutePath,
            traceRoot.file(traceOutput.get()).asFile.absolutePath)
        if (emit == "true") { args("--emit-traces") }
    }
}

/* 서버 실험 adapter가 main Runner/JAR 대신 이 독립 artifact만 참조한다. */
val benchmarkJar = tasks.register<Jar>("benchmarkJar") {
    archiveClassifier = "benchmark"
    from(benchmark.output)
}
configurations.create("benchmarkElements") {
    isCanBeConsumed = true
    isCanBeResolved = false
    extendsFrom(configurations[benchmark.implementationConfigurationName])
    outgoing.artifact(benchmarkJar)
}


// 연구별 이름·main은 config에만 둔다. 모든 모듈의 testClasses가 준비된 뒤 실행 입력을 기록한다.
val prepareResearchBackends = tasks.register<JavaExec>("prepareResearchBackends") {
    dependsOn(rootProject.subprojects.map { "${it.path}:testClasses" })
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass = "io.openkache.runner.ResearchBuild"
    args(rootProject.fileTree("experiments") {
        include("**/*.properties")
        exclude("**/runs/**", "**/results/**")
    }.files.sorted().map { it.absolutePath })
}

tasks.named("check") { dependsOn(prepareResearchBackends) }
tasks.named("installDist") { dependsOn(prepareResearchBackends) }
