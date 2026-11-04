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
