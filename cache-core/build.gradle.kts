plugins {
    `java-library`
}

// cache-core의 production 의존성은 JDK 표준 API만 허용한다.
dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val verifyNoProductionDependencies = tasks.register("verifyNoProductionDependencies") {
    description = "cache-core runtimeClasspath에 외부 라이브러리가 없는지 확인한다."
    group = "verification"
    val runtimeClasspath = configurations.runtimeClasspath
    inputs.files(runtimeClasspath)
    doLast {
        val files = runtimeClasspath.get().files
        check(files.isEmpty()) {
            "cache-core must not have production dependencies: ${files.map { it.name }}"
        }
    }
}

tasks.named("check") {
    dependsOn(verifyNoProductionDependencies)
}

// B3의 독립 Java 진입점. 기본값은 목록 출력이며 build/check에서 실행하지 않는다.
tasks.register<JavaExec>("b3") {
    group = "application"
    description = "B3 Java 전략 목록·설정 검증·계획표 생성 (기본 실행은 실험 없음)"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass = "io.openkache.core.migration.strategy.B3Main"
    workingDir = rootProject.projectDir
    args(providers.gradleProperty("b3.args").map { it.split(" ").filter(String::isNotBlank) }.getOrElse(listOf("--list")))
}

tasks.register<JavaExec>("b3Research") {
    group = "application"
    description = "B3 독립 Java 연구 backend 실행 (명시 호출 전용)"
    dependsOn(tasks.named("testClasses"))
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "io.openkache.core.migration.experiment.B3ResearchLauncher"
    workingDir = rootProject.projectDir
    args(providers.gradleProperty("b3.args").map { it.split(" ").filter(String::isNotBlank) }.getOrElse(listOf("--help")))
}
