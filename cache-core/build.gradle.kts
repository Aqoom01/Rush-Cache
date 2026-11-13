plugins {
    `java-library`
}

// Keep the user-facing hyphenated variant folder and valid Java package paths separate.
val ioniaSecondRoot = "src/main/java/io/openkache/core/replication/ionia-second"
val ioniaThirdRoot = "src/main/java/io/openkache/core/replication/ionia-third"
val ioniaFourthRoot = "src/main/java/io/openkache/core/replication/ionia-fourth"
val ioniaFifthRoot = "src/main/java/io/openkache/core/replication/ionia-fifth"
val ioniaSixthRoot = "src/main/java/io/openkache/core/replication/ionia-sixth"
val ioniaSeventhRoot = "src/main/java/io/openkache/core/replication/ionia-seventh"
val ioniaEighthRoot = "src/main/java/io/openkache/core/replication/ionia-eighth"
sourceSets {
    named("main") {
        java.exclude("io/openkache/core/replication/ionia-second/**")
        java.srcDir("$ioniaSecondRoot/src/main/java")
        java.exclude("io/openkache/core/replication/ionia-third/**")
        java.srcDir("$ioniaThirdRoot/src/main/java")
        java.exclude("io/openkache/core/replication/ionia-fourth/**")
        java.srcDir("$ioniaFourthRoot/src/main/java")
        java.exclude("io/openkache/core/replication/ionia-fifth/**")
        java.srcDir("$ioniaFifthRoot/src/main/java")
        java.exclude("io/openkache/core/replication/ionia-sixth/**")
        java.srcDir("$ioniaSixthRoot/src/main/java")
        java.exclude("io/openkache/core/replication/ionia-seventh/**")
        java.srcDir("$ioniaSeventhRoot/src/main/java")
        java.exclude("io/openkache/core/replication/ionia-eighth/**")
        java.srcDir("$ioniaEighthRoot/src/main/java")
    }
    named("test") {
        java.srcDir("$ioniaSecondRoot/src/test/java")
        java.srcDir("$ioniaThirdRoot/src/test/java")
        java.srcDir("$ioniaFourthRoot/src/test/java")
        java.srcDir("$ioniaFifthRoot/src/test/java")
        java.srcDir("$ioniaSixthRoot/src/test/java")
        java.srcDir("$ioniaSeventhRoot/src/test/java")
        java.srcDir("$ioniaEighthRoot/src/test/java")
    }
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

// 기존 알고리즘을 한 가상 클러스터에서 결합하는 명시 실행 전용 연구 backend.
tasks.register<JavaExec>("systemIntegration") {
    group = "application"
    description = "통합 조합 screening·독립 seed 재검증 (기본값: 지원 조합 목록)"
    dependsOn(tasks.named("testClasses"))
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "io.openkache.core.integration.SystemIntegrationMain"
    workingDir = rootProject.projectDir
    maxHeapSize = "768m"
    args(providers.gradleProperty("integration.args").map { it.split(" ").filter(String::isNotBlank) }.getOrElse(listOf("--list")))
}
