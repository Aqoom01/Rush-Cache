plugins {
    `java-library`
}

// Keep the user-facing hyphenated variant folder and valid Java package paths separate.
val ioniaSecondRoot = "src/main/java/io/openkache/core/replication/ionia-second"
sourceSets {
    named("main") {
        java.exclude("io/openkache/core/replication/ionia-second/**")
        java.srcDir("$ioniaSecondRoot/src/main/java")
    }
    named("test") {
        java.srcDir("$ioniaSecondRoot/src/test/java")
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
