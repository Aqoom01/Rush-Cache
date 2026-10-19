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
}
