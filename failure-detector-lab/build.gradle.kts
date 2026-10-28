plugins {
    java
    application
}

group = "com.openkache"
version = "0.1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.11.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass.set("com.openkache.failuredetector.cli.Main")
}

tasks.register<JavaExec>("selfTest") {
    group = "verification"
    description = "Runs dependency-free detector, health and failover assertions."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.validation.SelfTest")
}

tasks.register<JavaExec>("detailedExperiment") {
    group = "verification"
    description = "Runs the detailed broad failure-detector comparison matrix."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.cli.DetailedExperimentMain")
}

tasks.register<JavaExec>("parameterSweep") {
    group = "verification"
    description = "Sweeps SWIM suspicion and Phi threshold operating points."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.cli.ParameterSweepMain")
}

tasks.register<JavaExec>("ringAdjacentCrashExperiment") {
    group = "verification"
    description = "Runs the static Ring-N adjacent two-node crash edge-case matrix."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.cli.RingAdjacentCrashMain")
}
