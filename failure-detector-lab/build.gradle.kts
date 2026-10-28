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

tasks.register<JavaExec>("ringMembershipReconfigurationExperiment") {
    group = "verification"
    description = "Compares static and ideal shared-view Ring membership reconfiguration."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.cli.RingMembershipReconfigurationMain")
}

tasks.register<JavaExec>("distributedRingMembershipExperiment") {
    group = "verification"
    description = "Runs delayed per-node Ring-2 membership reconfiguration."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.cli.DistributedRingMembershipMain")
}

tasks.register<JavaExec>("ringPartitionSafetyExperiment") {
    group = "verification"
    description = "Compares partition membership safety with and without Verify Suspect."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.cli.RingPartitionSafetyMain")
}

tasks.register<JavaExec>("ringPartitionRobustnessExperiment") {
    group = "verification"
    description = "Sweeps Verify helper partition and seeded packet loss."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.cli.RingPartitionRobustnessMain")
}

tasks.register<JavaExec>("ringVerificationQuorumExperiment") {
    group = "verification"
    description = "Compares multi-helper Verify failure quorums under partition and loss."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.cli.RingVerificationQuorumMain")
}

tasks.register<JavaExec>("udpComparisonReport") {
    group = "verification"
    description = "Aggregates real-UDP Ring H3/Q3/R3 and SWIM Lifeguard matrix results."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.cli.UdpComparisonReportMain")
}

tasks.register<JavaExec>("udpDistributedMembershipReport") {
    group = "verification"
    description = "Aggregates real-UDP distributed Ring membership survivor results."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.cli.UdpDistributedMembershipReportMain")
}

tasks.register<JavaExec>("udpDistributedMembershipMatrixReport") {
    group = "verification"
    description = "Aggregates repeated real-UDP distributed Ring membership runs."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.openkache.failuredetector.cli.UdpDistributedMembershipMatrixReportMain")
}
