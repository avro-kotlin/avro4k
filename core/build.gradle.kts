
import org.jetbrains.kotlin.gradle.plugin.mpp.KotlinNativeTargetWithSimulatorTests
import org.jetbrains.kotlin.konan.target.Family

plugins {
    id("library-multiplatform-module-conventions")
    id("library-publish-conventions")
    kotlin("plugin.serialization")
}

description = "Core module of avro4k. Avro4k is the avro binary format support for kotlin, built on top of kotlinx-serialization."

kotlin {
    jvm()

    iosArm64()
    iosSimulatorArm64()
    macosArm64()
    watchosSimulatorArm64()
    watchosArm32()
    watchosArm64()
    tvosSimulatorArm64()
    tvosArm64()
    watchosDeviceArm64()
    iosX64()

    androidNativeArm32()
    androidNativeArm64()
    androidNativeX86()
    androidNativeX64()

    mingwX64()

    linuxArm64()
    linuxX64()

    js(IR) {
        browser()
        nodejs()
    }
//    @OptIn(ExperimentalWasmDsl::class)
//    wasmJs { browser() }

    sourceSets {
        commonMain {
            dependencies {
                api(libs.kotlinx.serialization.core)
                api(libs.kotlinx.serialization.json)
                api(libs.kotlinx.io)
            }
        }

        commonTest {
            dependencies {
                implementation(kotlin("test"))
                implementation(libs.kotest.core)
            }
        }

        jvmMain {
            dependencies {
                api(libs.apache.avro)
                implementation(libs.okio)
                implementation(libs.mockk)
            }
        }

        jvmTest {
            dependencies {
                implementation(libs.kotest.junit5)
                implementation(kotlin("reflect"))
            }
        }
    }
}

// Skip Apple simulator test tasks when the required simulator runtime is not installed
run {
    val simulatorFamilyPrefixes =
        mapOf(
            Family.IOS to "iOS ",
            Family.TVOS to "tvOS ",
            Family.WATCHOS to "watchOS "
        )

    val installedFamilies: Set<Family> =
        try {
            val lines =
                providers.exec {
                    commandLine("xcrun", "simctl", "list", "runtimes")
                }.standardOutput.asText.get().lines()
            simulatorFamilyPrefixes.filterValues { prefix -> lines.any { it.startsWith(prefix) } }.keys
        } catch (_: Exception) {
            emptySet()
        }

    kotlin.testableTargets
        .filterIsInstance<KotlinNativeTargetWithSimulatorTests>()
        .filter { it.konanTarget.family in simulatorFamilyPrefixes }
        .forEach { target ->
            val family = target.konanTarget.family
            tasks.named("${target.name}Test") {
                onlyIf("No $family simulator runtime installed") { family in installedFamilies }
            }
        }
}

spotless {
    json {
        target("**.json")
        prettier()
    }
}