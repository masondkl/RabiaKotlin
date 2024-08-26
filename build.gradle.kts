plugins {
    kotlin("jvm").version("1.9.0")
}

group = "me.mason"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral(); mavenLocal()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")
    implementation("com.github.exerosis.mynt:Mynt:1.1.0")
}

tasks.compileKotlin.get().kotlinOptions {
    languageVersion = "1.9"
    jvmTarget = "17"
    freeCompilerArgs = listOf(
        "-Xcontext-receivers", "-Xinline-classes",
        "-Xopt-in=kotlin.time.ExperimentalTime",
        "-Xopt-in=kotlin.contracts.ExperimentalContracts",
        "-Xopt-in=kotlin.ExperimentalUnsignedTypes",
        "-Xopt-in=kotlinx.coroutines.ExperimentalCoroutinesApi",
        "-Xopt-in=kotlinx.coroutines.DelicateCoroutinesApi",
        "-Xopt-in=kotlinx.coroutines.InternalCoroutinesApi"
    )
}