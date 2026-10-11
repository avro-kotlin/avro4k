plugins {
    `kotlin-dsl`
}

dependencies {
    plugin(libs.plugins.maven.publish)
    plugin(libs.plugins.kotlin.jvm)
    plugin(libs.plugins.dokka)
    plugin(libs.plugins.dokka.javadoc)
    plugin(libs.plugins.kotlinx.binary.compatibility.validator)
    plugin(libs.plugins.spotless)

    // Hack to get type-safe `libs` inside convention plugins (gradle/gradle#15383)
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
}

fun DependencyHandler.plugin(dep: Provider<PluginDependency>) =
    compileOnly(dep.map { "${it.pluginId}:${it.pluginId}.gradle.plugin:${it.version.requiredVersion}" })

fun DependencyHandler.plugin(dep: ProviderConvertible<PluginDependency>)=
    plugin(dep.asProvider())
