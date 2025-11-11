plugins {
    id("com.vanniktech.maven.publish")
}

group = "com.github.avro-kotlin.avro4k"

mavenPublishing {
    publishToMavenCentral()
    // do not sign local builds
    if (version != "local-SNAPSHOT") {
        signAllPublications()
    }
}

mavenPublishing {
    coordinates(
        artifactId = "avro4k-${project.name}",
    )
    pom {
        val projectUrl = "https://github.com/avro-kotlin/avro4k"
        name = project.name
        description = project.description?.ifEmpty { null } ?: error("Missing ${project.name} project description")
        url = projectUrl

        scm {
            connection = "scm:git:$projectUrl"
            developerConnection = "scm:git:$projectUrl"
            url = projectUrl
        }

        licenses {
            license {
                name = "Apache-2.0"
                url = "https://opensource.org/licenses/Apache-2.0"
            }
        }

        developers {
            developer {
                id = "thake"
                name = "Thorsten Hake"
                email = "mail@thorsten-hake.com"
            }
            developer {
                id = "chuckame"
                name = "Antoine Michaud"
                email = "contact@antoine-michaud.fr"
            }
        }
    }
}