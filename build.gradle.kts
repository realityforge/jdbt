import net.ltgt.gradle.errorprone.errorprone

plugins {
  java
  jacoco
  id("com.diffplug.spotless") version "8.2.1"
  id("net.ltgt.errorprone") version "5.0.0"
}

group = "org.realityforge"
version = "0.1-SNAPSHOT"

repositories {
  mavenCentral()
}

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(17))
  }
  withSourcesJar()
}

dependencies {
  implementation("info.picocli:picocli:4.7.7")
  implementation("org.snakeyaml:snakeyaml-engine:2.10")
  implementation("org.jspecify:jspecify:1.0.0")
  implementation("com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11")
  implementation("org.postgresql:postgresql:42.7.8")

  errorprone("com.google.errorprone:error_prone_core:2.31.0")
  errorprone("com.uber.nullaway:nullaway:0.10.26")

  testImplementation(platform("org.junit:junit-bom:5.13.4"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation("org.assertj:assertj-core:3.27.3")
  testImplementation("org.mockito:mockito-core:5.18.0")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile>().configureEach {
  options.release.set(17)
  options.errorprone.disableWarningsInGeneratedCode.set(true)
  if (name == "compileJava") {
    options.errorprone.error("NullAway")
    options.errorprone.option("NullAway:AnnotatedPackages", "org.realityforge.jdbt")
    options.errorprone.option("NullAway:JSpecifyMode", "true")
  } else {
    options.errorprone.disable("NullAway")
  }
}

tasks.withType<Jar>().configureEach {
  isPreserveFileTimestamps = false
  isReproducibleFileOrder = true
}

spotless {
  java {
    palantirJavaFormat("2.87.0")
    target("src/*/java/**/*.java")
  }
}

tasks.jar {
  manifest {
    attributes["Main-Class"] = "org.realityforge.jdbt.Main"
  }
}

val fatJar by tasks.registering(Jar::class) {
  group = LifecycleBasePlugin.BUILD_GROUP
  description = "Builds a runnable fat jar."
  archiveClassifier.set("all")
  manifest {
    attributes["Main-Class"] = "org.realityforge.jdbt.Main"
  }
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
  from(sourceSets.main.get().output)
  dependsOn(configurations.runtimeClasspath)
  from(
    {
      configurations.runtimeClasspath.get().filter { it.name.endsWith(".jar") }.map { zipTree(it) }
    }
  ) {
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA", "META-INF/*.EC")
  }
}

tasks.assemble {
  dependsOn(fatJar)
}

tasks.test {
  useJUnitPlatform()
}

jacoco {
  toolVersion = "0.8.13"
}

val coverageExclusions = listOf("org/realityforge/jdbt/Main.class")

tasks.jacocoTestReport {
  dependsOn(tasks.test)
  classDirectories.setFrom(
    files(
      classDirectories.files.map { directory ->
        fileTree(directory) {
          exclude(coverageExclusions)
        }
      }
    )
  )
  reports {
    xml.required.set(true)
    html.required.set(true)
  }
}

tasks.jacocoTestCoverageVerification {
  dependsOn(tasks.test)
  classDirectories.setFrom(
    files(
      classDirectories.files.map { directory ->
        fileTree(directory) {
          exclude(coverageExclusions)
        }
      }
    )
  )
  violationRules {
    rule {
      element = "BUNDLE"
      limit {
        counter = "LINE"
        value = "COVEREDRATIO"
        minimum = "0.85".toBigDecimal()
      }
      limit {
        counter = "BRANCH"
        value = "COVEREDRATIO"
        minimum = "0.75".toBigDecimal()
      }
    }
  }
}

tasks.check {
  dependsOn(tasks.spotlessCheck)
  dependsOn(tasks.jacocoTestReport)
  dependsOn(tasks.jacocoTestCoverageVerification)
}
