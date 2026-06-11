// Standalone AAR packaging project for the patched moq-ffi UniFFI bindings.
// Not part of the app build — run on demand when the AAR is rebuilt:
//   ./gradlew -p tools/moq-aar assembleRelease
// See patches/README.md for the full AAR build pipeline.
pluginManagement {
    repositories {
        google {
            content {
                includeGroupByRegex("com\\.android.*")
                includeGroupByRegex("com\\.google.*")
                includeGroupByRegex("androidx.*")
            }
        }
        mavenCentral()
        gradlePluginPortal()
    }
}
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "moq-aar"
