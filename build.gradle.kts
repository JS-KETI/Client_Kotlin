// Top-level (project) build file.
// Plugins applied to subprojects are declared here with `apply false`
// and activated inside each subproject's build.gradle.kts.
plugins {
    alias(libs.plugins.android.application) apply false
    alias(libs.plugins.kotlin.android) apply false
    alias(libs.plugins.kotlin.compose) apply false
}
