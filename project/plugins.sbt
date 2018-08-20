// You may use this file to add plugin dependencies for sbt.

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

// scapegoat: static analysis compiler plugin
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.0.9")

// scalastyle: coding style check and enforcer
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

