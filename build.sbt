name := "utils"

version := "0.1"

scalaVersion := "2.12.10"

//unmanagedJars in =+ ""

libraryDependencies += "io.minio" % "minio" % "7.0.0"
//libraryDependencies += "io.minio" % "minio" % "8.0.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided"
//libraryDependencies += "io.fabric8" % "kubernetes-client" % "5.6.0"


