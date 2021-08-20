name := "utils"

version := "0.1"

scalaVersion := "2.12.10"

//unmanagedJars in =+ ""

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  //"io.fabric8" % "kubernetes-client" % "5.6.0",

  //Date range
  //"io.lamma" % "lamma" % "1.6.2",

  //Scala HTTP
  //"org.scalaj" % "scalaj-http" % "2.4.2",


  //MINIO
  "io.minio" % "minio" % "7.0.0",
  // "io.minio" % "minio" % "8.0.0",

  //Kubernetes
  "io.fabric8" % "kubernetes-client" % "5.0.0",
  "io.jsonfire" % "gson-fire" % "1.8.5"

  //"io.kubernetes" % "client-java" % "11.0.0",
  //"io.kubernetes" % "client-java-api" % "11.0.0",

  //scala k8s
  //"io.skuber" % "skuber" % "2.6.0"

  //openstack
  //"org.apache.hadoop" % "hadoop-openstack" %3.3.1%
  //
)

//remove files during package
//mappings in (compile, packageBin) ~= { _.filter(!_._1.getName.equalsIgnoreCase("application_test.properties"))
//}
