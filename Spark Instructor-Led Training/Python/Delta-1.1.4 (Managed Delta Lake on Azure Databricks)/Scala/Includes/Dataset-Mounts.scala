// Databricks notebook source
val tags = com.databricks.logging.AttributionContext.current.tags

//*******************************************
// GET VERSION OF APACHE SPARK
//*******************************************

// Get the version of spark
val Array(sparkMajorVersion, sparkMinorVersion, _) = spark.version.split("""\.""")

// Set the major and minor versions
spark.conf.set("com.databricks.training.spark.major-version", sparkMajorVersion)
spark.conf.set("com.databricks.training.spark.minor-version", sparkMinorVersion)

//*******************************************
// GET VERSION OF DATABRICKS RUNTIME
//*******************************************

// Get the version of the Databricks Runtime
val version = tags.collect({ case (t, v) if t.name == "sparkVersion" => v }).head
val runtimeVersion = if (version != "") {
  spark.conf.set("com.databricks.training.job", "false")
  version
} else {
  spark.conf.set("com.databricks.training.job", "true")
  dbutils.widgets.get("sparkVersion")
}

val runtimeVersions = runtimeVersion.split("""-""")
// The GPU and ML runtimes push the number of elements out to 5
// so we need to account for every scenario here. There should
// never be a case in which there is less than two so we can fail
// with an helpful error message for <2 or >5
val (dbrVersion, scalaVersion) = {
  runtimeVersions match {
    case Array(d, _, _, _, s) => (d, s.replace("scala", ""))
    case Array(d, _, _, s)    => (d, s.replace("scala", ""))
    case Array(d, _, s)       => (d, s.replace("scala", ""))
    case Array(d, s)          => (d, s.replace("scala", ""))
    case _ =>
      throw new IllegalArgumentException(s"""Dataset-Mounts: Cannot parse version(s) from "${runtimeVersions.mkString(", ")}".""")
  }
}
val Array(dbrMajorVersion, dbrMinorVersion, _*) = dbrVersion.split("""\.""")

// Set the the major and minor versions
spark.conf.set("com.databricks.training.dbr.major-version", dbrMajorVersion)
spark.conf.set("com.databricks.training.dbr.minor-version", dbrMinorVersion)

//*******************************************
// GET USERNAME AND USERHOME
//*******************************************

// Get the user's name
val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")

val userhome = s"dbfs:/user/$username"

// Set the user's name and home directory
spark.conf.set("com.databricks.training.username", username)
spark.conf.set("com.databricks.training.userhome", userhome)

//**********************************
// VARIOUS UTILITY FUNCTIONS
//**********************************

def assertSparkVersion(expMajor:Int, expMinor:Int):String = {
  val major = spark.conf.get("com.databricks.training.spark.major-version")
  val minor = spark.conf.get("com.databricks.training.spark.minor-version")

  if ((major.toInt < expMajor) || (major.toInt == expMajor && minor.toInt < expMinor)) {
    throw new Exception(s"This notebook must be ran on Spark version $expMajor.$expMinor or better, found Spark $major.$minor")
  }
  return s"$major.$minor"
}

def assertDbrVersion(expMajor:Int, expMinor:Int):String = {
  val major = spark.conf.get("com.databricks.training.dbr.major-version")
  val minor = spark.conf.get("com.databricks.training.dbr.minor-version")

  if ((major.toInt < expMajor) || (major.toInt == expMajor && minor.toInt < expMinor)) {
    throw new Exception(s"This notebook must be ran on Databricks Runtime (DBR) version $expMajor.$expMinor or better, found $major.$minor.")
  }
  return s"$major.$minor"
}

//*******************************************
// CHECK FOR REQUIRED VERIONS OF SPARK & DBR
//*******************************************

assertDbrVersion(4, 0)
assertSparkVersion(2, 3)

displayHTML("Initialized classroom variables & functions...")

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC if course_name == "Spark ILT":
// MAGIC   # Using course_name is a temporary hack that will 
// MAGIC   # not scale to all the different ILT releases
// MAGIC   spark.conf.set("com.databricks.training.valid", "true")
// MAGIC
// MAGIC elif spark.conf.get("com.databricks.training.job") == "true":
// MAGIC   spark.conf.set("com.databricks.training.valid", "true")
// MAGIC   
// MAGIC else:
// MAGIC   import base64
// MAGIC   s = 'CmltcG9ydCByZXF1ZXN0cwpzcGFyay5jb25mLnNldCgiY29tLmRhdGFicmlja3MudHJhaW5pbmcudmFsaWQiLCAiZmFsc2UiKQp1c2VybmFtZSA9IHNwYXJrLmNvbmYuZ2V0KCJjb20uZGF0YWJyaWNrcy50cmFpbmluZy51c2VybmFtZSIpCnVybCA9ICdodHRwOi8vc2VydmVyMS5kYXRhYnJpY2tzLnRyYWluaW5nOjkwOTkvYXBpLzEvdmFsaWRhdGUnCnZhbGlkID0gVHJ1ZQp0cnk6CiAgciA9IHJlcXVlc3RzLnBvc3QodXJsLCBqc29uPXsidXNlckVtYWlsIjogdXNlcm5hbWUsICJjb3Vyc2VOYW1lIjogY291cnNlX25hbWV9LCB0aW1lb3V0PTE1KQogIHZhbGlkID0gci5zdGF0dXNfY29kZSA9PSAyMDAKZXhjZXB0IHJlcXVlc3RzLmV4Y2VwdGlvbnMuVGltZW91dCBhcyBlOgogIHBhc3MKCmlmIHZhbGlkOgogIHNwYXJrLmNvbmYuc2V0KCJjb20uZGF0YWJyaWNrcy50cmFpbmluZy52YWxpZCIsICJ0cnVlIikKZWxzZToKICBwcmludCgiKiIgKiA4MCkKICBwcmludCgiVXNlciB7fSBpcyBub3QgYXV0aG9yaXplZCB0byBydW4gdGhpcyBjb3Vyc2UuIi5mb3JtYXQodXNlcm5hbWUpKQogIHByaW50KCJQZXJoYXBzIHlvdSBwdXJjaGFzZWQgdGhlIGNvdXJzZSB3aXRoIGFub3RoZXIgZW1haWwgYWRkcmVzcy4iKQogIHByaW50KCIqIiAqIDgwKQogIHJhaXNlIEV4Y2VwdGlvbigiVW5hdXRob3JpemVkLiIpCg=='
// MAGIC   c = base64.b64decode(s)
// MAGIC   eval(compile(c, '<string>', 'exec'))
// MAGIC
// MAGIC None

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC #**********************************
// MAGIC # VARIOUS UTILITY FUNCTIONS
// MAGIC #**********************************
// MAGIC
// MAGIC def assertSparkVersion(expMajor, expMinor):
// MAGIC   major = spark.conf.get("com.databricks.training.spark.major-version")
// MAGIC   minor = spark.conf.get("com.databricks.training.spark.minor-version")
// MAGIC
// MAGIC   if (int(major) < expMajor) or (int(major) == expMajor and int(minor) < expMinor):
// MAGIC     msg = "This notebook must run on Spark version {}.{} or better, found.".format(expMajor, expMinor, major, minor)
// MAGIC     raise Exception(msg)
// MAGIC     
// MAGIC   return major+"."+minor
// MAGIC
// MAGIC def assertDbrVersion(expMajor, expMinor):
// MAGIC   major = spark.conf.get("com.databricks.training.dbr.major-version")
// MAGIC   minor = spark.conf.get("com.databricks.training.dbr.minor-version")
// MAGIC
// MAGIC   if (int(major) < expMajor) or (int(major) == expMajor and int(minor) < expMinor):
// MAGIC     msg = "This notebook must run on Databricks Runtime (DBR) version {}.{} or better, found.".format(expMajor, expMinor, major, minor)
// MAGIC     raise Exception(msg)
// MAGIC     
// MAGIC   return major+"."+minor
// MAGIC
// MAGIC #**********************************
// MAGIC # INIT VARIOUS VARIABLES
// MAGIC #**********************************
// MAGIC
// MAGIC username = spark.conf.get("com.databricks.training.username")
// MAGIC userhome = spark.conf.get("com.databricks.training.userhome")
// MAGIC
// MAGIC import sys
// MAGIC pythonVersion = spark.conf.set("com.databricks.training.python-version", sys.version[0:sys.version.index(" ")])
// MAGIC
// MAGIC None # suppress output

// COMMAND ----------


//**********************************
// CREATE THE MOUNTS
//**********************************

def getAwsRegion():String = {
  try {
    import scala.io.Source
    import scala.util.parsing.json._

    val jsonString = Source.fromURL("http://169.254.169.254/latest/dynamic/instance-identity/document").mkString // reports ec2 info
    val map = JSON.parseFull(jsonString).getOrElse(null).asInstanceOf[Map[Any,Any]]
    map.getOrElse("region", null).asInstanceOf[String]

  } catch {
    // We will use this later to know if we are Amazon vs Azure
    case _: java.io.FileNotFoundException => null
  }
}

def getAzureRegion():String = {
  import com.databricks.backend.common.util.Project
  import com.databricks.conf.trusted.ProjectConf
  import com.databricks.backend.daemon.driver.DriverConf

  new DriverConf(ProjectConf.loadLocalConfig(Project.Driver)).region
}

val awsAccessKey = "AKIAJBRYNXGHORDHZB4A"
val awsSecretKey = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF"
val awsAuth = s"${awsAccessKey}:${awsSecretKey}"

def getAwsMapping(region:String):(String,Map[String,String]) = {

  val MAPPINGS = Map(
    "ap-northeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-1/common", Map[String,String]()),
    "ap-northeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-2/common", Map[String,String]()),
    "ap-south-1"     -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-south-1/common", Map[String,String]()),
    "ap-southeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-1/common", Map[String,String]()),
    "ap-southeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-2/common", Map[String,String]()),
    "ca-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ca-central-1/common", Map[String,String]()),
    "eu-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
    "eu-west-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-1/common", Map[String,String]()),
    "eu-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-2/common", Map[String,String]()),
    "eu-west-3"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-3/common", Map[String,String]()),
    "sa-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-sa-east-1/common", Map[String,String]()),
    "us-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-1/common", Map[String,String]()),
    "us-east-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-2/common", Map[String,String]()),
    "us-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]()),
    "_default"       -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]())
  )

  MAPPINGS.getOrElse(region, MAPPINGS("_default"))
}

def getAzureMapping(region:String):(String,Map[String,String]) = {

  // Databricks only wants the query-string portion of the SAS URL (i.e., the part from the "?" onward, including
  // the "?"). But it's easier to copy-and-paste the full URL from the Azure Portal. So, that's what we do.
  // The logic, below, converts these URLs to just the query-string parts.

  val EastAsiaAcct = "dbtraineastasia"
  val EastAsiaSas = "https://dbtraineastasia.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T05:02:54Z&st=2018-04-18T21:02:54Z&spr=https&sig=gfu42Oi3QqKjDUMOBGbayQ9WUsxEQ4EdHpI%2BRBCWPig%3D"

  val EastUSAcct = "dbtraineastus"
  val EastUSSas = "https://dbtraineastus.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:29:20Z&st=2018-04-18T22:29:20Z&spr=https&sig=drx0LE2W%2BrUTvblQtVU4SiRlWk1WbLUJI6nDvFWIfHA%3D"

  val EastUS2Acct = "dbtraineastus2"
  val EastUS2Sas = "https://dbtraineastus2.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:32:30Z&st=2018-04-18T22:32:30Z&spr=https&sig=BB%2FQzc0XHAH%2FarDQhKcpu49feb7llv3ZjnfViuI9IWo%3D"

  val NorthCentralUSAcct = "dbtrainnorthcentralus"
  val NorthCentralUSSas = "https://dbtrainnorthcentralus.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:35:29Z&st=2018-04-18T22:35:29Z&spr=https&sig=htJIax%2B%2FAYQINjERk0z%2B0jR%2BBF8MpPK3BdBFa8%2FLAUU%3D"

  val NorthEuropeAcct = "dbtrainnortheurope"
  val NorthEuropeSas = "https://dbtrainnortheurope.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:37:15Z&st=2018-04-18T22:37:15Z&spr=https&sig=upIQ%2FoMa4v2aRB8AAB3gBY%2BvybhLwQGS2%2Bsyq0Z3mZw%3D"

  val SouthCentralUSAcct = "dbtrainsouthcentralus"
  val SouthCentralUSSas = "https://dbtrainsouthcentralus.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:38:27Z&st=2018-04-18T22:38:27Z&spr=https&sig=OL2amlrWn4X9ABAoWyvaL%2FVIf83GVrAnRL6gpauxqzA%3D"

  val SouthEastAsiaAcct = "dbtrainsoutheastasia"
  val SouthEastAsiaSas = "https://dbtrainsoutheastasia.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:39:59Z&st=2018-04-18T22:39:59Z&spr=https&sig=9LFC3cZXe4qWMGABmu%2BuMEAsSKGB%2BfxO0kZTxDAhvF8%3D"

  val WestCentralUSAcct = "dbtrainwestcentralus"
  val WestCentralUSSas = "https://dbtrainwestcentralus.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:33:55Z&st=2018-04-18T22:33:55Z&spr=https&sig=5tZWw9V4pYuFu7sjTmEcFujAJlcVg3hBl1jgWcSB3Qg%3D"

  val WestEuropeAcct = "dbtrainwesteurope"
  val WestEuropeSas = "https://dbtrainwesteurope.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T13:30:09Z&st=2018-04-19T05:30:09Z&spr=https&sig=VRX%2Fp6pC3jJsrPoR7Lz8kvFAUhJC1%2Fzye%2FYvvgFbD5E%3D"

  val WestUSAcct = "dbtrainwestus"
  val WestUSSas = "https://dbtrainwestus.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T13:31:40Z&st=2018-04-19T05:31:40Z&spr=https&sig=GRH1J%2FgUiptQHYXLX5JmlICMCOvqqshvKSN4ygqFc3Q%3D"

  val WestUS2Acct = "dbtrainwestus2"
  val WestUS2Sas = "https://dbtrainwestus2.blob.core.windows.net/?sv=2018-03-28&ss=bfqt&srt=sco&sp=rl&se=3000-01-01T05:59:59Z&st=2019-01-18T01:16:38Z&spr=https&sig=vY4rsDZhyQ9eq5NblfqVDiPIOmTEQquRIEHB4MH4BTA%3D"

  // For each Azure region we support, associate an appropriate Storage Account and SAS token
  // to use to mount /mnt/training (so that we use the version that's closest to the
  // region containing the Databricks instance.)
  // FUTURE RELEASE: New regions are rolled back for this release.  Test new regions before deployment
  
  var MAPPINGS = Map(
    "eastasia"         -> (EastAsiaAcct, EastAsiaSas),
    "eastus"           -> (EastUSAcct, EastUSSas),
    "eastus2"          -> (EastUS2Acct, EastUS2Sas),
    "northcentralus"   -> (NorthCentralUSAcct, NorthCentralUSSas),
    "northeurope"      -> (NorthEuropeAcct, NorthEuropeSas),
    "southcentralus"   -> (SouthCentralUSAcct, SouthCentralUSSas),
    "southeastasia"    -> (SouthEastAsiaAcct, SouthEastAsiaSas),
    "westcentralus"    -> (WestCentralUSAcct, WestCentralUSSas),
    "westeurope"       -> (WestEuropeAcct, WestEuropeSas),
    "westus"           -> (WestUSAcct, WestUSSas),
    "westus2"          -> (WestUS2Acct, WestUS2Sas),
    "_default"         -> (WestUS2Acct, WestUS2Sas) // Following trainers in West-US-2
  ).map { case (key, (acct, url)) => key -> (acct, url.slice(url.indexOf('?'), url.length)) }

  val (account: String, sasKey: String) = MAPPINGS.getOrElse(region, MAPPINGS("_default"))

  val blob = "training"
  val source = s"wasbs://$blob@$account.blob.core.windows.net/"
  val configMap = Map(
    s"fs.azure.sas.$blob.$account.blob.core.windows.net" -> sasKey
  )

  (source, configMap)
}

def mountFailed(msg:String): Unit = {
  println(msg)
}

def retryMount(source: String, mountPoint: String): Unit = {
  try { 
    // Mount with IAM roles instead of keys for PVC
    dbutils.fs.mount(source, mountPoint)
  } catch {
    case e: Exception => mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
  }
}

def mount(source: String, extraConfigs:Map[String,String], mountPoint: String): Unit = {
  try {
    dbutils.fs.mount(source, mountPoint, extraConfigs=extraConfigs)
  } catch {
    case ioe: java.lang.IllegalArgumentException => retryMount(source, mountPoint)
    case e: Exception => mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
  }
}

def autoMount(fix:Boolean = false, failFast:Boolean = false, mountDir:String = "/mnt/training"): Unit = {
  var awsRegion = getAwsRegion()

  val (source, extraConfigs) = if (awsRegion != null)  {
    spark.conf.set("com.databricks.training.region.name", awsRegion)
    getAwsMapping(awsRegion)

  } else {
    val azureRegion = getAzureRegion()
    spark.conf.set("com.databricks.training.region.name", azureRegion)
    getAzureMapping(azureRegion)
  }
  
  val resultMsg = mountSource(fix, failFast, mountDir, source, extraConfigs)
  displayHTML(resultMsg)
}

def mountSource(fix:Boolean, failFast:Boolean, mountDir:String, source:String, extraConfigs:Map[String,String]): String = {
  val mntSource = source.replace(awsAuth+"@", "")

  if (dbutils.fs.mounts().map(_.mountPoint).contains(mountDir)) {
    val mount = dbutils.fs.mounts().filter(_.mountPoint == mountDir).head
    if (mount.source == mntSource) {
      return s"""Datasets are already mounted to <b>$mountDir</b> from <b>$mntSource</b>"""
      
    } else if (failFast) {
      throw new IllegalStateException(s"Expected $mntSource but found ${mount.source}")
      
    } else if (fix) {
      println(s"Unmounting existing datasets ($mountDir from $mntSource)")
      dbutils.fs.unmount(mountDir)
      mountSource(fix, failFast, mountDir, source, extraConfigs)

    } else {
      return s"""<b style="color:red">Invalid Mounts!</b></br>
                      <ul>
                      <li>The training datasets you are using are from an unexpected source</li>
                      <li>Expected <b>$mntSource</b> but found <b>${mount.source}</b></li>
                      <li>Failure to address this issue may result in significant performance degradation. To address this issue:</li>
                      <ol>
                        <li>Insert a new cell after this one</li>
                        <li>In that new cell, run the command <code style="color:blue; font-weight:bold">%scala fixMounts()</code></li>
                        <li>Verify that the problem has been resolved.</li>
                      </ol>"""
    }
  } else {
    println(s"""Mounting datasets to $mountDir from $mntSource""")
    mount(source, extraConfigs, mountDir)
    return s"""Mounted datasets to <b>$mountDir</b> from <b>$mntSource<b>"""
  }
}

def fixMounts(): Unit = {
  autoMount(true)
}

autoMount(true)
