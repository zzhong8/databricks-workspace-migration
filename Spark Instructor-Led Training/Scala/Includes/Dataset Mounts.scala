// Databricks notebook source
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
    "us-west-2"      -> (s"s3a://${awsAuth}@databricks-corp-training/common", Map[String,String]()),
    "_default"       -> (s"s3a://${awsAuth}@databricks-corp-training/common", Map[String,String]())
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
  val WestUS2Sas = "https://dbtrainwestus2.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=ra&se=2023-04-19T13:32:45Z&st=2018-04-19T05:32:45Z&spr=https&sig=TJpU%2FHaVkDNiY%2B9zyyjBDt8GKadRvwnFArG2q8JXyhY%3D"

  // For each Azure region we support, associate an appropriate Storage Account and SAS token
  // to use to mount /mnt/training (so that we use the version that's closest to the
  // region containing the Databricks instance.)
  // FUTURE RELEASE: New regions are rolled back for this release.  Test new regions before deployment

  var MAPPINGS = Map(
//     "EastAsia"         -> (EastAsiaAcct, EastAsiaSas),
//     "EastUS"           -> (EastUSAcct, EastUSSas),
//     "EastUS2"          -> (EastUS2Acct, EastUS2Sas),
//     "NorthCentralUS"   -> (NorthCentralUSAcct, NorthCentralUSSas),
//     "NorthEurope"      -> (NorthEuropeAcct, NorthEuropeSas),
//     "SouthCentralUS"   -> (SouthCentralUSAcct, SouthCentralUSSas),
//     "SouthEastAsia"    -> (SouthEastAsiaAcct, SouthEastAsiaSas),
//     "WestCentralUS"    -> (WestCentralUSAcct, WestCentralUSSas),
//     "WestEurope"       -> (WestEuropeAcct, WestEuropeSas),
//     "WestUS"           -> (WestUSAcct, WestUSSas),
    "WestUS2"          -> (WestUS2Acct, WestUS2Sas),
    "_default"         -> (EastUS2Acct, EastUS2Sas)
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

def autoMount(): Unit = {

  var awsRegion = getAwsRegion()
  
  val (source, extraConfigs) = if (awsRegion != null)  {
    spark.conf.set("com.databricks.training.region.name", awsRegion)
    getAwsMapping(awsRegion)

  } else {
    val azureRegion = getAzureRegion()
    spark.conf.set("com.databricks.training.region.name", azureRegion)
    getAzureMapping(azureRegion)
  }

  val mountDir = "/mnt/training"
  val mountSource = source.replace(awsAuth+"@", "")

  if (dbutils.fs.mounts().map(_.mountPoint).contains(mountDir)) {
    val mount = dbutils.fs.mounts().filter(_.mountPoint == mountDir).head
    if (mount.source == mountSource) {
      displayHTML(s"""Datasets are already mounted to "$mountDir" ($mountSource)""")
    } else {
      throw new IllegalStateException(s"""Invalid Mounts: Expected "${mountSource}" but found "${mount.source}". Please unmount "$mountDir" and then rerun this command.""")
    }
  } else {
    println(s"""Mounting datasets to "$mountDir" ($mountSource)""")
    mount(source, extraConfigs, mountDir)
    displayHTML(s"""Mounted datasets to "$mountDir" ($mountSource)""")
  }
}

autoMount()
