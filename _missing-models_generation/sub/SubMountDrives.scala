// Databricks notebook source
// MAGIC %md Mount the "processed" storage container

// COMMAND ----------


val mount = dbutils.secrets.get(scope = "blob", key = "dbr-processed-mountpoint")
if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(mount)) {
  val src = dbutils.secrets.get(scope = "blob", key = "dbr-processed-source")

  val key = dbutils.secrets.get(scope = "blob", key = "dbr-storage-extraconfigs-key") 
  val storageAccountKey = dbutils.secrets.get(scope = "blob", key = "dbr-storage-extraconfigs-value") 
  dbutils.fs.mount(
    source = src,
    mountPoint = mount,
    extraConfigs = Map(key -> storageAccountKey)
  )
}

// COMMAND ----------

display(dbutils.fs.mounts)

// COMMAND ----------

// MAGIC %md Mount the "artifacts" storage container

// COMMAND ----------

val mount = dbutils.secrets.get(scope = "blob", key = "dbr-artifacts-mountpoint")
if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(mount)) {
  val src = dbutils.secrets.get(scope = "blob", key = "dbr-artifacts-source")

  val key = dbutils.secrets.get(scope = "blob", key = "dbr-storage-extraconfigs-key") 
  val storageAccountKey = dbutils.secrets.get(scope = "blob", key = "dbr-storage-extraconfigs-value") 
  dbutils.fs.mount(
    source = src,
    mountPoint = mount,
    extraConfigs = Map(key -> storageAccountKey)
  )
}
