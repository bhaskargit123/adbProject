# Databricks notebook source
dbutils.fs.mkdirs("/mnt/gold")

# COMMAND ----------

storageAccountName = "bhaskaradlsproject"
storageAccountAccessKey = "cKLIU8xY8XqCLPDMFAsnaCc/BDB4qyoaep9H4inWdbQ3mFRA4Bz2N9Vo93rW7PW4kkTnC1Dy6PmW+AStQkzo3g=="
storageBlobContainerName = "gold"

# COMMAND ----------

dbutils.fs.mount(
	source = "wasbs://{}@{}.blob.core.windows.net".format(storageBlobContainerName, storageAccountName),
  mount_point = "/mnt/gold",
  extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net' : storageAccountAccessKey}
)
