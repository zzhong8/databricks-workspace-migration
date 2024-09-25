# Databricks notebook source
# MAGIC %md
# MAGIC # Intro to data science in Databricks notebooks
# MAGIC
# MAGIC Prefer another notebooking environment? 
# MAGIC * [Databricks Connect for any IDE](https://docs.databricks.com/dev-tools/databricks-connect.html)
# MAGIC * [Jupyter Labs (coming soon!)](https://databricks.com/blog/2019/12/03/jupyterlab-databricks-integration-bridge-local-and-remote-workflows.html)
# MAGIC * [Hosted R Studio](https://docs.databricks.com/spark/latest/sparkr/rstudio.html)

# COMMAND ----------

# MAGIC %md ## Use widgets in a notebook
# MAGIC
# MAGIC Databrick utilites (e.g. `dbutils`) provides functionality for many common tasks within Databricks notebooks: 
# MAGIC https://docs.databricks.com/dev-tools/databricks-utils.html
# MAGIC
# MAGIC One useful feature is "Widgets" that allow you to dynamically program within your notebooks: https://docs.databricks.com/notebooks/widgets.html

# COMMAND ----------

#Uncomment this to remove all widgets
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("dropdown_widget", "1", [str(x) for x in range(1, 4)])

# COMMAND ----------

print("The current value of the dropdown_widget is:", dbutils.widgets.get("dropdown_widget"))

# COMMAND ----------

dbutils.widgets.text("text_widget","Hello World!")

# COMMAND ----------

# %sql
# SELECT 
#   COUNT(c1),
#   getArgument("text_widget")
# FROM (VALUES (1)) t1(c1)

# COMMAND ----------

# MAGIC %md ## Magic Commands with "%"
# MAGIC Switch between Python, R, SQL, and Scala within the same notebook

# COMMAND ----------

# MAGIC %scala
# MAGIC val x = 1
# MAGIC x+x

# COMMAND ----------

# MAGIC %md
# MAGIC Or interact programmatically with the file system:

# COMMAND ----------

# MAGIC %fs ls
# MAGIC /databricks-datasets/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Management
# MAGIC In Machine Learning Runtime, you can use Conda for environment management

# COMMAND ----------

# MAGIC %conda info --envs

# COMMAND ----------

# MAGIC %md 
# MAGIC "Notebook-scoped libraries" can be used to install libraries only for a particular notebook in Databricks Runtime 
# MAGIC * Notebook-scoped Libraries [Blog](https://databricks.com/blog/2019/01/08/introducing-databricks-library-utilities-for-notebooks.html) | [Databricks Utilities]()
# MAGIC * For Machine Learning Runtime, use Conda or Pip for environment management: [Notebook Scoped Python Libraries](https://docs.databricks.com/notebooks/notebooks-python-libraries.html)

# COMMAND ----------

#%pip install pandas

# COMMAND ----------

# MAGIC %md 
# MAGIC # Data Exploration Basics

# COMMAND ----------

import pandas as pd
df = pd.read_csv('/dbfs/databricks-datasets/bikeSharing/data-001/day.csv')

# COMMAND ----------

#In-line tabular data with "display()" command
display(df)

# COMMAND ----------

#In-line charts
display(df)

# COMMAND ----------

# Use your favorite charting library
import numpy as np
import matplotlib.pyplot as plt

points, zorder1, zorder2 = 500, 10, 5
x = np.linspace(0, 1, points)
y = np.sin(4 * np.pi * x) * np.exp(-5 * x)

fig, ax = plt.subplots()

ax.fill(x, y, zorder=zorder1)
ax.grid(True, zorder=zorder2)
plt.show()
display() # Databricks display
plt.close() # Ensure you close it

# COMMAND ----------

# MAGIC %md
# MAGIC # Revision History Tracking 
# MAGIC From within a notebook, click the right-most Revision History button to see all past versions of the notebook. 
# MAGIC
# MAGIC Click Restore if you want to revert the notebook to a previous version

# COMMAND ----------

# MAGIC %md
# MAGIC # Collaboration
# MAGIC Try this:
# MAGIC 1. Hi-light code
# MAGIC 2. A small bubble should appear on the right-hand side
# MAGIC 3. Any code (Python, markdown, etc.) can be commented this way

# COMMAND ----------

1+1

# COMMAND ----------

# MAGIC %md 
# MAGIC # Quickly Share your Findings
# MAGIC After your exploratory data science work, share your findings via: 
# MAGIC 1. Dashboard: In the top utility bar, click the drop-down to create a new dashboard. Or click the small chart icon at the top-right of each cell
# MAGIC 2. Export a notebook to HTML to be viewed in any browser. 

# COMMAND ----------

display(df)
