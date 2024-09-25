# Databricks notebook source
# MAGIC %md # Using Dynamic Time Warping and MLflow to Predict Sales Trends
# MAGIC
# MAGIC In this notebook we will show we can use the time series comparison method of dynamic time warping to produce a distance metric between two input sales data time series.
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ## Requirements
# MAGIC Install the [`ucrdtw`](https://github.com/klon/ucrdtw) and [`dill`](https://github.com/uqfoundation/dill) PyPi libraries; instructions to install PyPi libraries can be found here: [Azure](https://docs.azuredatabricks.net/user-guide/libraries.html#pypi-libraries) | [AWS](https://docs.databricks.com/user-guide/libraries.html#pypi-libraries).
# MAGIC

# COMMAND ----------

# MAGIC %run "./Custom Flavor Definition"

# COMMAND ----------

htmlTxt="""
<h1><span style="font-family:helvetica,arial">Dynamic Time Warping Background</span></h1>
<table width=80% border="0">
  <tr>
    <td width=50% valign="top">
      <span style="font-family:helvetica,arial;line-height: 1.6">
      The objective of time series comparison methods is to produces a <b>distance</b> metric between two input time series.  
      Dynamic time warping is a seminal time series comparison technique that has been used for speech recognition and word recognition since the 1970s with sound waves as the source; an often cited paper is <a href="https://ieeexplore.ieee.org/document/1171695" target="_blank">Dynamic time warping for isolated word recognition based on ordered graph searching techniques</a>. 
      <br/><br/>
      This technique can not only be used for pattern matching but also anomaly detection (e.g. overlap time series between two disjoint time periods to understand if the shape has changed significantly, examine outliers).  For example, when looking at the red and blue lines in the graph to the right, note the traditional time series matching (i.e. Euclidean Matching) is extremely restrictive.  On the otherhand, dynamic time warping allows the two curves to match up even though the x-axis (i.e. time) are not necessarily in sync. Another way is to think of this as a robust dissimilarlity score where <b>lower number</b> means the series are more similar.
      <br/><br/>
      Two time series (the base time series and new time series) are considered <b>similar</b> when it is possible to map with function <em>f(x)</em> according to the following rules so as to match the magnitudes using an optimal (warping) path. 
      <br/><br/>
      <center>
      <img src="https://pages.databricks.com/rs/094-YMS-629/images/dtw-rules-formula.png" width="400"/>
      </center>
      </span>
    </td>
    <td width=50% align="center">
        <img src="https://upload.wikimedia.org/wikipedia/commons/6/69/Euclidean_vs_DTW.jpg" width=500 />
        <br/>
        <center>
          <span style="font-family:helvetica,arial">Source: Wiki Commons: <a href="https://commons.wikimedia.org/wiki/File:Euclidean_vs_DTW.jpg">File:Euclidean_vs_DTW.jpg</a></span>
        </center>
    </td>
  </tr>
</table>
"""
displayHTML(htmlTxt)

# COMMAND ----------

# MAGIC %md ### Speech-based Dynamic Time Warping Example
# MAGIC For a speech-based dynamic time warping example, refer to the [Dynamic Time Warping Background Notebook](https://pages.databricks.com/rs/094-YMS-629/images/dynamic-time-warping-background.html)

# COMMAND ----------

# MAGIC %md ## Distance Concept
# MAGIC
# MAGIC When working with time series comparisons, the concept of `distance` is a metric that defines the proximity of time series:
# MAGIC * The closer the distance (i.e. lower value) the more similar these two different time series.
# MAGIC * The farther the distance (i.e. higher value) the less similar these two different time series.
# MAGIC
# MAGIC For example, when looking at the `data` and `query` example below (both with the same values), the `distance` should be 0 indicating they are the very similar/same time series.

# COMMAND ----------

import numpy as np
import _ucrdtw
import matplotlib.pyplot as plt

data = np.array([2, 3, 1, 0])
query = np.array([2, 3, 1, 0])

distance = _ucrdtw.ucrdtw(data, query, 0.05, True)[1]
print("DTW distance: %s " % distance) 

# COMMAND ----------

# MAGIC %md ### Simple Linear Increase Translation with Random Noise
# MAGIC
# MAGIC In this example, we will:
# MAGIC * Create a simple linear increase as our base time series,
# MAGIC * Translate it with random noise as our new time series,
# MAGIC * And compare these two time series using dynamic time warping

# COMMAND ----------

# Configure
Fs = 100
f = 0.02
sample = 100

# Base Time Series
x1 = np.arange(sample)
y1 = np.sin(2 * np.pi * f * x1 / Fs)
x1 = np.arange(200)
y1 = np.concatenate((np.zeros(100), y1))

# New Time Series
x2 = np.arange(sample)
y2 = np.sin((2*np.pi * f * x2) / Fs) + np.random.uniform(-0.005, 0.025, 100)
x2 = np.arange(200)
y2 = np.concatenate((np.zeros(50), y2, np.zeros(50)))

# COMMAND ----------

# MAGIC %md The preceding code snippets creates our two time series while the following code (which is currently hidden) plots the two time series.

# COMMAND ----------

from matplotlib.pyplot import figure
figure(num=None, figsize=(14, 5), dpi=80, facecolor='w', edgecolor='k')

# Configure matplotlib
plt.gcf().clear()
plt.style.use('seaborn-whitegrid')


# Base Time Series Plot
ax = plt.subplot(1, 2, 1)
ax.plot(x1, y1, color='#67A0DA')

# Change Title
ax.set_title("Base Time Series")
ax.title.set_fontsize(10)

# Set tick labels
ax.set(xlabel='sample(n)', ylabel='voltage(V)')
ax.tick_params(axis='both', labelsize=10)

# New Time Series Plot
ax = plt.subplot(1, 2, 2)
ax.plot(x2, y2, color='#DAA067')

# Change Title
ax.set_title("New Time Series")
ax.title.set_fontsize(10)

# Set tick labels
ax.set(xlabel='sample(n)', ylabel='voltage(V)')
ax.tick_params(axis='both', labelsize=10)

# Display created figure
fig=plt.show()
display(fig)

# COMMAND ----------

# MAGIC %md To calculate the distance between the two series, we will use the same dynamic time warping library

# COMMAND ----------

distance = _ucrdtw.ucrdtw(y1, y2, 0.99, True)[1]
print("DTW distance is %s" % distance)

# COMMAND ----------

# MAGIC %md ##### As you can see the distance between these two time series is relative small

# COMMAND ----------

# MAGIC %md ## Applying Dynamic Time Warping to Sales Data
# MAGIC We will use the [weekly sales transaction data set](https://archive.ics.uci.edu/ml/datasets/Sales_Transactions_Dataset_Weekly) found in the [UCI Dataset Repository](https://archive.ics.uci.edu/ml/index.php) to perform our sales-based time series analysis.
# MAGIC
# MAGIC Source Attribution: James Tan, jamestansc '@' suss.edu.sg, Singapore University of Social Sciences

# COMMAND ----------

# Configure
sales_filepath = "/FileStore/tables/Sales_Transactions_Dataset_Weekly.csv"
sales_dbfspath = "/dbfs" + sales_filepath

# COMMAND ----------

import pandas as pd

# Use Pandas to read this data
sales_pdf = pd.read_csv(sales_dbfspath, header='infer')

# Review data
display(spark.createDataFrame(sales_pdf))

# COMMAND ----------

# Calculate how many products
print("There are %s products in this dataset" % len(sales_pdf))

# COMMAND ----------

# MAGIC %md ##### Notes: Data is organized by Product (rows) by 52 weeks (column) representing units sold per week.

# COMMAND ----------

# Define optimal weekly product sales 
optimal_pattern=[12.5, 13, 13.5, 14, 17, 10, 15, 16, 17, 16, 19, 14, 14, 15, 11, 20, 18, 17, 19, 21, 19, 15, 17, 17, 16, 15, 19, 19, 20, 21,19, 18, 17, 16, 20, 19, 19, 18, 17, 18, 26, 25, 17, 23, 21, 19, 28, 18, 19, 16, 21, 23]

# Define x and y-axis (optimal time series)
xo = np.arange(52)
yo = optimal_pattern

# COMMAND ----------

# Clear Plot
plt.gcf().clear()

# Configure labels
plt.xlabel('Week')
plt.ylabel('Sales')
plt.plot(xo, yo, 'o-', markersize=4, color='#67A0DA')
plt.title("Optimal Weekly Product Sales")
plt.xlim(0,52)
plt.ylim(-1, 35)

# Show plot
fig=plt.show()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calculate distance to optimal time series by product code
# MAGIC The next set of functions will compute the distance between the sales time series and the optimal time trend broken down by product code.

# COMMAND ----------

# Calculate distance via dynamic time warping between product code and optimal time series
import numpy as np

def get_keyed_values(s):
  return(s[0], s[1:])

def compute_distance(row):
  return(row[0], _ucrdtw.ucrdtw(list(row[1][0:52]), list(optimal_pattern), 0.05, True)[1])

ts_values = pd.DataFrame(np.apply_along_axis(get_keyed_values, 1, sales_pdf.values))
distances = pd.DataFrame(np.apply_along_axis(compute_distance, 1, ts_values.values))
distances.columns = ['pcode', 'dtw_dist']

# COMMAND ----------

# Create Spark DataFrame to more easily view the data
from pyspark.sql.functions import desc
spark_distances = spark.createDataFrame(distances) 
spark_distances.createOrReplaceTempView("distances")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 product codes closest to the optimal sales trend 
# MAGIC select pcode, cast(dtw_dist as float) as dtw_dist from distances order by cast(dtw_dist as float) limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 product codes farthest from the optimal sales trend 
# MAGIC select pcode, cast(dtw_dist as float) as dtw_dist from distances order by cast(dtw_dist as float) desc limit 10

# COMMAND ----------

# Example: P675 has the closest distance to the optimal sales trend
y_p675 = sales_pdf.query('Product_Code == "P675"').iloc[:,1:53].values.flatten()

# Example: P716 has the fartest distance to the optimal sales trend
y_p716 = sales_pdf.query('Product_Code == "P716"').iloc[:,1:53].values.flatten()

# COMMAND ----------

# Configure matplotlib
plt.gcf().clear()
plt.style.use('seaborn-whitegrid')

plt.plot(xo, yo, 'o-', markersize=4, color='#67A0DA', label='Optimal Sales Trend')
plt.plot(xo, y_p675, '^-', markersize=6, color='#DAA067', label='P675')
plt.plot(xo, y_p716, '*-', markersize=6, color='#3FBF7F', label='P716')
plt.legend(loc=2, prop={'size':10})
plt.title("Comparing Optimal Sales Trends with P675 and P716")
plt.xlim(0,52)
plt.ylim(-3, 35)

fig=plt.show()
display(fig)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Visualize Distribution of DTW Distances
# MAGIC
# MAGIC Calculate the DTW distances for each of the pairwise product sales.

# COMMAND ----------

# Generate data on commute times.
size, scale = 1000, 10
dtw_values = pd.Series(distances['dtw_dist']).astype(float)

# Reset Histogram Plot
plt.gcf().clear()
dtw_values.plot.hist(grid=True, bins=20, rwidth=0.9,color='#607c8e')

# Pairwise Product Sales Comparison
plt.title('DTW Distances For Each Pairwise Product Sales Comparison')
plt.xlabel('Distances')
plt.ylabel('Counts')

# Configure Plot
plt.grid(axis='y', alpha=0.75)
fig=plt.show()
display(fig)

# COMMAND ----------

# Calculate median DTW
print("Median DTW: %s" % distances.loc[:,"dtw_dist"].median())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Example: Undesired Sales Trend - Big Sales Numbers but Inconsistent
# MAGIC
# MAGIC * DTW median is 7.89
# MAGIC * Product 83 has a *DTW distance* of 8.95

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from distances where pcode = 'P83'

# COMMAND ----------

# Review P83 weekly sales  
y_p83 = sales_pdf[sales_pdf['Product_Code'] == 'P83'].values[0][1:53]

# COMMAND ----------

# Configure matplotlib
plt.gcf().clear()
plt.style.use('seaborn-whitegrid')

plt.plot(xo, yo, 'o-', markersize=4, color='#67A0DA', label='Optimal Sales Trend')
plt.plot(xo, y_p83, '^-', markersize=6, color='#DAA067', label='P675')
#plt.plot(xo, y_p716, '*-', markersize=6, color='#3FBF7F', label='P716')
plt.legend(loc=2, prop={'size':10})
plt.title("Comparing Optimal Sales Trends with Weekly Sales for P83")
plt.xlim(0,52)
plt.ylim(-3, 65)

fig=plt.show()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Example: Desired Sales Trend: Product with High Degree of Similarity 
# MAGIC * DTW median is 7.89
# MAGIC * Product 202 has a *DTW distance* of 6.86

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from distances where pcode = 'P202'

# COMMAND ----------

# Review P202 weekly sales  
y_p202 = sales_pdf[sales_pdf['Product_Code'] == 'P202'].values[0][1:53]

# COMMAND ----------

# Configure matplotlib
plt.gcf().clear()
plt.style.use('seaborn-whitegrid')

plt.plot(xo, yo, 'o-', markersize=4, color='#67A0DA', label='Optimal Sales Trend')
plt.plot(xo, y_p202, '*-', markersize=6, color='#3FBF7F', label='P716')
plt.legend(loc=2, prop={'size':10})
plt.title("Comparing Optimal Sales Trends with Weekly Sales for P2")
plt.xlim(0,52)
plt.ylim(-3, 65)

fig=plt.show()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Now Vary My Stretch Factor and Use MLflow to Track Best and Worst Trending Products Along With Artifacts

# COMMAND ----------

# Configure MLflow experiment ID
#   This code is using the Databricks integrated version of MLflow
import mlflow
mlflow.set_experiment("/Users/ricardo.portilla@databricks.com/ricardo.portilla@databricks.com/Unsupervised Learning/MLflowSalesTrendingExp")

# COMMAND ----------

# Run multiple DTW runs 
def run_DTW(ts_stretch_factor, verbose=False, log_outliers=False, log_zscore=False):
  import mlflow
  import numpy as np
  from scipy import stats

  def compute_distance(row):
     return(row[0], _ucrdtw.ucrdtw(list(row[1][0:52]), list(optimal_pattern), ts_stretch_factor, True)[1])
  
  # MLflow Tracking
  with mlflow.start_run() as run:
    
    # Log Model using Custom Flavor
    dtw_model = {'stretch_factor' : float(ts_stretch_factor), 'pattern' : optimal_pattern}        
    mlflow_custom_flavor.log_model(dtw_model, artifact_path="model")
    
    # Log parameters
    mlflow.log_param("stretch_factor", ts_stretch_factor)
    
    # Calculate distance
    ts_values = pd.DataFrame(np.apply_along_axis(get_keyed_values, 1, sales_pdf.values))
    distances = pd.DataFrame(np.apply_along_axis(compute_distance, 1, ts_values.values))
    distances.columns = ['pcode', 'dtw_dist']
    distances['dtw_dist'] = distances['dtw_dist'].astype('float')
    
    # Print if true
    if verbose:
      print('distances -> ', distances)
          
    # Configure plots
    size, scale = 1000, 10
    dtw_values = pd.Series(distances['dtw_dist']).astype(float)

    # Clear plot
    plt.gcf().clear()
    
    # Create histogram plot
    dtw_values.plot.hist(grid=True, bins=20, rwidth=0.9,color='#607c8e')
    plt.title('DTW Distances For Each Pairwise Product Sales Comparison')
    plt.xlabel('Distances')
    plt.ylabel('Counts')
    plt.grid(axis='y', alpha=0.75)
      
    # Save figure
    plt.savefig("DTW_dist_histogram.png")
       
    # look at IQR to get the lower threshold for trending performers
    lower_threshold = distances['dtw_dist'].astype('float').quantile(0.1)
    iqr_outliers = distances.loc[distances['dtw_dist'] <= lower_threshold]

    # Log iqr_outliers metric by individual product codes  
    if log_outliers:
      for index, row in iqr_outliers.iterrows():
          parameter = "Distance-" + row['pcode']
          metric = row['dtw_dist']
          mlflow.log_metric(parameter, metric)


    # look at z-score to get the lower threshold for trending performers
    z = stats.zscore(distances['dtw_dist'])
    zscore_outliers = np.where(z <= -2)
    dist_zscore_outliers = distances.loc[zscore_outliers[0]]
    
    # Log dist_zscore_outliers metric by individual product codes  
    if log_zscore: 
      for index, row in dist_zscore_outliers.iterrows():
          parameter = "Distance-" + row['pcode']
          metric = row['dtw_dist']
          mlflow.log_metric(parameter, metric)
    
    # Print if true
    if verbose:
      print('sscore outliers -> ', dist_zscore_outliers)
    
    # Outliers to CSV
    iqr_outliers.to_csv(path_or_buf = 'iqr_outliers_' + str(ts_stretch_factor) + '.csv', index = False)
    dist_zscore_outliers.to_csv(path_or_buf = 'zscore_outliers_' + str(ts_stretch_factor) + '.csv', index = False)

    # Log files to MLflow
    mlflow.log_artifact('iqr_outliers_' + str(ts_stretch_factor) + '.csv')
    mlflow.log_artifact('zscore_outliers_' + str(ts_stretch_factor) + '.csv')
    mlflow.log_artifact('DTW_dist_histogram.png')
    
  return run.info

# COMMAND ----------

# Trying various Stretch Factor values and logging this to MLflow
#  Parameter 2: Print out distances, outliers, zscore to console
#  Parameter 3: Log outlier metrics to MLflow (takes a little longer there are 811 metrics)
#  Parameter 4: Log zscore metrics to MLflow
run_DTW(0.0, False, False, True)
run_DTW(0.25, False, False, True)
run_DTW(0.05, False, False, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Trending Artifacts in MLflow Hosted Tracking Server
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/dtw-mlflow.gif)

# COMMAND ----------


