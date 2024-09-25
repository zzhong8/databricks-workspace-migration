# Databricks notebook source
# MAGIC %md
# MAGIC ## Holidays
# MAGIC Build a dataframe with all of the holidays for the specified country and specify their lead/lag window sizes

# COMMAND ----------

from pyspark.sql import functions as pyf
from pyspark.sql.types import ShortType

from datetime import date, datetime                              # Required for date manipulations
from dateutil import easter                                      # Direct function to get Easter date
from dateutil.relativedelta import relativedelta                 # Used to construct conditional date holidays
from dateutil.relativedelta import MO, TU, WE, TH, FR, SA, SU    # Days of week for relative date logic

import holidays                                                  # Library used to create holidays

from acosta.alerting.helpers.features import get_day_of_month_column_names, get_days_of_month, get_day_of_month_column_name, get_days_of_week, get_day_of_week_column_name

# COMMAND ----------

# Process Parameters
years = range(2015, 2026)
defaultLeadLagWindowSize = 7
countryCodes = [ 'US', 'CA', 'UK' ]

# COMMAND ----------

# Expand the specified range of years by one year in each direction.
# This will let us look up holidays outside of the specified range.
# We'll trim it back down to spec at the end of all of this
years = range(min(years) - 1, max(years) + 2)

# COMMAND ----------

# Make a function that will create holiday calendar for specified countries
# There's a bug in the "holidays" package that causes an error when using the "CA" country code... so work around that
def getHolidaysForCountry(countryCode):
    if (countryCode != "CA"):
        output = holidays.CountryHoliday(countryCode)
    else:
        output = holidays.CA()

    output.observed = False

    # Loop through all years to hydrate the collection of holidays
    for y in years:
        date(y, 1, 1) in output

    return output

# COMMAND ----------

# Create a Spark dataframe with holidays
row_list = []

for countryCode in countryCodes:
    # Get standard holidays from the "holidays" package
    x = getHolidaysForCountry(countryCode)
    for holiday in x:
        name = x.get(holiday)
        row_list.append((countryCode, holiday, name))

    # Add additional unofficial holidays that affect retail behavior
    for y in years:
        row_list.append((countryCode, date(y, 2, 14), "Valentine's Day"))
        row_list.append((countryCode, date(y, 10, 31), "Halloween"))
        
        # Easter logic is very complex; use the "easter" library to determine the date of Easter Sunday
        row_list.append((countryCode, easter.easter(y), "Easter"))            

        if countryCode == "US":
            # Super Bowl is currently played on the first Sunday in February. This could change in the future
            row_list.append((countryCode, date(y, 2, 1) + relativedelta(weekday=SU(+1)), "Super Bowl"))   

# Create a Spark dataframe with our data
holidays_df = spark.createDataFrame(row_list, ["country", "date", "name"])
holidays_df = holidays_df.withColumn("year", pyf.year("date"))

display(holidays_df.groupby('name').agg(pyf.count("*"), pyf.min("date"), pyf.max("date"), pyf.countDistinct("country")))

# COMMAND ----------

# Remove holidays that don't drive consumption or are covered by other holidays
holidays_df = holidays_df[holidays_df.name != "Martin Luther King, Jr. Day"]
holidays_df = holidays_df[holidays_df.name != "Washington's Birthday"]
holidays_df = holidays_df[holidays_df.name != "Columbus Day"]
holidays_df = holidays_df[holidays_df.name != "Veterans Day"]
holidays_df = holidays_df[holidays_df.name != "Family Day"]
holidays_df = holidays_df[holidays_df.name != "Good Friday"]
holidays_df = holidays_df[holidays_df.name != "Boxing Day"]
holidays_df = holidays_df[holidays_df.name != "New Year Holiday [Scotland]"]
holidays_df = holidays_df[holidays_df.name != "St. Andrew's Day [Scotland]"]
holidays_df = holidays_df[holidays_df.name != "Summer Bank Holiday [Scotland]"]
holidays_df = holidays_df[holidays_df.name != "Battle of the Boyne [Northern Ireland]"]
holidays_df = holidays_df[holidays_df.name != "St. Patrick's Day [Northern Ireland]"]
holidays_df = holidays_df[~ holidays_df.name.rlike("Easter Monday.*")]

# Identify holidays that only occur once and remove them
# (The UK has some one-off holidays for royal events)
names = holidays_df.groupby('name').count().filter("count > 1").select("name").rdd.flatMap(lambda x: x).collect()
holidays_df = holidays_df.filter(holidays_df.name.isin(names))

# Some holidays in the UK have a region descriptor in the name. This is contained in square brackets.  We want to cut this off.
# Example: Late Summer Bank Holiday [England, Wales, Northern Ireland]
# Use a regular expression to remove everything after the first "[" in a holiday name.
holidays_df = holidays_df.select(holidays_df.country, holidays_df.date, pyf.rtrim(pyf.regexp_extract('name', r'[^\[]*', 0)).alias('name'), holidays_df.year)

# Combine US "Labor Day" and CA "Labour Day" into one holiday since they follow the same schedule
holidays_df = holidays_df.select(holidays_df.country, holidays_df.date, pyf.rtrim(pyf.regexp_replace('name', 'Labour', 'Labor')).alias('name'), holidays_df.year)

holidays_df.groupby('name').count().show()

# COMMAND ----------

# Get the holidays that have the same name but fall on different days in different countries
# (e.g. US and Canada both have "Thanksgiving" but it's on different days)
names = holidays_df.groupby("name", "year")\
        .agg(pyf.countDistinct("date").alias("date_count"))\
        .filter("date_count > 1")\
        .select("name")\
        .distinct()\
        .rdd\
        .flatMap(lambda x: x)\
        .collect()

# Pull out the holidays that need to be renamed into their own dataframe
renames = holidays_df.filter(holidays_df.name.isin(names))
holidays_df = holidays_df.filter(~holidays_df.name.isin(names))

# Add the country code to the prefix of the holidays that need to be renames
renames = renames.select(renames.country, renames.date, pyf.concat_ws(" ", renames.country, renames.name).alias("name"), renames.year)

# Add the renamed holidays back in
holidays_df = holidays_df.union(renames)

# COMMAND ----------

# We are now going to specify what the lead/lag window size should be for each holiday
# We'll start by using the default size for all holidays and then re-size based on exceptions
hw = holidays_df.groupBy("name").agg(pyf.max("country").alias("country"), pyf.max("date").alias("date")).orderBy(pyf.desc("country"), "date")
hw = hw.select("name")

# New Years lead collides with Christmas, so cut it short
# Extend the lead window for major retail holidays that have really long shopping seasons
# Some holidays have more effect on retail behavior. Increase their lead windows... but less than the major holidays
longSeasonNames  = [ "Halloween", "US Thanksgiving", "Christmas Day", "CA Thanksgiving" ]
midSeasonNames = [ "Super Bowl", "Valentine's Day", "Easter" ]

hw = hw.withColumn("leadWindowSize",
                   pyf.when(hw.name == "New Year's Day",   6)\
                   .when(hw.name.isin(longSeasonNames),   21)\
                   .when(hw.name.isin(midSeasonNames),    14)\
                   .otherwise(defaultLeadLagWindowSize))

# US Thanksgiving marks the start of the Christmas shopping season so give it a longer lag
# Christmas lag collides with New Year's so cut it short
hw = hw.withColumn("lagWindowSize",
                   pyf.when(hw.name == "US Thanksgiving",   14)\
                   .when(hw.name == "Christmas Day",   0)\
                   .otherwise(defaultLeadLagWindowSize))

hw.show()

# COMMAND ----------

# Convert pretty holiday names to strings that can be more readily used as labels and column names
hw = hw.withColumn("name", pyf.regexp_replace(pyf.regexp_replace(pyf.upper(hw.name), "[^A-Z ]", ""), " ", "_"))
holidays_df = holidays_df.withColumn("name", pyf.regexp_replace(pyf.regexp_replace(pyf.upper(holidays_df.name), "[^A-Z ]", ""), " ", "_"))

# COMMAND ----------

# Create a date range that defines the lead/lag window for each holiday based on the window sizes defined above
holidays_df = holidays_df.alias("hdf")\
              .join(hw.alias("hw"), holidays_df.name == hw.name)\
              .selectExpr("hdf.*", "date_add(hdf.date, hw.leadWindowSize * -1) AS lead_start", "date_add(hdf.date, hw.lagWindowSize) AS lag_end")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Days
# MAGIC Build a dataframe that has one row for each day.  Apply attributes to each day that will be used in the machine learning process.  This will be the dataframe that gets saved as our dimension table.

# COMMAND ----------

# Create a dataframe with a row for each day in the specified years, for each country

from pyspark.sql import Row

startDate = date(years[0], 1, 1)
endDate = date(years[-1], 12, 31)
daysCount = (endDate - startDate).days + 1

print("Dataframe should have {} days".format(daysCount))

days = []
for countryCode in countryCodes:
    days.extend([Row(SALES_DT = startDate + relativedelta(days = x), COUNTRY = countryCode) for x in range(0, daysCount)])

days = spark.createDataFrame(days)

# COMMAND ----------

# Add columns for day of month and day of week
days = days.selectExpr("*", "UPPER(DATE_FORMAT(SALES_DT, 'EEEE')) AS DAY_OF_WEEK", "CAST(DATE_FORMAT(SALES_DT, 'd') AS integer) AS DAY_OF_MONTH")

# COMMAND ----------

# Each day of the week will get its own column
for x in get_days_of_week():
    columnName = get_day_of_week_column_name(x)
    days = days.withColumn(columnName, pyf.when(days.DAY_OF_WEEK == x, 1).otherwise(0).cast(ShortType()))

# Each day of the month will get its own column
for x in get_days_of_month():
    columnName = get_day_of_month_column_name(x)
    days = days.withColumn(columnName, pyf.when(days.DAY_OF_MONTH == x, 1).otherwise(0).cast(ShortType()))
  
display(days)

# COMMAND ----------

# Each holiday will get a column that holds the number days relative to that holiday
holidayNames = hw.select("name").rdd.flatMap(lambda x: x).collect()

# Now we need to compute the "relative" column values.
# This is a continuous feature that indicates how many days since/until the closest occurrence of a holiday.

# Examples
# --------
# 1) SALES_DT is 12/20/2018, then "CHRISTMAS.DAY_RELATIVE" should be 5... because it's 5 days until the next occurrence of Christmas.
# 2) SALES_DT is 12/28/2018, then "CHRISTMAS.DAY_RELATIVE" should be -3... because the most recent occurrence of Christmas was three days prior


# Loop through each holiday
counter = 0
for x in holidayNames:
    counter += 1
    columnName = "HOLIDAY_{}_RELATIVE".format(x)
    print('{0: >2} Computing values for column {1: <40}  {2}'.format(counter, columnName, datetime.now()))

    # For each day, join it to all past and future occurrences of the current holiday.
    # This creates an exploded rowset (i.e. many more rows than in the original "days" dataset)
    z = days.alias("d")\
            .join(holidays_df.filter(holidays_df.name == x).alias("p"),
                  (pyf.col("p.country") == pyf.col("d.COUNTRY")) & (pyf.col("p.date") <  pyf.col("d.SALES_DT")), "outer")\
            .join(holidays_df.filter(holidays_df.name == x).alias("n"),
                  (pyf.col("n.country") == pyf.col("d.COUNTRY")) & (pyf.col("n.date") >= pyf.col("d.SALES_DT")), "outer")

    # Summarize the exploded row set for each day to find the most recent and next occurrence of each holiday
    z = z.groupBy("d.COUNTRY", "d.SALES_DT").agg(pyf.max("p.date").alias("prev"), pyf.min("n.date").alias("next"))

    # How many days since the last occurrence and how many days until the next occurrence?
    z = z.selectExpr("*", "datediff(prev, SALES_DT) AS prev_num", "datediff(next, SALES_DT) AS next_num")

    # Pick the closest occurrence and use that number as our "relative number of days to the holiday" value
    z = z.withColumn("rel", pyf.when(pyf.abs(z.prev_num) < pyf.abs(z.next_num), z.prev_num).otherwise(z.next_num))

    # Merge our computed relative value back into the "days" dataframe
    # Replace null values with -366. This will apply to dates in countries where the current holiday is not observed
    days = days.alias("d")\
        .join(z.alias("z"), (pyf.col("z.COUNTRY") == pyf.col("d.COUNTRY")) & (pyf.col("z.SALES_DT") == pyf.col("d.SALES_DT")))\
        .selectExpr("d.*", "CAST(COALESCE(z.rel, -366) AS short) AS `" + columnName + "`")

    # All of this repeated joining causes our distributed data set to get scattered.
    # This causes the time required for each iteration of the loop to double every time.
    # To fight this, we will recreate the "days" dataframe after every 5th iteration
    if counter % 5 == 0:
        print(" - CHECKPOINT: Control for growing number of partitions...")
        days = spark.createDataFrame(days.collect(), days.schema)
        rc = days.count()
  
# All done. Make sure our row count is solid
print("DONE! Final clean up of dataframe...")
days = spark.createDataFrame(days.collect(), days.schema)
days.count()

# COMMAND ----------

# For each holiday, we will add columns for every day in the lead/lag window
for i in hw.collect():
    start = i.leadWindowSize * -1
    end = i.lagWindowSize
    relativeColumn = "`HOLIDAY_{}_RELATIVE`".format(i.name)

    for j in range(start, end + 1):
        columnName = ""

        if j < 0:
            columnName = "HOLIDAY_{0}_LEAD_{1}".format(i.name, j * -1)
        elif j == 0:
            columnName = "HOLIDAY_{0}".format(i.name)
        else:
            columnName = "HOLIDAY_{0}_LAG_{1}".format(i.name, j)

        days = days.withColumn(columnName, pyf.when(days[relativeColumn] == j * -1, 1).otherwise(0).cast(ShortType()))\
                              .orderBy("COUNTRY", "SALES_DT")

# COMMAND ----------

# Remember way back at the beginning when we expanded our range of years beyond the specified range?
# Well now it's time to trim it back to where it should be.
min_year = min(years)
max_year = max(years)
filterStatement = "year(SALES_DT) > {0} AND year(SALES_DT) < {1}".format(min_year, max_year)
days = days.filter(filterStatement)
days.count()

# COMMAND ----------

# Congratulations! You've successfully built a dataframe with the date/country data that we need to support our machine learning training.
# Now we just need to save all of this data to a Hive table so we can use it later!

# If the table already exists, kill it
sqlContext.sql("DROP TABLE IF EXISTS RETAIL_FORECAST_ENGINE.COUNTRY_DATES")

# Dynamically build a "CREATE TABLE" SQL statement that will mimic the structure of our dataframe
# (Note that our Hive table will use the Databricks "Delta" format)
sqlStatement = '''
CREATE TABLE RETAIL_FORECAST_ENGINE.COUNTRY_DATES
   (
'''

# Loop through the columns in our final dataframe...
for c in days.dtypes:
    sqlStatement += "      `{0}` {1},\n".format(c[0], c[1])

sqlStatement = sqlStatement[:-2]
sqlStatement += '''
   )
   USING DELTA
   PARTITIONED BY (COUNTRY)
   COMMENT 'a date dimension with country-specific holiday features'
'''

# Run the "CREATE TABLE" statement we just created
sqlContext.sql(sqlStatement)

# Copy data from our dataframe into the new Hive table
days.createOrReplaceTempView("vwDays")
sqlContext.sql("INSERT INTO RETAIL_FORECAST_ENGINE.COUNTRY_DATES SELECT * FROM vwDays")
spark.catalog.dropTempView("vwDays")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE RETAIL_FORECAST_ENGINE.COUNTRY_DATES
# MAGIC
# MAGIC -- Potential future improvement: ZORDER this table by date
