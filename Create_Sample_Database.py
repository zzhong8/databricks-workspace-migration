# Databricks notebook source
# MAGIC %md
# MAGIC # Create Sample Database
# MAGIC
# MAGIC This script is used to create a small database with sample data.  This sample data is copied from an existing database.  However, because it contains only a small subset of a *real*
# MAGIC database, your scripts should be able to process all of the data in this database very quickly.  This allows you to test your code as you're developing it and iterate very quickly.
# MAGIC You can run your code as-is and process the entire database in just a few minutes because this database only has a few items.  Once your code is tested, you can run it against real
# MAGIC data by just changing the parameters; you do not need to modify your code.
# MAGIC
# MAGIC Please note that this database does not contain a *representative* sample of the data.  You cannot use this database for testing statistical ideas... only coding ideas.
# MAGIC
# MAGIC To use the script, you only need to specify the parameter values in the cell below.  The `retailer` and `client` variables will determine from which database the sample is taken.
# MAGIC The other two parameters let you specify the item numbers and store numbers you want to include.

# COMMAND ----------

# Use these variables to specify the source from which your sample database will be built
retailer = "WALMART"
client = "CLOROX"

# Specify the items and stores you want to include in the sample database
retailer_item_ids = [ "1386516", "9266150", "9290682", "551033404", "1362828", "417132", "550646457", "553302812", "551174052", "9292593" ]
organization_unit_nums = [ 27, 833, 896, 2259, 580 ]

# Do you want to run the optional "optimize files" process at the end?
optimize_files = False

# COMMAND ----------

from pyspark.sql import functions as pyf

# Use the parameters provided to build other values we will need to script the new database
source_database_name = "{0}_{1}_dv".format(retailer.strip().lower(), client.strip().lower())
destination_database_name = "test_sample2_dv"

# The parameters above provide the item numbers and store numbers... because those are easier for humans to interpret.
# But in the database, those things are referred to by a hash key, not the human-interpretable number.
# So to make our code more efficient, we will look up the hash keys associated with the items and stores and then use
# those hash keys for our querying.

# Also note that since we will be using those hash keys multiple times, we use the Spark "cache" function so it will
# remember the hash keys and not re-query for them every time.  (This helps this script run much faster.)

master_hk_name = "LINK_EPOS_SUMMARY_HK"
hash_keys = {}

# Look up the hash keys associated with the specified items
hash_keys["HUB_RETAILER_ITEM_HK"] = spark.table("{}.hub_retailer_item".format(source_database_name))\
                                      .filter(pyf.col("RETAILER_ITEM_ID").isin(retailer_item_ids))\
                                      .selectExpr("HUB_RETAILER_ITEM_HK AS hash_key")\
                                      .cache()

# Look up the hash keys associated with the specified stores
hash_keys["HUB_ORGANIZATION_UNIT_HK"] = spark.table("{}.hub_organization_unit".format(source_database_name))\
                                          .filter(pyf.col("ORGANIZATION_UNIT_NUM").isin(organization_unit_nums))\
                                          .selectExpr("HUB_ORGANIZATION_UNIT_HK AS hash_key")\
                                          .cache()

# Look up the hash keys associated with EPOS summary rows for the store/item combos
hash_keys[master_hk_name] = spark.table("{}.link_epos_summary".format(source_database_name)).alias("link")\
                              .join(hash_keys["HUB_RETAILER_ITEM_HK"], hash_keys["HUB_RETAILER_ITEM_HK"].hash_key == pyf.col("link.HUB_RETAILER_ITEM_HK"))\
                              .join(hash_keys["HUB_ORGANIZATION_UNIT_HK"], hash_keys["HUB_ORGANIZATION_UNIT_HK"].hash_key == pyf.col("link.HUB_ORGANIZATION_UNIT_HK"))\
                              .selectExpr(master_hk_name + " AS hash_key")\
                              .cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Hive Database
# MAGIC
# MAGIC The first thing we need to do is to create a database in the Hive metastore where our tables and views will live.  If the destination database already exists, we will drop it
# MAGIC so we can re-create it.
# MAGIC
# MAGIC Note, though, that it's dangerous to have an automated script like this dropping a database.  We would never want to drop a database that we weren't supposed to drop.  To reduce the
# MAGIC likelihood of this, we will use a random string (which I will call a `database_security_key`) and put it in the description of the database.  If the database we're trying to drop
# MAGIC does not have this random string in its description, we will not drop it.

# COMMAND ----------

database_security_key = "3J9F7X3S9A"

database_count = spark.sql("SHOW DATABASES LIKE '{}'".format(destination_database_name)).count()

# Remove any pre-existing copies of the test database and create a fresh new version
if database_count > 0:

    # Looks like we have an existing copy of the database in Hive
    # Let's check retrieve its description
    db_desc_df = spark.sql("DESCRIBE DATABASE {}".format(destination_database_name)).filter("database_description_item = 'Description'").collect()
    db_desc = db_desc_df[0][1]

    # Check to make sure the description string contains our random string (so we know it's a database that we created)
    if database_security_key in db_desc:
        spark.sql("DROP DATABASE {} CASCADE".format(destination_database_name))
    else:
        error_message = "Will not drop existing database `{0}`.  The database description does not contain the database security key.  Are you sure this database was created by this process?".format(destination_database_name)
        raise ValueError(error_message)


# Create a fresh new version of the database
sql_statement = '''
    CREATE DATABASE {0}
    COMMENT 'a database with a small subset of stores and items, used for quickly testing code  (Database Security Key: {1})'
'''.format(destination_database_name, database_security_key)

spark.sql(sql_statement)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the Structure of Tables and Views

# COMMAND ----------

# List all of the tables and views in the database we are going to emulate.  Filter out anything with a name ending with "_bckup" because those
# are just backup copies of tables, and we don't need those in our test database.
db_objects_df = spark.sql("SHOW TABLES IN {}".format(source_database_name))\
                  .filter("isTemporary = false")\
                  .filter("RIGHT(tableName, 6) <> '_bckup'")\
                  .select("tableName")

db_objects = [row[0] for row in db_objects_df.collect()]

# COMMAND ----------

# Loop through all of the database objects we found above and get the "CREATE" SQL statement for each one.
# Put "CREATE VIEW" statements in their own list.  "CREATE TABLE" statements go into another list.

views = {}
tables = {}
other_objs = {}

for obj in db_objects:
    ddl = spark.sql("SHOW CREATE TABLE {0}.{1}".format(source_database_name, obj)).first().createtab_stmt
    if ddl.startswith("CREATE VIEW"):
        views[obj] = ddl
    elif ddl.startswith("CREATE TABLE"):
        tables[obj] = ddl
    else:
        other_objs[obj] = ddl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the Tables and Views
# MAGIC
# MAGIC First, let's copy the structure of the source database to our new test database.  This means we'll create empty tables with a schema that matches the original source database.  We'll
# MAGIC also re-create all of the views found in the source database.

# COMMAND ----------

create_statements = []

# Views depend on tables.  So we will create all of the tables first.
for t in tables.keys():
    ddl = tables[t]
    phrases = ddl.split("\n")

    create_table_phrase = ""
    partitioned_by_phrase = ""

    # We're only interested in the "CREATE TABLE" and "PARTITIONED BY" phrases of the table DLL
    for phrase in phrases:
        if phrase.startswith("CREATE TABLE"):
            create_table_phrase = phrase
        elif phrase.startswith("PARTITIONED BY"):
            partitioned_by_phrase = phrase

    # We will add our own phrases that specify the file format and location
    using_phrase = "USING DELTA"
    location_phrase = "" #no need to specify a location right now; let Databricks handle it

    # Assemble all of the phrases into a new SQL statement
    create_statement = [create_table_phrase, using_phrase, location_phrase, partitioned_by_phrase]
    create_statement = "\n".join(create_statement)
    create_statements.append(create_statement)

# COMMAND ----------

# Now we can process the views...  They don't need much processing so put them straight to the create statements list
for v in views.keys():
    ddl = views[v]
    create_statements.append(ddl)

# COMMAND ----------

# Now we will actually run the create statements that we cataloged above.

# Set Spark's current database focus to our new sample database.  This controls the database in which our new objects will be created
spark.sql("USE {}".format(destination_database_name))

# We need to do a string replace to pull out the source database name and put in our test database name
import re
db_name_regex = re.compile(re.escape(source_database_name), re.IGNORECASE)

for c in create_statements:
    create_statement = db_name_regex.sub(destination_database_name, c)
    print(re.search('CREATE [A-Z]* `.*?`\.`.*?`', create_statement, re.IGNORECASE).group())
    spark.sql(create_statement)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importing Data Into the Tables
# MAGIC
# MAGIC Now that we have created the database structure, it's time to import some data.  We will query the tables in the source database, pull out only the rows that apply to our specified items and stores, and pump the results into the new test database.

# COMMAND ----------

target_columns = hash_keys.keys()

number_of_tables = len(tables)
counter = 0

for t in tables.keys():
    counter += 1

    source_table = "{0}.{1}".format(source_database_name, t)
    destination_table = "{0}.{1}".format(destination_database_name, t)

    # Get the names of all of the columns in the destination table
    all_columns_df = spark.sql("SHOW COLUMNS IN " + destination_table).collect()
    all_columns = [row[0].upper() for row in all_columns_df]

    # Check all of the columns in the table and figure out which ones we will be filtering on
    filter_columns = []
    for tc in target_columns:
        if tc in all_columns:
            filter_columns.append(tc)

    # If a table contains the "LINK_EPOS_SUMMARY_HK" column, then that's the only column we need to filter by
    if master_hk_name in filter_columns:
        filter_columns = [master_hk_name]

    # Print some messages about the work we're doing so you know what's going on while it's running
    print("{0:02d} of {1:02d}   Loading data into table `{2}`".format(counter, number_of_tables, destination_table))
    if len(filter_columns) == 0:
        print("           NO FILTERS")
        print("           " + str(all_columns))
        
    else:
        print("           " + str(filter_columns))

    # Query the source database and apply filters to it
    # Note that we once again use Spark's "cache" function since we'll use the dataframe twice (once to count rows and once to load the data).  Caching prevents us from reading twice.
    df = spark.table(source_table).alias("x")
    
    for fc in filter_columns:
        df = df.join(hash_keys[fc], hash_keys[fc].hash_key == pyf.col("x." + fc))
        
    df = df.select("x.*").cache()

    row_count = df.count()
    print( "           Rows: {:,}".format(row_count) )

    if len(filter_columns) == 0 and row_count > 100000:
        # if there are no filters specified for the table and it looks like a big table then just skip it
        # This prevents the code from running away and moving a bunch of data if a new (unexpected) table gets added to the source database
        print("        !! SKIPPING TABLE !!! There are two many rows to load for a table that has no filters specified")
        
    else:
        # Load the data we pulled from the source database into the destination database
        df.write.insertInto(destination_table)

    # We're done with this dataframe now so we can remove it from the cache
    df.unpersist()

    print("================================================================================")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimizing Files
# MAGIC The process above creates the new tables as Delta files in Azure blob storage.  Depending on how the cluster is configured, the data could be spread across multiple, small files.
# MAGIC This makes it slower for Spark to read the data.
# MAGIC
# MAGIC To improve the read performance, you can use Databricks Delta's `OPTIMIZE` feature.  This cleans up the files and optimizes their layout for further processing.  However, this
# MAGIC optimization process can take some time (up to an hour).
# MAGIC
# MAGIC The optimization process is 100% **optional**. You can control whether or not it runs using the `optimize_files` variable at the top of this notebook.
# MAGIC You need to decide if it's worth investing an hour of processing time to optimize the files just to shave a few seconds off all of your future queries.

# COMMAND ----------

if optimize_files:
    spark.sql("USE {}".format(destination_database_name))
    databricks_retentioncheck_current = spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    for t in tables.keys():
        print("Optimizing table {}".format(t))
        spark.sql("OPTIMIZE `{}`".format(t))
        spark.sql("VACUUM `{}` RETAIN 0 hours".format(t))

    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", databricks_retentioncheck_current)
