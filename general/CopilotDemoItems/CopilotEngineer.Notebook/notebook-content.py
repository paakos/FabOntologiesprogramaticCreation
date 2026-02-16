# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "698c1415-2e06-4754-b5c5-e78626212a7b",
# META       "default_lakehouse_name": "ontology",
# META       "default_lakehouse_workspace_id": "ba719d48-0ec5-4425-846f-885d29d89764",
# META       "known_lakehouses": [
# META         {
# META           "id": "698c1415-2e06-4754-b5c5-e78626212a7b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM ontology.dbo.dimproducts LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

from pyspark.sql.functions import concat_ws

# Combine 'ProductName' and 'Category' columns, separated by '||', into a new column
df_M = df.withColumn(
    "productname_category",
    concat_ws("||", df["ProductName"], df["Category"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_M)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

df_M.write.mode("overwrite").format("delta").saveAsTable("dimproducts_modified")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Chat in cell 

# CELL ********************

# MAGIC %%chat
# MAGIC analyze the df dataframe


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
