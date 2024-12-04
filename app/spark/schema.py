from pyspark.sql.types import *

schema = StructType([
   StructField("Country", StringType()),
   StructField("Year", StringType()),
   StructField("Disease Name", StringType()),
   StructField("Disease Category", StringType()),
   StructField("Prevalence Rate (%)", StringType()),
   StructField("Incidence Rate (%)", StringType()),
   StructField("Mortality Rate (%)", StringType()),
   StructField("Age Group", StringType()),
   StructField("Gender", StringType()),
   StructField("Population Affected", StringType()),
   StructField("Healthcare Access (%)", StringType()),
   StructField("Doctors per 1000", StringType()),
   StructField("Hospital Beds per 1000", StringType()),
   StructField("Treatment Type", StringType()),
   StructField("Average Treatment Cost (USD)", StringType()),
   StructField("Availability of Vaccines/Treatment", StringType()),
   StructField("Recovery Rate (%)", StringType()),
   StructField("DALYs", StringType()),
   StructField("Improvement in 5 Years (%)", StringType()),
   StructField("Per Capita Income (USD)", StringType()),
   StructField("Education Index", StringType()),
   StructField("Urbanization Rate (%)", StringType())
])