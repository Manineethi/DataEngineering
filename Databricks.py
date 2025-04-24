


df = spark.read.format("csv")\
            .option("header", True)\
            .option("inferSchema", True)\
            .load("abfss://1stage@manistoragedatalake.dfs.core.windows.net/healthcare")
			
			
df.printSchema()

from pyspark.sql.functions import col, sum

# Count null values for each column
df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

df.show(truncate=False)  # Show full content without truncation

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, datediff, col, avg, count, round, year, corr
import matplotlib.pyplot as plt

# Data Cleaning and Feature Engineering
df_cleaned = df.withColumn("Name", initcap(col("Name"))) \
               .withColumn("Length of Stay", datediff(col("Discharge Date"), col("Date of Admission"))) \
               .withColumn("Admission Year", year(col("Date of Admission"))) \
               .withColumn("Age_Range", when(col("Age") < 20, "<20")
                                  .when(col("Age").between(20, 39), "20-39")
                                  .when(col("Age").between(40, 59), "40-59")
                                  .otherwise("60+"))
								  
								  
# 1. Patient Demographics
# Age Distribution (binned)
age_bins = df_cleaned.selectExpr("CASE WHEN Age < 20 THEN '<20' " +
                                 "WHEN Age BETWEEN 20 AND 39 THEN '20-39' " +
                                 "WHEN Age BETWEEN 40 AND 59 THEN '40-59' " +
                                 "ELSE '60+' END AS Age_Range") \
                     .groupBy("Age_Range").agg(count("*").alias("Count")).orderBy("Age_Range")
display(age_bins)

# Gender Distribution
gender_dist = df_cleaned.groupBy("Gender").agg(count("*").alias("Count")).toPandas()
plt.pie(gender_dist["Count"], labels=gender_dist["Gender"], autopct='%1.1f%%')
plt.title("Gender Distribution")
display(plt.gcf())

# Blood Type Distribution
blood_dist = df_cleaned.groupBy("Blood Type").agg(count("*").alias("Count")).orderBy("Count", ascending=False)
display(blood_dist)

# 2. Billing Analysis
# Average Billing by Medical Condition
billing_by_condition = df_cleaned.groupBy("Medical Condition") \
                                .agg(round(avg("Billing Amount"), 2).alias("Avg Billing"), 
                                     count("*").alias("Patient Count")) \
                                .orderBy("Avg Billing", ascending=False)
display(billing_by_condition)

# Correlation with Insurance Provider
insurance_corr = df_cleaned.groupBy("Insurance Provider") \
                          .agg(round(avg("Billing Amount"), 2).alias("Avg Billing")) \
                          .join(df_cleaned.select("Billing Amount", "Insurance Provider"), "Insurance Provider") \
                          .select(corr("Avg Billing", "Billing Amount").alias("Billing_Insurance_Corr"))
display(insurance_corr)

# 3. Hospital Stay Metrics
stay_by_condition = df_cleaned.groupBy("Medical Condition") \
                             .agg(round(avg("Length of Stay"), 1).alias("Avg Length of Stay"), 
                                  count("*").alias("Patient Count")) \
                             .orderBy("Avg Length of Stay", ascending=False)
display(stay_by_condition)

# 4. Medication and Test Outcomes
med_freq = df_cleaned.groupBy("Medication").agg(count("*").alias("Count")).orderBy("Count", ascending=False)
display(med_freq)

test_dist = df_cleaned.groupBy("Test Results").agg(count("*").alias("Count")).orderBy("Count", ascending=False)
display(test_dist)

# 5. Hospital and Doctor Performance
hospital_stats = df_cleaned.groupBy("Hospital") \
                          .agg(count("*").alias("Patient Count"), 
                               round(avg("Billing Amount"), 2).alias("Avg Billing"), 
                               round(avg("Length of Stay"), 1).alias("Avg Length of Stay")) \
                          .orderBy("Patient Count", ascending=False)
display(hospital_stats)

doctor_stats = df_cleaned.groupBy("Doctor") \
                         .agg(count("*").alias("Patient Count"), 
                              round(avg("Billing Amount"), 2).alias("Avg Billing")) \
                         .orderBy("Patient Count", ascending=False)
display(doctor_stats)

# Leading Medical Condition and Age Range
leading_condition = df_cleaned.groupBy("Medical Condition") \
                             .agg(count("*").alias("Patient Count")) \
                             .orderBy("Patient Count", ascending=False).limit(1)
leading_condition_age = df_cleaned.join(leading_condition, "Medical Condition") \
                                 .groupBy("Age_Range").agg(count("*").alias("Count")) \
                                 .orderBy("Count", ascending=False)
display(leading_condition)
display(leading_condition_age)

# Trend Over Years
yearly_trend = df_cleaned.groupBy("Admission Year", "Medical Condition") \
                        .agg(count("*").alias("Patient Count")) \
                        .orderBy("Admission Year")
display(yearly_trend)

# Gender Comparison
gender_compare = df_cleaned.groupBy("Gender") \
                          .agg(round(avg("Billing Amount"), 2).alias("Avg Billing"), 
                               round(avg("Length of Stay"), 1).alias("Avg Length of Stay"), 
                               count("*").alias("Patient Count"))
display(gender_compare)

# Blood Type Impact on Conditions
blood_condition = df_cleaned.groupBy("Blood Type", "Medical Condition") \
                           .agg(count("*").alias("Count")) \
                           .orderBy("Blood Type", "Count", ascending=False)
display(blood_condition)

#Resource Allocation Optimization
resource_alloc = df_cleaned.groupBy("Hospital") \
                          .agg(count("*").alias("Patient Load"), 
                               round(avg("Length of Stay"), 1).alias("Avg Stay"), 
                               round(avg("Billing Amount"), 2).alias("Avg Billing")) \
                          .orderBy("Patient Load", ascending=False)
display(resource_alloc)