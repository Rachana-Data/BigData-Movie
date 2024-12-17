# Databricks notebook source
# MAGIC %md
# MAGIC # Extrating the data From "[https://www.themoviedb.org/](url)"

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ## Just Representing How the API key stored as secret and Retrieving it (API)

# COMMAND ----------

import requests
API_KEY = dbutils.secrets.get(scope="tmdb_scope", key="tmdb_api_key")

url = f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}&language=en-US&page=1"
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    if 'results' in data and len(data['results']) > 0:
        first_movie = data['results'][0]
        df = pd.DataFrame([first_movie])
        display(df)
        print("API Key retrieved successfully and A Smaple Movie data fetched Succesfully.")
else:
    print("Failed to fetch data. Status Code:", response.status_code)

# COMMAND ----------

import requests
import time
import pandas as pd

def fetch_all_movies_for_year(api_key, year):
    movies = []
    page = 1

    while True:
        url = f"https://api.themoviedb.org/3/discover/movie?api_key={api_key}&language=en-US&primary_release_year={year}&page={page}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            movies.extend(data['results'])  
            total_pages = data.get('total_pages', 0)  
            
            if page >= total_pages:
                break
            else:
                page += 1  
        else:
            break  
        
        time.sleep(0.01)

    return movies


def fetch_movie_details(api_key, movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&language=en-US"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            "adult": data.get("adult"),
            "budget": data.get("budget"),
            "genres": [genre["name"] for genre in data.get("genres", [])],
            "id": data.get("id"),
            "overview": data.get("overview"),
            "popularity": data.get("popularity"),
            "production_countries": [country["name"] for country in data.get("production_countries", [])],
            "release_date": data.get("release_date"),
            "revenue": data.get("revenue"),
            "runtime": data.get("runtime"),
            "spoken_languages": [lang["name"] for lang in data.get("spoken_languages", [])],
            "status": data.get("status"),
            "title": data.get("title"),
            "video": data.get("video"),
            "vote_average": data.get("vote_average"),
            "vote_count": data.get("vote_count"),
        }
    else:
        return None

API_KEY = dbutils.secrets.get(scope="tmdb_scope", key="tmdb_api_key")

start_year = 1970
end_year = 2024

total_records = 0

for year in range(end_year, start_year - 1, -1):  
    print(f"Fetching movies for year {year}...")
    
    movies_data = fetch_all_movies_for_year(API_KEY, year)
    
    detailed_movies = []
    for i, movie in enumerate(movies_data, start=1):
        movie_details = fetch_movie_details(API_KEY, movie['id'])
        if movie_details:
            detailed_movies.append(movie_details)
            total_records += 1
        
        if total_records % 1000 == 0:
            print(f"Total Records Processed: {total_records}")
        
        if total_records >= 106632:
            print(f"Reached 100,000 records. Stopping.")
            break
    
    df_year = pd.DataFrame(detailed_movies)
    file_name = f"movies_data_{year}.csv"
    df_year.to_csv(file_name, index=False)
    
    print(f"Year {year}: Total Movies Fetched: {len(df_year)}")
    
    if total_records >= 106632:
        break

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import os
import glob
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql.functions import col, sum
from pyspark.sql import Row

# COMMAND ----------


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Combine CSV Files with Sample Date Column") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

# Directory containing the CSV files
data_dir = "./"  # Change this to the directory where CSV files are located

# Step 1: Dynamically fetch all CSV files starting with "movies_data_"
csv_files = glob.glob(os.path.join(data_dir, "movies_data_*.csv"))
print(f"Found {len(csv_files)} files to combine:")
print(csv_files)

# Step 2: Define Spark Schema with LongType for large integer values
schema = StructType([
    StructField("adult", StringType(), True),
    StructField("budget", LongType(), True),  # LongType to handle large integers
    StructField("genres", StringType(), True),
    StructField("id", LongType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", FloatType(), True),
    StructField("production_countries", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", LongType(), True),  # LongType to handle large integers
    StructField("runtime", LongType(), True),
    StructField("spoken_languages", StringType(), True),
    StructField("status", StringType(), True),
    StructField("title", StringType(), True),
    StructField("video", StringType(), True),
    StructField("vote_average", FloatType(), True),
    StructField("vote_count", LongType(), True),
    StructField("sample_date", StringType(), True)  # Added sample_date
])

# Step 3: Combine and Add "sample_date" Column
combined_df = pd.DataFrame()
current_date = datetime.now().strftime("%Y-%m-%d")  # Capture current date as sample_date

for file in csv_files:
    try:
        print(f"Processing file: {file}")
        temp_df = pd.read_csv(file)
        temp_df["sample_date"] = current_date  # Add sample_date column
        temp_df["adult"] = temp_df["adult"].astype(str)  # Convert 'adult' column to string
        combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
    except Exception as e:
        print(f"Error processing file {file}: {e}")

# Step 4: Save the Combined DataFrame to a New CSV File
combined_csv_file = os.path.abspath("combined_movies_data_with_date.csv")
combined_df.to_csv(combined_csv_file, index=False)
print(f"Combined CSV file saved as: {combined_csv_file}")

# Step 5: Load the Combined CSV into Spark DataFrame
print("Loading combined CSV file into Spark DataFrame...")
spark_df = spark.createDataFrame(combined_df, schema=schema)

# Step 6: Verify the Spark DataFrame
print("Schema of the Spark DataFrame:")
spark_df.printSchema()
print("First 10 rows of the Spark DataFrame:")
spark_df.show(10, truncate=False)

# Step 7: Save the Spark DataFrame as Parquet with Absolute Path
output_parquet = os.path.abspath("movies_data_combined_with_date.parquet")
print(f"Saving Spark DataFrame as Parquet file to: {output_parquet}")
spark_df.write.mode("overwrite").parquet(output_parquet)

print(f"Spark DataFrame successfully saved as Parquet file: {output_parquet}")


# COMMAND ----------

# Total Rows and Columns
total_rows = spark_df.count()
total_columns = len(spark_df.columns)

# Tabular display
from tabulate import tabulate
summary_data = [["Total Rows", total_rows], ["Total Columns", total_columns]]
print("\nSummary Table:")
print(tabulate(summary_data, headers=["Metric", "Value"], tablefmt="grid"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer

# COMMAND ----------

# MAGIC %md 
# MAGIC Checking For the null values 

# COMMAND ----------

from pyspark.sql.functions import col, sum

#Show the Total Rows and Columns
total_rows = spark_df.count()
total_columns = len(spark_df.columns)

print("\nSummary of DataFrame:")
print(f"Total Rows: {total_rows}")
print(f"Total Columns: {total_columns}")

#Count Non-Null Values for Each Column
print("\nNon-Null Value Counts for Each Column:")
non_null_counts = spark_df.select([
    sum(col(c).isNotNull().cast("int")).alias(c) for c in spark_df.columns
])
non_null_counts.show(truncate=False)



# Count Null Values for Each Column
print("\nNon-Null Value Counts for Each Column:")
non_null_counts = spark_df.select([
    sum(col(c).isNull().cast("int")).alias(c) for c in spark_df.columns
])
non_null_counts.show(truncate=False)

# COMMAND ----------


# Print the Schema of the DataFrame
print("Schema of the DataFrame:")
spark_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Getting the Data-Types
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, split, regexp_replace, to_date

# Step 1: Start from the existing spark_df and clean it
clean_df = spark_df \
    .withColumn("adult", col("adult").cast("boolean")) \
    .withColumn("budget", col("budget").cast("long")) \
    .withColumn("genres", split(regexp_replace(col("genres"), "[\\[\\]']", ""), ", ")) \
    .withColumn("id", col("id").cast("long")) \
    .withColumn("overview", col("overview").cast("string")) \
    .withColumn("popularity", col("popularity").cast("float")) \
    .withColumn("production_countries", split(regexp_replace(col("production_countries"), "[\\[\\]']", ""), ", ")) \
    .withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd")) \
    .withColumn("revenue", col("revenue").cast("long")) \
    .withColumn("runtime", col("runtime").cast("int")) \
    .withColumn("spoken_languages", split(regexp_replace(col("spoken_languages"), "[\\[\\]']", ""), ", ")) \
    .withColumn("status", col("status").cast("string")) \
    .withColumn("title", col("title").cast("string")) \
    .withColumn("video", col("video").cast("boolean")) \
    .withColumn("vote_average", col("vote_average").cast("float")) \
    .withColumn("vote_count", col("vote_count").cast("int"))

# Step 2: Drop any duplicate rows based on unique ID
clean_df = clean_df.dropDuplicates(["id"])

# Step 3: Filter rows with invalid or missing critical values
clean_df = clean_df.filter(
    (col("id").isNotNull()) & 
    (col("title").isNotNull()) &
    (col("release_date").isNotNull()) 

)

# Step 4: Verify Cleaned Schema and Data
print("Cleaned Data Schema:")
clean_df.printSchema()


# Step 5: Show Total Row Count After Cleaning
total_rows = clean_df.count()
print(f"\nTotal Rows in Cleaned DataFrame: {total_rows}")


# COMMAND ----------

# MAGIC %md
# MAGIC Trimming and Standardizing Strings 

# COMMAND ----------

from pyspark.sql.functions import trim, lower

clean_df = clean_df.withColumn("title", lower(trim(col("title")))) \
                   .withColumn("status", lower(trim(col("status"))))


# COMMAND ----------

clean_df = clean_df.withColumnRenamed("vote_average", "average_vote") \
                   .withColumnRenamed("vote_count", "total_votes")


# COMMAND ----------

# MAGIC %md
# MAGIC Handling Invalid or Inconsistent Values 

# COMMAND ----------

from pyspark.sql.functions import min, max

# Step 1: Aggregate to Find Min and Max for Specific Columns
stats_df = clean_df.agg(
    min("runtime").alias("min_runtime"), max("runtime").alias("max_runtime"),
    min("budget").alias("min_budget"), max("budget").alias("max_budget"),
    min("revenue").alias("min_revenue"), max("revenue").alias("max_revenue"),
    min("total_votes").alias("min_total_votes"), max("total_votes").alias("max_total_votes"),
    min("average_vote").alias("min_average_vote"), max("average_vote").alias("max_average_vote"),
    min("popularity").alias("min_popularity"), max("popularity").alias("max_popularity")
)

# Step 2: Show the Results
print("Minimum and Maximum Values for Key Numeric Columns:")
stats_df.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Add new Columns like Month and Year 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import date_format

clean_df = clean_df.withColumn("month_name", date_format(col("release_date"), "MMMM"))


print("Schema after adding 'month_name':")
clean_df.printSchema()

print("\nSample Data with 'month_name':")
clean_df.select("title", "release_date", "month_name").show(5, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import year

# Add a new column 'year' extracted from 'release_date'
clean_df = clean_df.withColumn("year", year(col("release_date")))

# Verify the schema and the updated DataFrame
print("Schema after adding 'year':")
clean_df.printSchema()

print("\nSample Data with 'year':")
clean_df.select("title", "release_date", "year").show(5, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Calculation the profit Percent(%)

# COMMAND ----------

from pyspark.sql.functions import when, col, round

# Step 1: Add 'profit_percentage' column safely handling zero or null budgets
clean_df = clean_df.withColumn(
    "profit_percentage",
    when(col("budget") > 0, ((col("revenue") - col("budget")) / col("budget")) * 100)
    .otherwise(None)
)

# Step 2: Replace null values with 0
clean_df = clean_df.fillna({"profit_percentage": 0})

# Step 3: Round 'profit_percentage' to 2 decimal places
clean_df = clean_df.withColumn("profit_percentage", round(col("profit_percentage"), 2))

# Step 4: Verify the results
print("Schema after adding 'profit_percentage':")
clean_df.printSchema()




# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ##Top Movies Production Countries

# COMMAND ----------

from pyspark.sql.functions import explode, col, lit, when, size
import matplotlib.pyplot as plt

clean_df = clean_df.withColumn(
    "production_countries",
    when(col("production_countries").isNull() | (size(col("production_countries")) == 0), lit(["Other"]))
    .otherwise(col("production_countries"))
)

production_country_count = clean_df.withColumn("country", explode(col("production_countries")))

production_country_count = production_country_count.groupBy("country").count().orderBy("count", ascending=False)

production_country_count = production_country_count.filter(col("country") != "")

top_10_countries = production_country_count.limit(10).toPandas()

plt.fill_between(top_10_countries['country'], top_10_countries['count'], color="skyblue", alpha=0.4)
plt.plot(top_10_countries['country'], top_10_countries['count'], color="Slateblue", alpha=0.6)
plt.xlabel('Country')
plt.ylabel('Count')
plt.title('Top 10 Production Countries')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Top 10 Movies by Budget

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

top_10_budget_df = clean_df.orderBy(col('budget').desc()).select('title', 'budget', 'revenue', 'profit_percentage').limit(10)
top_10_budget_pandas = top_10_budget_df.toPandas()

# Extract data for plotting
titles = top_10_budget_pandas["title"]
budget = top_10_budget_pandas["budget"]
revenue = top_10_budget_pandas["revenue"]

# Plotting the bar chart
fig, ax = plt.subplots(figsize=(12, 6))

# Bar chart for Budget
ax.bar(titles, budget, label="Budget", color="teal", width=0.4, align="center")

# Bar chart for Revenue
ax.bar(titles, revenue, label="Revenue", color="orange", width=0.4, align="edge")

# Customizations
plt.title("Top 10 Movies by Budget: Budget vs Revenue")
plt.xlabel("Movie Title")
plt.ylabel("Amount (in billions)")
plt.xticks(rotation=45, ha="right")  # Rotate x-axis labels for better readability
plt.legend()
plt.tight_layout()

# Show the plot
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 10 Movies by Revenue

# COMMAND ----------


top_10_revenue_df = clean_df.orderBy(col("revenue").desc()).select(
    "title", "budget", "revenue", "profit_percentage"
).limit(10)

display(top_10_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Movie Release Count

# COMMAND ----------

import matplotlib.pyplot as plt

monthly_movie_count = clean_df.groupBy("month_name").count().orderBy(col("count").desc())
monthly_movie_count_pandas = monthly_movie_count.toPandas()

# Extract data for plotting
months = monthly_movie_count_pandas["month_name"]
counts = monthly_movie_count_pandas["count"]

# Plotting the bar chart
fig, ax = plt.subplots(figsize=(10, 6))

# Bar chart for monthly movie count
ax.bar(months, counts, color="skyblue")

# Customizations
plt.title("Number of Movies Released in Each Month")
plt.xlabel("Month")
plt.ylabel("Number of Movies")
plt.xticks(rotation=45, ha="right")  # Rotate x-axis labels for better readability
plt.tight_layout()

# Show the plot
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribution of Movie Genres(Top_5)

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# Assuming Spark session is already created
# spark = SparkSession.builder.appName("example").getOrCreate()

# Example data for genre_count
data = [("Action", 100), ("Comedy", 80), ("Drama", 60), ("Horror", 40), ("Sci-Fi", 20)]
columns = ["genre", "count"]
genre_count = spark.createDataFrame(data, columns)

# Step 1: Convert genre_count to Pandas DataFrame
genre_count_pandas = genre_count.limit(5).toPandas()

# Step 2: Extract data for plotting
labels = genre_count_pandas["genre"]  # Genre names
sizes = genre_count_pandas["count"]   # Counts
colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#aec7e8"]  # Custom colors

# Step 3: Plot the donut pie chart
plt.figure(figsize=(8, 6))
wedges, texts, autotexts = plt.pie(
    sizes, labels=None, autopct='%1.2f%%', startangle=140, colors=colors, pctdistance=0.85
)

# Draw a white circle in the middle to create a donut chart
centre_circle = plt.Circle((0, 0), 0.70, fc='white')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)

# Add a title
plt.title("Top 5 Genres by Count")

# Add legend
plt.legend(wedges, labels, title="Genres", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))

# Equal aspect ratio ensures the pie chart is circular
plt.axis('equal')

# Show the chart
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Count Drama Movies Year-on-Year
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.sql.functions import col, explode

# Step 1: Convert the Spark DataFrame to Pandas for plotting
drama_movies_yearly = clean_df.select(
        col("year"), explode(col("genres")).alias("genre")
    ) \
    .filter(col("genre") == "Drama") \
    .groupBy("year").count() \
    .orderBy("year")

# Convert to Pandas DataFrame
drama_movies_pandas = drama_movies_yearly.toPandas()

# Step 2: Extract data for plotting
years = drama_movies_pandas["year"]
counts = drama_movies_pandas["count"]

# Step 3: Create the filled area chart
plt.figure(figsize=(12, 6))

# Plot the filled area
plt.fill_between(years, counts, color="salmon", alpha=0.7, label="Drama Movies Count")

# Plot the line on top of the filled area
plt.plot(years, counts, color="red", linewidth=2)

# Add annotations for each point
for i, count in enumerate(counts):
    plt.text(years[i], counts[i] + 50, f"{count}", ha="center", va="bottom", fontsize=9)

# Customizations
plt.title("Year-on-Year Count of Drama Movies")
plt.xlabel("Year")
plt.ylabel("Number of Movies")
plt.xticks(rotation=45)
plt.grid(True, linestyle="--", alpha=0.5)
plt.legend()
plt.tight_layout()

# Show the plot
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Production Countries

# COMMAND ----------

from pyspark.sql.functions import explode, col, lit, when, size, sum as spark_sum, count

clean_df = clean_df.withColumn(
    "production_countries",
    when(col("production_countries").isNull() | (size(col("production_countries")) == 0), lit(["Other"]))
    .otherwise(col("production_countries"))
)

production_country_count = clean_df.withColumn("country", explode(col("production_countries")))

production_country_count = production_country_count.groupBy("country") \
    .agg(
        spark_sum("budget").alias("total_budget"),
        spark_sum("revenue").alias("total_revenue"),
        (spark_sum("revenue") - spark_sum("budget")).alias("profit"),
        ((spark_sum("revenue") - spark_sum("budget")) / spark_sum("budget") * 100).alias("profit_percentage"),
        count("id").alias("total_movies")
    ) \
    .orderBy("profit_percentage", ascending=False)

production_country_count = production_country_count.filter(col("country") != "")

top_10_countries = production_country_count.limit(10).toPandas()

# Plotting the bar chart
plt.figure(figsize=(12, 6))
plt.bar(top_10_countries["country"], top_10_countries["profit_percentage"], color="skyblue")
plt.xlabel("Country")
plt.ylabel("Profit Percentage")
plt.title("Top 10 Countries by Profit Percentage")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
