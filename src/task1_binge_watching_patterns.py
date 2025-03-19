from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import round as spark_round

def initialize_spark(app_name="Task1_Binge_Watching_Patterns"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the movie ratings data from a CSV file into a Spark DataFrame.
    """
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def detect_binge_watching_patterns(df):
    """
    Identify the percentage of users in each age group who binge-watch movies.

    Steps:
    1. Filter users who have IsBingeWatched = True.
    2. Group by AgeGroup and count the number of binge-watchers.
    3. Count the total number of users in each age group.
    4. Calculate the binge-watching percentage for each age group.
    """
    # Step 1: Filter users who have IsBingeWatched = True
    binge_watch_df = df.filter(F.col("IsBingeWatched") == True)

    # Step 2: Group by AgeGroup and count binge-watchers
    binge_watch_counts = binge_watch_df.groupBy("AgeGroup").count().withColumnRenamed("count", "BingeWatchers")

    # Step 3: Count the total number of users in each age group
    total_users = df.groupBy("AgeGroup").count().withColumnRenamed("count", "TotalUsers")

    # Step 4: Join both DataFrames (binge_watch_counts and total_users) on AgeGroup
    result_df = binge_watch_counts.join(total_users, on="AgeGroup", how="left")

    # Step 5: Calculate the binge-watching percentage for each age group
    result_df = result_df.withColumn(
        "Percentage",
        (F.col("BingeWatchers") / F.col("TotalUsers")) * 100
    )

    # Round the Percentage to 2 decimal places
    result_df = result_df.withColumn("Percentage", spark_round(F.col("Percentage"), 2))

    # Return the final DataFrame with AgeGroup, BingeWatchers, TotalUsers, and Percentage
    return result_df.select("AgeGroup", "BingeWatchers", "Percentage")

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 1.
    """
    spark = initialize_spark()

    input_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-sdj2001/input/movie_ratings_data.csv"
    output_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-sdj2001/Outputs/binge_watching_patterns.csv"  # Updated output path

    df = load_data(spark, input_file)
    result_df = detect_binge_watching_patterns(df)  # Call function here
    write_output(result_df, output_file)

    spark.stop()

if __name__ == "__main__":
    main()
