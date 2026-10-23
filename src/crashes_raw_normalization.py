import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, trim, upper, coalesce, date_format, regexp_replace,
    to_timestamp, concat_ws, when, lit, hour, month, year,
    rand
)
from pyspark.sql.types import IntegerType, LongType


def normalize_data():
    # 1. Initialize Spark Session
    spark = SparkSession.builder.appName("NYCCrashNormalization").getOrCreate()

    # Allow flexible date parsing (handles mixed M/d/yyyy vs MM/dd/yyyy etc.)
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # 2. Define input and output paths (Update these paths if needed)
    input_path = "gs://YOUR_BUCKET_NAME/raw_data/crashes_RAW.csv"
    parquet_output = "gs://YOUR_BUCKET_NAME/processed_data/crashes_cleaned_1M.parquet"
    csv_output = "gs://YOUR_BUCKET_NAME/processed_data/crashes_excel_folder_1M"

    # 3. Read the full CSV data
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # 4. Bulletproof Column Name Normalization (Regex)
    #    Converts "CRASH DATE", "Vehicle Type Code 1", "NUMBER OF PERSONS INJURED" etc.
    #    into clean snake_case: crash_date, vehicle_type_code_1, number_of_persons_injured
    for col_name in df.columns:
        clean_name = re.sub(r"[^a-zA-Z0-9]", "_", col_name.strip()).lower()
        clean_name = re.sub(r"_+", "_", clean_name).rstrip("_")
        df = df.withColumnRenamed(col_name, clean_name)

    # ------------------------------------------------------------------
    # 5. Advanced Data Cleaning & Normalization Rules
    # ------------------------------------------------------------------

    # A. Parse mixed date formats into a proper DateType
    df = df.withColumn(
        "crash_date",
        coalesce(
            to_date(col("crash_date"), "M/d/yyyy"),
            to_date(col("crash_date"), "M/d/yy"),
            to_date(col("crash_date"), "MM/dd/yyyy"),
            to_date(col("crash_date"), "MM/dd/yy"),
            to_date(col("crash_date"), "yyyy-MM-dd"),
        ),
    )

    # B. Combine date + time into a single TIMESTAMP (critical for time-based features)
    #    Format "H:mm" accepts both "5:15" and "15:00"
    df = df.withColumn(
        "crash_timestamp",
        to_timestamp(
            concat_ws(" ", col("crash_date"), col("crash_time")), "yyyy-MM-dd H:mm"
        ),
    )

    # Keep a clean string version of the date for easy CSV / Excel use
    df = df.withColumn("crash_date", date_format(col("crash_date"), "yyyy-MM-dd"))

    # C. Drop the redundant LOCATION column (it is just a stringified lat/long pair)
    if "location" in df.columns:
        df = df.drop("location")

    # D. Dynamic text cleaning for every string column
    #    - Trim whitespace
    #    - Upper-case for consistency
    #    - Collapse internal multiple spaces
    #    - Convert pure empty strings to real nulls
    string_columns = [
        f.name for f in df.schema.fields if f.dataType.simpleString() == "string"
    ]
    for c in string_columns:
        cleaned = upper(trim(col(c)))
        cleaned = regexp_replace(cleaned, r"\s+", " ")
        cleaned = when(cleaned == "", None).otherwise(cleaned)
        df = df.withColumn(c, cleaned)

    # E. Geographic bounds filter – keep only realistic NYC-area coordinates
    #    (removes 0/0, -201.*, and other data-entry garbage)
    df = df.filter(
        (col("latitude").between(40.40, 41.00))
        & (col("longitude").between(-74.30, -73.60))
    )

    # F. Drop rows that still lack the most critical fields after parsing
    df = df.dropna(subset=["crash_date", "collision_id", "crash_timestamp", "latitude", "longitude"])

    # G. Clean & standardize borough
    valid_boroughs = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]
    df = df.withColumn(
        "borough",
        when(col("borough").isin(valid_boroughs), col("borough")).otherwise(
            lit("UNSPECIFIED")
        ),
    )

    # H. Clean ZIP code – keep only true 5-digit ZIPs, everything else → UNKNOWN
    df = df.withColumn(
        "zip_code",
        when(
            (col("zip_code").rlike(r"^\d{5}$")), col("zip_code")
        ).otherwise(lit("UNKNOWN")),
    )

    # I. Fill nulls in the numeric injury / fatality count columns with 0 and cast to int
    count_columns = [c for c in df.columns if c.startswith("number_of_")]
    for c in count_columns:
        df = df.withColumn(c, coalesce(col(c).cast(IntegerType()), lit(0)))

    # J. Fill nulls / blanks in contributing-factor and vehicle-type columns
    factor_and_vehicle_cols = [
        c
        for c in df.columns
        if "contributing_factor" in c or "vehicle_type_code" in c
    ]
    for c in factor_and_vehicle_cols:
        df = df.withColumn(c, coalesce(col(c), lit("UNSPECIFIED")))

    # K. Fill street-name nulls so downstream feature engineering does not break
    for c in ["on_street_name", "cross_street_name", "off_street_name"]:
        if c in df.columns:
            df = df.withColumn(c, coalesce(col(c), lit("UNKNOWN")))

    # L. Ensure collision_id is a clean long integer and remove any residual duplicates
    df = df.withColumn("collision_id", col("collision_id").cast(LongType()))
    df = df.dropDuplicates(["collision_id"])

    # ------------------------------------------------------------------
    # 6. Feature engineering – columns that make the ML project much easier
    # ------------------------------------------------------------------

    # Time-based features extracted from the unified timestamp
    df = df.withColumn("hour_of_day", hour(col("crash_timestamp")))
    df = df.withColumn("month", month(col("crash_timestamp")))
    df = df.withColumn("year", year(col("crash_timestamp")))
    # dayofweek: 1 = Sunday … 7 = Saturday; we also keep a readable name
    df = df.withColumn(
        "day_of_week",
        upper(date_format(col("crash_timestamp"), "EEEE")),
    )

    # Target variables for classification / regression
    # severity: 0 = property damage only, 1 = injury, 2 = fatality
    df = df.withColumn(
        "severity",
        when(col("number_of_persons_killed") > 0, 2)
        .when(col("number_of_persons_injured") > 0, 1)
        .otherwise(0),
    )
    # Convenient binary flag
    df = df.withColumn(
        "has_injury", when(col("severity") > 0, 1).otherwise(0)
    )

    # ------------------------------------------------------------------
    # 7. Enforce the ~1M row limit AFTER all cleaning (random sample)
    # ------------------------------------------------------------------
    current_count = df.count()
    print(f"Rows remaining after full normalization & feature engineering: {current_count}")

    TARGET_ROWS = 1000000
    if current_count > TARGET_ROWS:
        print(f"Randomly sampling down to exactly {TARGET_ROWS} rows ...")
        # orderBy(rand()) + limit is simple and works well on a Dataproc cluster
        df = df.orderBy(rand(seed=42)).limit(TARGET_ROWS)
    else:
        print(f"Dataset already ≤ {TARGET_ROWS} rows – keeping all {current_count} rows.")

    # Final column order (nice for humans and for notebooks)
    desired_order = [
        "collision_id",
        "crash_date",
        "crash_time",
        "crash_timestamp",
        "year",
        "month",
        "day_of_week",
        "hour_of_day",
        "borough",
        "zip_code",
        "latitude",
        "longitude",
        "on_street_name",
        "cross_street_name",
        "off_street_name",
        "number_of_persons_injured",
        "number_of_persons_killed",
        "number_of_pedestrians_injured",
        "number_of_pedestrians_killed",
        "number_of_cyclist_injured",
        "number_of_cyclist_killed",
        "number_of_motorist_injured",
        "number_of_motorist_killed",
        "contributing_factor_vehicle_1",
        "contributing_factor_vehicle_2",
        "contributing_factor_vehicle_3",
        "contributing_factor_vehicle_4",
        "contributing_factor_vehicle_5",
        "vehicle_type_code_1",
        "vehicle_type_code_2",
        "vehicle_type_code_3",
        "vehicle_type_code_4",
        "vehicle_type_code_5",
        "severity",
        "has_injury",
    ]
    # Keep only columns that actually exist (defensive)
    final_cols = [c for c in desired_order if c in df.columns]
    df = df.select(final_cols)

    # 8. Save as Parquet (best for Spark/ML) and a single CSV (easy for Excel / sklearn)
    df.write.mode("overwrite").parquet(parquet_output)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_output)

    final_count = df.count()
    print(f"Job complete! Final dataset contains {final_count} clean rows.")
    print("Files written to:")
    print(f"  Parquet → {parquet_output}")
    print(f"  CSV     → {csv_output}")
    spark.stop()


if __name__ == "__main__":
    normalize_data()
