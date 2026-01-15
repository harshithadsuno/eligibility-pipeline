from pyspark.sql import SparkSession, functions as F
import yaml
import os

# Setting up Spark so we can read files and process them using Spark
spark = (
    SparkSession.builder
    .appName("EligibilityPipeline")
    .master("local[*]")
    .getOrCreate()
)

# loading partner configurations from the yaml file
def load_partners_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["partners"]

#bronze code

def load_raw_to_bronze(cfg):
    df = (
        spark.read.option("header", True)
        .option("sep", cfg["delimiter"])
        .csv(cfg["file_path"])
        .withColumn("partner_code", F.lit(cfg["partner_code"]))
        .withColumn("ingest_ts", F.current_timestamp())
    )
    return df

def write_bronze(df, partner_name):
    out_path = os.path.join("bronze", partner_name)
    df.write.mode("overwrite").parquet(out_path)
    print(f"bronze written: {out_path}")



# silver code

def normalize_phone(col):
    digits = F.regexp_replace(F.coalesce(col, F.lit("")), r"[^0-9]", "")
    return F.when(
        F.length(digits) == 10,
        F.concat_ws(
            "-",
            F.substring(digits, 1, 3),
            F.substring(digits, 4, 3),
            F.substring(digits, 7, 4),
        ),
    ).otherwise(F.lit(None))


def normalize_dob(col):
    s = F.trim(col)

    parsed = (
        F.when(s.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(s, "yyyy-MM-dd"))
         .when(s.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"), F.to_date(s, "MM/dd/yyyy"))
         .when(s.rlike(r"^\d{4}/\d{2}/\d{2}$"), F.to_date(s, "yyyy/MM/dd"))

         .otherwise(F.lit(None))
    )

    return F.date_format(parsed, "yyyy-MM-dd")


def bronze_to_silver(bronze_df, cfg):
    df = bronze_df

    # map partner columns -> standard columns
    for src, dst in cfg["column_mapping"].items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)

    # apply required transformations
    df = (
        df
        .withColumn("external_id", F.trim(F.col("external_id")))
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
        .withColumn("dob", normalize_dob(F.col("dob")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("phone", normalize_phone(F.col("phone")))
    )

    # validation: drop rows where external_id is missing/blank
    df = df.filter(F.col("external_id").isNotNull() & (F.col("external_id") != ""))

    # final silver schema (exact output fields; partner_code last)
    return df.select(
        "external_id",
        "first_name",
        "last_name",
        "dob",
        "email",
        "phone",
        "partner_code",
    )

def write_silver(df, partner_name):
    out_path = os.path.join("silver", partner_name)
    df.write.mode("overwrite").parquet(out_path)
    print(f"silver written: {out_path}")

#gold code

def write_gold(spark, partners):
    gold_df = None

    for partner_name in partners.keys():
        path = os.path.join("silver", partner_name)
        df = spark.read.parquet(path)

        if gold_df is None:
            gold_df = df
        else:
            gold_df = gold_df.unionByName(df)

    out_path = os.path.join("gold", "eligibility_unified")
    gold_df.write.mode("overwrite").parquet(out_path)
    print(f"gold written: {out_path}")





if __name__ == "__main__":
    partners = load_partners_config("config.yaml")

    for partner_name, cfg in partners.items():
        bronze_df = load_raw_to_bronze(cfg)
        write_bronze(bronze_df, partner_name)

        silver_df = bronze_to_silver(bronze_df, cfg)
        write_silver(silver_df, partner_name)

    write_gold(spark, partners)
    spark.stop()
