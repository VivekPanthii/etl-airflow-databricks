import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Bronze: Customer Info
@dlt.table(name='bronze_crm_data_cust_info', comment="Raw data reading from cust_info")
def bronze_cust_info():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze_crm_erp/bronze_volume/bronze_crm_data/cust_info/checkpoint")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load("/Volumes/workspace/raw_crm_erp/rawvolume/raw_crm_data/cust_info/")
    )

# Bronze: Product Info
@dlt.table(name='bronze_crm_data_prd_info', comment="Raw data reading from prd_info")
def bronze_prd_info():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze_crm_erp/bronze_volume/bronze_crm_data/prd_info/checkpoint")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load("/Volumes/workspace/raw_crm_erp/rawvolume/raw_crm_data/prd_info/")
    )

# Bronze: Sales Details
@dlt.table(name='bronze_crm_data_sales_details', comment="Raw data reading from sales_details")
def bronze_sales_details():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze_crm_erp/bronze_volume/bronze_crm_data/sales_details/checkpoint")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load("/Volumes/workspace/raw_crm_erp/rawvolume/raw_crm_data/sales_details/")
    )

# Bronze: ERP Customer AZ12
@dlt.table(name='bronze_erp_data_cust_az12', comment="Raw data reading from CUST_AZ12")
def bronze_erp_cust_az12():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze_crm_erp/bronze_volume/bronze_erp_data/CUST_AZ12/checkpoint")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load("/Volumes/workspace/raw_crm_erp/rawvolume/raw_erp_data/CUST_AZ12/")
    )

# Bronze: ERP LOC A101
@dlt.table(name='bronze_erp_data_LOC_A101', comment="Raw data reading from LOC_A101")
def bronze_erp_loc_a101():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze_crm_erp/bronze_volume/bronze_erp_data/LOC_A101/checkpoint")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load("/Volumes/workspace/raw_crm_erp/rawvolume/raw_erp_data/LOC_A101/")
    )

# Bronze: ERP PX_CAT_G1V2
@dlt.table(name='bronze_erp_data_PX_CAT_G1V2', comment="Raw data reading from PX_CAT_G1V2")
def bronze_erp_px_cat_g1v2():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze_crm_erp/bronze_volume/bronze_erp_data/PX_CAT_G1V2/checkpoint")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load("/Volumes/workspace/raw_crm_erp/rawvolume/raw_erp_data/PX_CAT_G1V2/")
    )




# Silver table: cleaned and transformed customer info
@dlt.table(
    name="silver_crm_data_cust_info",
    comment="Cleansed and transformed customer data"
)
def silver_cust_info():
    # Read from the Bronze DLT table
    df_cust_info = dlt.read("bronze_crm_data_cust_info")

    # Window spec for latest row per customer
    window_spec = Window.partitionBy("cst_id").orderBy(F.col("cst_create_date").desc())
    df_with_rownum = df_cust_info.withColumn("newDateUpdate", F.row_number().over(window_spec))
    latest_df = df_with_rownum.filter(F.col("newDateUpdate") == 1)

    # Dictionaries to map abbreviations
    marital_status_map = {"M": "Married", "S": "Single"}
    gender_map = {"F": "Female", "M": "Male"}

    # Transformation and cleansing
    silver_df = (
        latest_df
        .withColumn("cst_id", col("cst_id").cast("int"))
        .filter(col("cst_key").rlike("^AW\\d+$"))
        .withColumn("cst_firstname", trim(col("cst_firstname")))
        .withColumn("cst_lastname", trim(col("cst_lastname")))
        .withColumn("cst_marital_status", trim(upper(col("cst_marital_status"))))
        .na.replace(marital_status_map, subset=["cst_marital_status"])
        .na.fill({"cst_marital_status": "Unknown"}, subset=["cst_marital_status"])
        .withColumn("cst_gndr", trim(upper(col("cst_gndr"))))
        .na.replace(gender_map, subset=["cst_gndr"])
        .na.fill({"cst_gndr": "Unknown"}, subset=["cst_gndr"])
        .withColumn("cst_create_date", to_date(col("cst_create_date"), "yyyy-MM-dd"))
        .withColumn("DWH_create_date", current_timestamp())
        .drop(col("_rescued_data"))
    )

    return silver_df

# Silver table: cleaned and transformed product info
@dlt.table(
    name="silver_crm_data_prd_info",
    comment="Cleansed and transformed product data"
)
def silver_prd_info():
    # Read from Bronze DLT table
    df_prd_info = dlt.read("bronze_crm_data_prd_info")

    # Split prd_key into components
    split_col = split(col("prd_key"), "-")

    # Product line mapping
    product_map = {
        "R": "Road",
        "S": "Other Sales",
        "M": "Mountain",
        "T": "Touring"
    }

    # Window spec for ordering by start date per product
    window_spec = Window.partitionBy(col("prd_key")).orderBy(col("prd_start_dt"))

    # Transformations
    silver_prd_df = (
        df_prd_info
        .withColumn("prd_id", col("prd_id").cast("int"))
        .withColumn("cat_id", concat_ws("_", split_col.getItem(0), split_col.getItem(1)))
        .withColumn("prd_key", concat_ws("-", slice(split_col, 3, F.size(split_col) - 2)))
        .withColumn("prd_nm", col("prd_nm"))
        .withColumn("prd_cost", col("prd_cost").cast("int")).na.fill({"prd_cost": 0}, subset=["prd_cost"])
        .withColumn("prd_line", trim(upper(col("prd_line"))))
        .na.replace(product_map, subset=["prd_line"])
        .na.fill({"prd_line": "Unknown"}, subset=["prd_line"])
        .withColumn("prd_start_dt", to_date(col("prd_start_dt"), "yyyy-MM-dd"))
        .withColumn("prd_end_dt", lead(col("prd_start_dt"), 1).over(window_spec))
        .withColumn("prd_end_dt", coalesce(col("prd_end_dt"), current_timestamp().cast("date")))
        .withColumn("DWH_update_date", current_timestamp())
        .drop(col("_rescued_data"))
    )

    # Reorder columns: place 'cat_id' after 'prd_id'
    cols = silver_prd_df.columns
    cols.remove("cat_id")
    prd_id_idx = cols.index("prd_id")
    new_cols_order = cols[:prd_id_idx + 1] + ["cat_id"] + cols[prd_id_idx + 1:]
    silver_prd_df = silver_prd_df.select(new_cols_order)

    return silver_prd_df

    # Silver table: cleaned and transformed sales details
@dlt.table(
    name="silver_crm_data_sales_details",
    comment="Cleansed and transformed sales details"
)
def silver_sales_details():
    # Read from Bronze DLT table
    df_sales_details = dlt.read("bronze_crm_data_sales_details")

    # List of date columns to convert
    date_columns = ["sls_order_dt", "sls_ship_dt", "sls_due_dt"]

    # Apply transformations and data quality fixes
    silver_sales_df = (
        df_sales_details
        .withColumn("sls_ord_num", col("sls_ord_num"))
        .withColumn("sls_prd_key", col("sls_prd_key"))
        .withColumn("sls_cust_id", col("sls_cust_id").cast("int"))
        .withColumn("sls_sales", col("sls_sales").cast(FloatType()))
        # Correct sales amount
        .withColumn(
            "sls_sales",
            when(
                (col("sls_sales").isNull()) |
                (col("sls_sales") <= 0) |
                (col("sls_sales") != col("sls_quantity") * abs(col("sls_sales") / col("sls_quantity"))),
                col("sls_quantity") * abs(col("sls_price"))
            ).otherwise(col("sls_sales"))
        )
        .withColumn("sls_quantity", col("sls_quantity").cast(FloatType()))
        # Correct sales price
        .withColumn(
            "sls_price",
            when(
                ((col("sls_price").isNull()) | (col("sls_price") <= 0)) &
                (col("sls_quantity") != 0),
                col("sls_sales") / col("sls_quantity")
            ).otherwise(col("sls_price"))
        )
        .withColumn(
            "sls_price",
            when(
                (col("sls_sales") / col("sls_quantity") != col("sls_price")),
                col("sls_sales") / col("sls_quantity")
            ).otherwise(col("sls_price"))
        )
        .withColumn("DWH_update_date", current_timestamp())
        .drop(col("_rescued_data"))
    )

    # Convert date columns from 'yyyyMMdd' format to DateType
    for column_name in date_columns:
        silver_sales_df = silver_sales_df.withColumn(
            column_name,
            when(
                (length(col(column_name)) == 8) & (col(column_name) > 0),
                to_date(col(column_name), "yyyyMMdd")
            ).otherwise(lit(None).cast("date"))
        )

    return silver_sales_df


gender_map = {
    "M": "Male",
    "F": "Female",
    "FEMALE": "Female",
    "MALE": "Male",
    "": "Unknown"
}

# Silver table: cleaned and transformed CUST_AZ12 data
@dlt.table(
    name="silver_erp_data_cust_az12",
    comment="Cleansed and transformed CUST_AZ12 customer data"
)
def silver_cust_az12():
    # Read from Bronze DLT table
    df_cust_az12 = dlt.read("bronze_erp_data_cust_az12")

    # Transformations
    silver_cust_az12_df = (
        df_cust_az12
        .withColumn("CID", regexp_replace(col("CID"), "NAS", ""))
        .withColumnRenamed("CID", "cid")
        .withColumn(
            "BDATE",
            when(
                col("BDATE").cast("date") <= current_date(),
                col("BDATE").cast("date")
            ).otherwise(lit(None).cast("date"))
        )
        .withColumnRenamed("BDATE", "bdate")
        .withColumn("GEN", trim(upper(col("GEN"))))
        .na.replace(gender_map, subset=["GEN"])
        .na.fill("Unknown", subset=["GEN"])
        .withColumnRenamed("GEN", "gen")
        .withColumn("DWH_create_date", current_timestamp())
        .drop("_rescued_data")
    )

    return silver_cust_az12_df


# Silver table: cleaned LOC_A101 data
@dlt.table(
    name="silver_erp_data_loc_a101",
    comment="Cleansed and standardized LOC_A101 data"
)
def silver_loc_a101():
    # Read from Bronze DLT table
    df_loc_a101 = dlt.read("bronze_erp_data_LOC_A101")

    # Transformation and cleansing
    silver_loc_a101_df = (
        df_loc_a101
        .withColumn("CID", trim(regexp_replace(col("CID"), "-", "")))
        .withColumnRenamed("CID", "cid")
        .withColumn(
            "CNTRY",
            when(trim(upper(col("CNTRY"))) == "", "Unknown")
            .when(trim(upper(col("CNTRY"))) == "DE", "Germany")
            .when(trim(upper(col("CNTRY"))) == "US", "United States")
            .when(trim(upper(col("CNTRY"))) == "USA", "United States")
            .otherwise(col("CNTRY"))
        )
        .na.fill("Unknown", subset=["CNTRY"])
        .withColumnRenamed("CNTRY", "cntry")
        .withColumn("DWH_create_date", current_timestamp())
        .drop("_rescued_data")
    )

    return silver_loc_a101_df


# Silver table: cleaned PX_CAT_G1V2 data
@dlt.table(
    name="silver_erp_data_px_cat_g1v2",
    comment="Cleansed and standardized PX_CAT_G1V2 category data"
)
def silver_px_cat_g1v2():
    # Read from Bronze DLT table
    df_px_cat_g1v2 = dlt.read("bronze_erp_data_PX_CAT_G1V2")

    # Transformation
    silver_px_cat_g1v2_df = (
        df_px_cat_g1v2
        .withColumnRenamed("ID", "id")
        .withColumnRenamed("CAT_ID", "cat_id")
        .withColumnRenamed("CAT_NM", "cat_nm")
        .withColumnRenamed("MAINTENANCE", "maintenance")
        .withColumn("DWH_update_date", current_timestamp())
        .drop("_rescued_data")
    )

    return silver_px_cat_g1v2_df




@dlt.table(
name="gold_crm_erp_dim_customers",
comment="Dimension table for customers"
)
def dim_customers():
    df_cust_info = dlt.read("silver_crm_data_cust_info")
    df_cust_az12 = dlt.read("silver_erp_data_cust_az12")
    df_loc_a101 = dlt.read("silver_erp_data_loc_a101")

    joined_df = df_cust_info.alias("ci") \
        .join(df_cust_az12.alias("ca"), col("ci.cst_key") == col("ca.cid"), "left") \
        .join(df_loc_a101.alias("la"), col("ci.cst_key") == col("la.cid"), "left")

    window_distinct = Window.partitionBy("ci.cst_id").orderBy("ci.cst_id")
    df_with_rownum = joined_df.withColumn("row_num", row_number().over(window_distinct))
    latest_raw_df = df_with_rownum.filter(col("row_num") == 1)

    window_spec = Window.orderBy("ci.cst_id")

    dim_customers_df = latest_raw_df.select(
        row_number().over(window_spec).alias("customer_sk"),
        col("ci.cst_id").alias("customer_id"),
        col("ci.cst_key").alias("customer_number"),
        col("ci.cst_firstname").alias("first_name"),
        col("ci.cst_lastname").alias("last_name"),
        col("la.cntry").alias("country"),
        col("ci.cst_marital_status").alias("marital_status"),
        when(col("ci.cst_gndr") != 'Unknown', col("ci.cst_gndr"))
            .otherwise(coalesce(col("ca.gen"), lit('Unknown'))).alias("gender"),
        col("ca.bdate").alias("birth_date"),
        col("ci.cst_create_date").alias("created_date"),
        current_timestamp().alias("dwh_update_date")
    )

    return dim_customers_df


@dlt.table(
name="gold_crm_erp_dim_products",
comment="Dimension table for products"
)
def dim_products():
    df_prd_info = dlt.read("silver_crm_data_prd_info")
    df_px_cat_g1v2 = dlt.read("silver_erp_data_px_cat_g1v2")

    window_spec = Window.orderBy(col("prd_start_dt"), col("prd_key"))

    df_product_dim = (
        df_prd_info.alias("pn")
        .join(df_px_cat_g1v2.alias("pc"), col("pn.cat_id") == col("pc.id"), "left")
        .select(
            row_number().over(window_spec).alias("product_sk"),
            col("pn.prd_id").alias("product_id"),
            col("pn.prd_key").alias("product_number"),
            col("pn.prd_nm").alias("product_name"),
            col("pn.cat_id").alias("category_id"),
            col("pc.cat").alias("category"),
            col("pc.subcat").alias("subcategory"),
            col("pc.maintenance"),
            col("pn.prd_cost").alias("cost"),
            col("pn.prd_line").alias("product_line"),
            col("pn.prd_start_dt").alias("start_date"),
            current_timestamp().alias("dwh_update_date")
        )
    )

    return df_product_dim

@dlt.table(
    name="gold_crm_erp_fact_sales",
    comment="Fact table for sales"
)
def fact_sales():
    df_sales_details = dlt.read("silver_crm_data_sales_details")
    df_product_dim = dlt.read("gold_crm_erp_dim_products")
    df_customer_dim = dlt.read("gold_crm_erp_dim_customers")

    df_fact_sales = (
        df_sales_details.alias("sd")
        .join(df_product_dim.alias("pr"), col("sd.sls_prd_key") == col("pr.product_number"), "left")
        .join(df_customer_dim.alias("cr"), col("sd.sls_cust_id") == col("cr.customer_id"), "left")
        .select(
            col("sd.sls_ord_num").alias("order_number"),
            col("pr.product_sk"),
            col("cr.customer_sk"),
            col("sd.sls_order_dt").alias("order_date"),
            col("sd.sls_ship_dt").alias("ship_date"),
            col("sd.sls_due_dt").alias("due_date"),
            col("sd.sls_sales").alias("sales"),
            col("sd.sls_quantity").alias("quantity"),
            col("sd.sls_price").alias("price"),
            current_timestamp().alias("dwh_update_date")
        )
    )

    return df_fact_sales

