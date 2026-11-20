"""
Olist Data Engineering ETL Pipeline
-----------------------------------

Purpose:
    Reproduce the complete data engineering workflow developed and
    validated in the project notebooks.

Pipeline stages:
    1. Extract source datasets
    2. Clean and transform data
    3. Validate data quality
    4. Integrate datasets
    5. Engineer analytical features
    6. Perform final validation
    7. Export canonical CSV and Parquet outputs

Author: Hanson Olatunde
"""

from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------
# PROJECT PATHS
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"
OUTPUT_DATA_PATH = PROJECT_ROOT / "data" / "output"


# ---------------------------------------------------------
# PIPELINE START
# ---------------------------------------------------------
# ---------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------

def extract_data():
    """Load all raw CSV datasets into a dictionary."""

    print("\n" + "=" * 60)
    print("STAGE 1: EXTRACT")
    print("=" * 60)

    csv_files = sorted(RAW_DATA_PATH.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in: {RAW_DATA_PATH}"
        )

    datasets = {}

    for file_path in csv_files:
        dataset_name = file_path.stem

        df = pd.read_csv(file_path)

        datasets[dataset_name] = df

        print(
            f"Loaded {dataset_name:<40} "
            f"{len(df):>10,} rows x {len(df.columns):>2} columns"
        )

    print(f"\nTotal datasets loaded: {len(datasets)}")

    return datasets

# ---------------------------------------------------------
# TRANSFORM — DATA CLEANING
# ---------------------------------------------------------

def clean_data(datasets):
    """Clean and standardise the extracted source datasets."""

    print("\n" + "=" * 60)
    print("STAGE 2: CLEAN & TRANSFORM")
    print("=" * 60)

    # Create independent copies so the raw extracted
    # DataFrames remain unchanged.
    cleaned_datasets = {
        name: df.copy()
        for name, df in datasets.items()
    }

    print(
        f"Created working copies of "
        f"{len(cleaned_datasets)} datasets."
    )

    # -----------------------------------------------------
    # Remove exact geolocation duplicates
    # -----------------------------------------------------

    geo_df = cleaned_datasets["geolocation_dataset"]

    rows_before = len(geo_df)

    geo_df = geo_df.drop_duplicates().copy()

    rows_after = len(geo_df)
    rows_removed = rows_before - rows_after

    cleaned_datasets["geolocation_dataset"] = geo_df

    print("\nGeolocation deduplication:")
    print(f"Rows before : {rows_before:,}")
    print(f"Rows after  : {rows_after:,}")
    print(f"Removed     : {rows_removed:,}")

    # Validate expected result
    if rows_after != 738_332:
        raise ValueError(
            f"Unexpected geolocation row count: {rows_after:,}. "
            "Expected 738,332."
        )
    # -----------------------------------------------------
    # Standardise date and timestamp columns
    # -----------------------------------------------------

    date_columns = {
        "closed_deals_dataset": [
            "won_date",
        ],
        "marketing_qualified_leads_dataset": [
            "first_contact_date",
        ],
        "orders_dataset": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "order_items_dataset": [
            "shipping_limit_date",
        ],
        "order_reviews_dataset": [
            "review_creation_date",
            "review_answer_timestamp",
        ],
    }

    print("\nDate/time standardisation:")

    for dataset_name, columns in date_columns.items():
        df = cleaned_datasets[dataset_name]

        for column in columns:
            df[column] = pd.to_datetime(
                df[column],
                errors="coerce"
            )

            print(
                f"{dataset_name:<35} "
                f"{column:<35} "
                f"{str(df[column].dtype)}"
            )
    expected_datetime_columns = 10
    converted_datetime_columns = sum(
        len(columns)
        for columns in date_columns.values()
     )

    if converted_datetime_columns != expected_datetime_columns:
        raise ValueError(
            "Unexpected number of configured datetime columns."
         )
    print(
         f"\nDatetime columns standardised: "
        f"{converted_datetime_columns}"
     )    
     # -----------------------------------------------------
    # Clean leading and trailing whitespace in text values
    # -----------------------------------------------------

    text_values_changed = 0

    for dataset_name, df in cleaned_datasets.items():

        for column in df.columns:

            if pd.api.types.is_string_dtype(df[column]):

                before = df[column].copy()

                df[column] = df[column].apply(
                    lambda x: x.strip() if isinstance(x, str) else x
                )

                changed_count = (
                    before.fillna("") != df[column].fillna("")
                ).sum()

                text_values_changed += changed_count

    print(
        f"\nText values cleaned: "
        f"{text_values_changed:,}"
    )  
    if text_values_changed != 11_450:
        raise ValueError(
            f"Unexpected number of text values cleaned: "
            f"{text_values_changed:,}. Expected 11,450."
        ) 
     # -----------------------------------------------------
    # Create data-quality exception flags
    # -----------------------------------------------------

    # 1. Delivered orders with missing customer delivery date
    orders_df = cleaned_datasets["orders_dataset"]

    orders_df["dq_missing_delivery_date"] = (
        orders_df["order_status"].eq("delivered")
        & orders_df["order_delivered_customer_date"].isna()
    )

    # 2. Payment records with zero or negative payment value
    payments_df = cleaned_datasets["order_payments_dataset"]

    payments_df["dq_zero_payment"] = (
        payments_df["payment_value"] <= 0
    )

    # 3. Products with missing or non-positive weight
    products_df = cleaned_datasets["products_dataset"]

    products_df["dq_invalid_weight"] = (
        products_df["product_weight_g"] <= 0
    )

    print("\nData-quality exception flags:")
    print(
        f"Missing delivered dates : "
        f"{orders_df['dq_missing_delivery_date'].sum():,}"
    )
    print(
        f"Zero/negative payments  : "
        f"{payments_df['dq_zero_payment'].sum():,}"
    )
    print(
        f"Invalid product weights : "
        f"{products_df['dq_invalid_weight'].sum():,}"
    )
    if orders_df["dq_missing_delivery_date"].sum() != 8:
         raise ValueError(
            "Unexpected delivered-order missing-date exception count."
          )
    if payments_df["dq_zero_payment"].sum() != 9:
        raise ValueError(
            "Unexpected zero-payment exception count."
        )
    if products_df["dq_invalid_weight"].sum() != 4:
        raise ValueError(
          "Unexpected invalid-product-weight exception count."
        )
        # -----------------------------------------------------
    # Final cleaning validation
    # -----------------------------------------------------

    cleaning_checks = {
        "dataset_count": len(cleaned_datasets) == 11,
        "geolocation_rows": (
            len(cleaned_datasets["geolocation_dataset"]) == 738_332
        ),
        "missing_delivery_date_flags": (
            cleaned_datasets["orders_dataset"][
                "dq_missing_delivery_date"
            ].sum() == 8
        ),
        "zero_payment_flags": (
            cleaned_datasets["order_payments_dataset"][
                "dq_zero_payment"
            ].sum() == 9
        ),
        "invalid_weight_flags": (
            cleaned_datasets["products_dataset"][
                "dq_invalid_weight"
            ].sum() == 4
        ),
    }

    if not all(cleaning_checks.values()):
        failed_checks = [
            check
            for check, passed in cleaning_checks.items()
            if not passed
        ]

        raise ValueError(
            f"Cleaning validation failed: {failed_checks}"
        )

    print("\nFinal cleaning validation: PASSED")
    return cleaned_datasets
# ---------------------------------------------------------
# INTEGRATE
# ---------------------------------------------------------

def integrate_data(cleaned_datasets):
    """Integrate cleaned datasets at order-item grain."""

    print("\n" + "=" * 60)
    print("STAGE 3: INTEGRATION")
    print("=" * 60)

    # Establish canonical grain: one row per order-item
    integrated_df = cleaned_datasets[
        "order_items_dataset"
    ].copy()

    expected_rows = len(integrated_df)

    duplicate_keys = integrated_df.duplicated(
        subset=["order_id", "order_item_id"]
    ).sum()

    if duplicate_keys != 0:
        raise ValueError(
            f"Order-item grain contains "
            f"{duplicate_keys:,} duplicate keys."
        )

    print(f"Base order-item rows : {expected_rows:,}")
    print(f"Duplicate grain keys : {duplicate_keys:,}")
    # -----------------------------------------------------
    # Join order information
    # -----------------------------------------------------

    orders_df = cleaned_datasets["orders_dataset"]

    rows_before = len(integrated_df)

    integrated_df = integrated_df.merge(
        orders_df,
        on="order_id",
        how="left",
        validate="many_to_one",
        indicator=True
    )

    rows_after = len(integrated_df)

    matched_rows = (integrated_df["_merge"] == "both").sum()
    unmatched_rows = (integrated_df["_merge"] == "left_only").sum()

    print("\nOrders join:")
    print(f"Rows before       : {rows_before:,}")
    print(f"Rows after        : {rows_after:,}")
    print(f"Matched rows      : {matched_rows:,}")
    print(f"Unmatched rows    : {unmatched_rows:,}")

    # Validate row-count preservation
    if rows_after != expected_rows:
        raise ValueError(
            "Orders join changed the protected order-item grain."
        )

    # All order-items are expected to have a matching order
    if unmatched_rows != 0:
        raise ValueError(
            f"Orders join produced {unmatched_rows:,} unmatched rows."
        )

    # Remove temporary merge indicator
    integrated_df = integrated_df.drop(columns="_merge")
    # -----------------------------------------------------
    # Join customer information
    # -----------------------------------------------------

    customers_df = cleaned_datasets["customers_dataset"]

    rows_before = len(integrated_df)

    integrated_df = integrated_df.merge(
        customers_df,
        on="customer_id",
        how="left",
        validate="many_to_one",
        indicator=True
    )

    rows_after = len(integrated_df)

    matched_rows = (integrated_df["_merge"] == "both").sum()
    unmatched_rows = (integrated_df["_merge"] == "left_only").sum()

    print("\nCustomers join:")
    print(f"Rows before       : {rows_before:,}")
    print(f"Rows after        : {rows_after:,}")
    print(f"Matched rows      : {matched_rows:,}")
    print(f"Unmatched rows    : {unmatched_rows:,}")

    if rows_after != expected_rows:
        raise ValueError(
            "Customers join changed the protected order-item grain."
        )

    if unmatched_rows != 0:
        raise ValueError(
            f"Customers join produced {unmatched_rows:,} unmatched rows."
        )

    integrated_df = integrated_df.drop(columns="_merge")
    # -----------------------------------------------------
    # Join product information
    # -----------------------------------------------------

    products_df = cleaned_datasets["products_dataset"]

    rows_before = len(integrated_df)

    integrated_df = integrated_df.merge(
        products_df,
        on="product_id",
        how="left",
        validate="many_to_one",
        indicator=True
    )

    rows_after = len(integrated_df)

    matched_rows = (integrated_df["_merge"] == "both").sum()
    unmatched_rows = (integrated_df["_merge"] == "left_only").sum()

    print("\nProducts join:")
    print(f"Rows before       : {rows_before:,}")
    print(f"Rows after        : {rows_after:,}")
    print(f"Matched rows      : {matched_rows:,}")
    print(f"Unmatched rows    : {unmatched_rows:,}")

    if rows_after != expected_rows:
        raise ValueError(
            "Products join changed the protected order-item grain."
        )

    if unmatched_rows != 0:
        raise ValueError(
            f"Products join produced {unmatched_rows:,} unmatched rows."
        )

    integrated_df = integrated_df.drop(columns="_merge")
        # -----------------------------------------------------
    # Join product category translation
    # -----------------------------------------------------

    category_df = cleaned_datasets[
        "product_category_name_translation"
    ]

    rows_before = len(integrated_df)

    integrated_df = integrated_df.merge(
        category_df,
        on="product_category_name",
        how="left",
        validate="many_to_one",
        indicator=True
    )

    rows_after = len(integrated_df)

    matched_rows = (integrated_df["_merge"] == "both").sum()
    unmatched_rows = (integrated_df["_merge"] == "left_only").sum()

    print("\nProduct category translation join:")
    print(f"Rows before       : {rows_before:,}")
    print(f"Rows after        : {rows_after:,}")
    print(f"Matched rows      : {matched_rows:,}")
    print(f"Unmatched rows    : {unmatched_rows:,}")

    # Protect the order-item grain
    if rows_after != expected_rows:
        raise ValueError(
            "Category translation join changed "
            "the protected order-item grain."
        )

    # Data-quality flag:
    # category translation was not available
    integrated_df["dq_missing_category_translation"] = (
        integrated_df["_merge"] == "left_only"
    )
    # Classify category translation exceptions
    missing_source_category = (
        integrated_df["product_category_name"].isna()
    ).sum()

    category_present_no_translation = (
        (integrated_df["_merge"] == "left_only")
        & integrated_df["product_category_name"].notna()
    ).sum()

    print("\nCategory translation exceptions:")
    print(
        f"Missing source category       : "
        f"{missing_source_category:,}"
    )
    print(
        f"Category present, untranslated: "
        f"{category_present_no_translation:,}"
    )

    if missing_source_category != 1_603:
        raise ValueError(
            "Unexpected missing source-category count."
        )

    if category_present_no_translation != 24:
        raise ValueError(
            "Unexpected untranslated-category count."
        )
    integrated_df = integrated_df.drop(columns="_merge")
        # -----------------------------------------------------
    # Join seller information
    # -----------------------------------------------------

    sellers_df = cleaned_datasets["sellers_dataset"]

    rows_before = len(integrated_df)

    integrated_df = integrated_df.merge(
        sellers_df,
        on="seller_id",
        how="left",
        validate="many_to_one",
        indicator=True
    )

    rows_after = len(integrated_df)

    matched_rows = (integrated_df["_merge"] == "both").sum()
    unmatched_rows = (integrated_df["_merge"] == "left_only").sum()

    print("\nSellers join:")
    print(f"Rows before       : {rows_before:,}")
    print(f"Rows after        : {rows_after:,}")
    print(f"Matched rows      : {matched_rows:,}")
    print(f"Unmatched rows    : {unmatched_rows:,}")

    if rows_after != expected_rows:
        raise ValueError(
            "Sellers join changed the protected order-item grain."
        )

    if unmatched_rows != 0:
        raise ValueError(
            f"Sellers join produced {unmatched_rows:,} unmatched rows."
        )

    integrated_df = integrated_df.drop(columns="_merge")
        # -----------------------------------------------------
    # Aggregate payment information to order grain
    # -----------------------------------------------------

    payments_df = cleaned_datasets["order_payments_dataset"]

    payment_summary = (
        payments_df
        .groupby("order_id")
        .agg(
            total_payment_value=("payment_value", "sum"),
            payment_record_count=("payment_value", "size"),
            payment_type_count=("payment_type", "nunique"),
            max_payment_installments=("payment_installments", "max"),
            payment_types=(
                "payment_type",
                lambda x: ", ".join(sorted(set(x.dropna())))
            )
        )
        .reset_index()
    )

    duplicate_payment_orders = payment_summary[
        "order_id"
    ].duplicated().sum()

    print("\nPayment aggregation:")
    print(f"Original payment rows   : {len(payments_df):,}")
    print(f"Aggregated payment rows : {len(payment_summary):,}")
    print(f"Duplicate order IDs     : {duplicate_payment_orders:,}")

    if duplicate_payment_orders != 0:
        raise ValueError(
            "Payment aggregation did not produce unique order IDs."
        )
       # -----------------------------------------------------
    # Join aggregated payment information
    # -----------------------------------------------------

    rows_before = len(integrated_df)

    integrated_df = integrated_df.merge(
        payment_summary,
        on="order_id",
        how="left",
        validate="many_to_one",
        indicator=True
    )

    rows_after = len(integrated_df)

    matched_rows = (integrated_df["_merge"] == "both").sum()
    unmatched_rows = (integrated_df["_merge"] == "left_only").sum()

    print("\nPayments join:")
    print(f"Rows before       : {rows_before:,}")
    print(f"Rows after        : {rows_after:,}")
    print(f"Matched rows      : {matched_rows:,}")
    print(f"Unmatched rows    : {unmatched_rows:,}")

    if rows_after != expected_rows:
        raise ValueError(
            "Payments join changed the protected order-item grain."
        )

    # Preserve legitimate missing-payment exceptions
    integrated_df["dq_missing_payment"] = (
        integrated_df["_merge"] == "left_only"
    )
    integrated_df = integrated_df.drop(columns="_merge") 
    # -----------------------------------------------------
    # Aggregate review information to order grain
    # -----------------------------------------------------

    reviews_df = cleaned_datasets["order_reviews_dataset"]

    review_summary = (
        reviews_df
        .groupby("order_id")
        .agg(
            review_count=("review_id", "size"),
            unique_review_count=("review_id", "nunique"),
            average_review_score=("review_score", "mean"),
            min_review_score=("review_score", "min"),
            max_review_score=("review_score", "max"),
            reviews_with_comments=(
                "review_comment_message",
                lambda x: x.notna().sum()
            )
        )
        .reset_index()
    )

    duplicate_review_orders = review_summary[
        "order_id"
    ].duplicated().sum()

    print("\nReview aggregation:")
    print(f"Original review rows   : {len(reviews_df):,}")
    print(f"Aggregated review rows : {len(review_summary):,}")
    print(f"Duplicate order IDs    : {duplicate_review_orders:,}")

    if duplicate_review_orders != 0:
        raise ValueError(
            "Review aggregation did not produce unique order IDs."
        )
        # -----------------------------------------------------
    # Join aggregated review information
    # -----------------------------------------------------

    rows_before = len(integrated_df)

    integrated_df = integrated_df.merge(
        review_summary,
        on="order_id",
        how="left",
        validate="many_to_one",
        indicator=True
    )

    rows_after = len(integrated_df)

    matched_rows = (integrated_df["_merge"] == "both").sum()
    unmatched_rows = (integrated_df["_merge"] == "left_only").sum()

    print("\nReviews join:")
    print(f"Rows before       : {rows_before:,}")
    print(f"Rows after        : {rows_after:,}")
    print(f"Matched rows      : {matched_rows:,}")
    print(f"Unmatched rows    : {unmatched_rows:,}")

    if rows_after != expected_rows:
        raise ValueError(
            "Reviews join changed the protected order-item grain."
        )

    # Preserve legitimate orders without review information
    integrated_df["dq_missing_review"] = (
        integrated_df["_merge"] == "left_only"
    )

    integrated_df = integrated_df.drop(columns="_merge")
        # -----------------------------------------------------
    # Create geography lookup at ZIP-code prefix grain
    # -----------------------------------------------------

    geo_df = cleaned_datasets["geolocation_dataset"]

    geo_lookup = (
        geo_df
        .groupby("geolocation_zip_code_prefix")
        .agg(
            geolocation_lat=("geolocation_lat", "median"),
            geolocation_lng=("geolocation_lng", "median"),
            geolocation_city=("geolocation_city", "first"),
            geolocation_state=("geolocation_state", "first")
        )
        .reset_index()
    )

    duplicate_zip_keys = geo_lookup[
        "geolocation_zip_code_prefix"
    ].duplicated().sum()

    print("\nGeography lookup:")
    print(f"Original geolocation rows : {len(geo_df):,}")
    print(f"Aggregated lookup rows     : {len(geo_lookup):,}")
    print(
        f"Unique ZIP-code prefixes   : "
        f"{geo_lookup['geolocation_zip_code_prefix'].nunique():,}"
    )
    print(f"Duplicate ZIP keys         : {duplicate_zip_keys:,}")

    if duplicate_zip_keys != 0:
        raise ValueError(
            "Geography lookup contains duplicate ZIP-code prefixes."
        )

        # -----------------------------------------------------
    # Create customer geography lookup
    # -----------------------------------------------------

    customer_geo = (
        cleaned_datasets["customers_dataset"]
        .merge(
            geo_lookup,
            left_on="customer_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
            how="left",
            validate="many_to_one"
        )
        .rename(
            columns={
                "geolocation_lat": "customer_geo_lat",
                "geolocation_lng": "customer_geo_lng",
                "geolocation_city": "customer_geo_city",
                "geolocation_state": "customer_geo_state",
            }
        )
    )

    customer_geo = customer_geo[
        [
            "customer_id",
            "customer_geo_lat",
            "customer_geo_lng",
            "customer_geo_city",
            "customer_geo_state",
        ]
    ]

    if customer_geo["customer_id"].duplicated().any():
        raise ValueError(
            "Customer geography lookup contains duplicate customer IDs."
        )

    print(
        f"\nCustomer geography missing coordinates: "
        f"{customer_geo['customer_geo_lat'].isna().sum():,}"
    )
        # -----------------------------------------------------
    # Create seller geography lookup
    # -----------------------------------------------------

    seller_geo = (
        cleaned_datasets["sellers_dataset"]
        .merge(
            geo_lookup,
            left_on="seller_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
            how="left",
            validate="many_to_one"
        )
        .rename(
            columns={
                "geolocation_lat": "seller_geo_lat",
                "geolocation_lng": "seller_geo_lng",
                "geolocation_city": "seller_geo_city",
                "geolocation_state": "seller_geo_state",
            }
        )
    )

    seller_geo = seller_geo[
        [
            "seller_id",
            "seller_geo_lat",
            "seller_geo_lng",
            "seller_geo_city",
            "seller_geo_state",
        ]
    ]

    if seller_geo["seller_id"].duplicated().any():
        raise ValueError(
            "Seller geography lookup contains duplicate seller IDs."
        )

    print(
        f"Seller geography missing coordinates: "
        f"{seller_geo['seller_geo_lat'].isna().sum():,}"
    )
        # -----------------------------------------------------
    # Join customer geography
    # -----------------------------------------------------

    rows_before = len(integrated_df)

    integrated_df = integrated_df.merge(
        customer_geo,
        on="customer_id",
        how="left",
        validate="many_to_one"
    )

    rows_after = len(integrated_df)

    if rows_after != expected_rows:
        raise ValueError(
            "Customer geography join changed "
            "the protected order-item grain."
        )

    print("\nCustomer geography join:")
    print(f"Rows before : {rows_before:,}")
    print(f"Rows after  : {rows_after:,}")
        # -----------------------------------------------------
    # Join seller geography
    # -----------------------------------------------------

    rows_before = len(integrated_df)

    integrated_df = integrated_df.merge(
        seller_geo,
        on="seller_id",
        how="left",
        validate="many_to_one"
    )

    rows_after = len(integrated_df)

    if rows_after != expected_rows:
        raise ValueError(
            "Seller geography join changed "
            "the protected order-item grain."
        )

    print("\nSeller geography join:")
    print(f"Rows before : {rows_before:,}")
    print(f"Rows after  : {rows_after:,}")
    return integrated_df
# ---------------------------------------------------------
# FEATURE ENGINEERING
# ---------------------------------------------------------

def engineer_features(integrated_df):
    """Create analytical features from the integrated dataset."""

    print("\n" + "=" * 60)
    print("STAGE 4: FEATURE ENGINEERING")
    print("=" * 60)

    df = integrated_df.copy()

    rows_before = len(df)

    # -----------------------------------------------------
    # Order-item total value
    # -----------------------------------------------------

    df["item_total_value"] = (
        df["price"] + df["freight_value"]
    )

    # -----------------------------------------------------
    # Delivery duration
    # -----------------------------------------------------

    df["delivery_days"] = (
        df["order_delivered_customer_date"]
        - df["order_purchase_timestamp"]
    ).dt.total_seconds() / 86_400

    # -----------------------------------------------------
    # Delivery performance against estimate
    # -----------------------------------------------------

    df["delivery_vs_estimate_days"] = (
        df["order_delivered_customer_date"]
        - df["order_estimated_delivery_date"]
    ).dt.total_seconds() / 86_400

    df["delivery_performance"] = np.where(
        df["delivery_vs_estimate_days"].isna(),
        pd.NA,
        np.where(
            df["delivery_vs_estimate_days"] <= 0,
            "On or before estimate",
            "Late"
        )
    )
    # -----------------------------------------------------
    # Customer-to-seller distance using Haversine formula
    # -----------------------------------------------------

    earth_radius_km = 6371.0

    customer_lat = np.radians(df["customer_geo_lat"])
    customer_lng = np.radians(df["customer_geo_lng"])
    seller_lat = np.radians(df["seller_geo_lat"])
    seller_lng = np.radians(df["seller_geo_lng"])

    delta_lat = seller_lat - customer_lat
    delta_lng = seller_lng - customer_lng

    haversine_a = (
        np.sin(delta_lat / 2) ** 2
        + np.cos(customer_lat)
        * np.cos(seller_lat)
        * np.sin(delta_lng / 2) ** 2
    )

    haversine_c = 2 * np.arcsin(
        np.sqrt(haversine_a)
    )

    df["customer_seller_distance_km"] = (
        earth_radius_km * haversine_c
    )
    rows_after = len(df)

    if rows_after != rows_before:
        raise ValueError(
            "Feature engineering changed the row count."
        )

    print(f"Rows after feature creation : {rows_after:,}")
    distances_calculated = (
        df["customer_seller_distance_km"].notna().sum()
    )

    distances_unavailable = (
        df["customer_seller_distance_km"].isna().sum()
    )

    print("\nCustomer-seller distance:")
    print(
        f"Distances calculated  : "
        f"{distances_calculated:,}"
    )
    print(
        f"Distances unavailable : "
        f"{distances_unavailable:,}"
    )
    print(
        f"Median distance       : "
        f"{df['customer_seller_distance_km'].median():,.2f} km"
    )
    print(
        f"Average distance      : "
        f"{df['customer_seller_distance_km'].mean():,.2f} km"
    )
    print(
        f"Maximum distance      : "
        f"{df['customer_seller_distance_km'].max():,.2f} km"
    )

    if distances_calculated != 112_096:
        raise ValueError(
            "Unexpected number of calculated distances."
        )

    if distances_unavailable != 554:
        raise ValueError(
            "Unexpected number of unavailable distances."
        )
    return df
# ---------------------------------------------------------
# FINAL VALIDATION
# ---------------------------------------------------------

def validate_final_dataset(analytical_df):
    """Validate the final analytical dataset before export."""

    print("\n" + "=" * 60)
    print("STAGE 5: FINAL VALIDATION")
    print("=" * 60)

    expected_rows = 112_650

    actual_rows = len(analytical_df)

    unique_order_items = (
        analytical_df[
            ["order_id", "order_item_id"]
        ]
        .drop_duplicates()
        .shape[0]
    )

    duplicate_order_items = (
        analytical_df.duplicated(
            subset=["order_id", "order_item_id"]
        ).sum()
    )

    validation_checks = {
        "Expected row count":
            actual_rows == expected_rows,

        "Unique order-item grain":
            unique_order_items == expected_rows,

        "No duplicate order-item keys":
            duplicate_order_items == 0,

        "Required item value feature":
            "item_total_value" in analytical_df.columns,

        "Required delivery feature":
            "delivery_performance" in analytical_df.columns,

        "Required distance feature":
            "customer_seller_distance_km"
            in analytical_df.columns,
    }

    print(f"Final rows                 : {actual_rows:,}")
    print(f"Unique order-item keys     : {unique_order_items:,}")
    print(f"Duplicate order-item keys  : {duplicate_order_items:,}")

    print("\nValidation checks:")

    for check_name, passed in validation_checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"{check_name:<35} {status}")

    if not all(validation_checks.values()):
        failed_checks = [
            name
            for name, passed in validation_checks.items()
            if not passed
        ]

        raise ValueError(
            f"Final validation failed: {failed_checks}"
        )

    print("\nFINAL DATASET VALIDATION: PASSED")
 # ---------------------------------------------------------
# EXPORT
# ---------------------------------------------------------

def export_data(analytical_df):
    """Export the validated analytical dataset."""

    print("\n" + "=" * 60)
    print("STAGE 6: EXPORT")
    print("=" * 60)

    OUTPUT_DATA_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    csv_path = (
        OUTPUT_DATA_PATH
        / "olist_integrated_dataset.csv"
    )

    parquet_path = (
        OUTPUT_DATA_PATH
        / "olist_integrated_dataset.parquet"
    )

    # Export CSV
    analytical_df.to_csv(
        csv_path,
        index=False
    )

    # Export Parquet
    analytical_df.to_parquet(
        parquet_path,
        index=False
    )

    print(f"CSV exported     : {csv_path}")
    print(f"Parquet exported : {parquet_path}")
         # -----------------------------------------------------
    # Validate exported files
    # -----------------------------------------------------

    if not csv_path.exists():
        raise FileNotFoundError(
            "CSV export was not created."
        )

    if not parquet_path.exists():
        raise FileNotFoundError(
            "Parquet export was not created."
        )

    parquet_check = pd.read_parquet(parquet_path)

    if len(parquet_check) != len(analytical_df):
        raise ValueError(
            "Parquet export row count does not match "
            "the analytical dataset."
        )

    if len(parquet_check.columns) != len(
        analytical_df.columns
    ):
        raise ValueError(
            "Parquet export column count does not match "
            "the analytical dataset."
        )

    print(f"\nExported rows    : {len(analytical_df):,}")
    print(
        f"Exported columns : "
        f"{len(analytical_df.columns):,}"
    )
    print("\nEXPORT VALIDATION: PASSED")  
    
def main():
    """Run the complete Olist ETL pipeline."""

    print("=" * 60)
    print("OLIST DATA ENGINEERING ETL PIPELINE")
    print("=" * 60)

    print(f"\nProject root : {PROJECT_ROOT}")
    print(f"Raw data     : {RAW_DATA_PATH}")
    print(f"Output data  : {OUTPUT_DATA_PATH}")

    OUTPUT_DATA_PATH.mkdir(parents=True, exist_ok=True)

    print("\nPipeline environment initialised successfully.")
    
    datasets = extract_data()

    if len(datasets) != 11:
       raise ValueError(
           f"Expected 11 source datasets, but found {len(datasets)}."
      )

    print("\nExtract stage completed successfully.")

    cleaned_datasets = clean_data(datasets)

    if len(cleaned_datasets) != len(datasets):
     raise ValueError(
         "Cleaning stage did not return all source datasets."
      )

    print("\nCleaning workspace initialised successfully.")
    integrated_df = integrate_data(cleaned_datasets)
    print(
             f"\nIntegration workspace initialised with "
             f"{len(integrated_df):,} rows."
    )
    analytical_df = engineer_features(integrated_df)

    print(
        f"\nFeature engineering completed with "
        f"{len(analytical_df):,} rows."
    )
    validate_final_dataset(analytical_df)
    export_data(analytical_df)
   
if __name__ == "__main__":
    main()