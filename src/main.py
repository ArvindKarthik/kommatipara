import sys
import logging
from utils import filter_data, rename_columns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    """
    Main module for KommatiPara client data processing.

    Application should recieve following three arguments as input:
        clients_path (str): Path to the clients dataset file.
        financial_path (str): Path to the financial details dataset file.
        countries (str): Comma-separated list of countries to filter.
    
    Input:
        client_data/dataset_one.csv client_data/dataset_two.csv "United Kingdom,Netherlands"
    """

    logging.basicConfig(level=logging.INFO)

    # Initialize Spark session
    spark = SparkSession.builder.appName("KommatiPara").getOrCreate()

    # Get command line arguments
    clients_path = sys.argv[1]
    financial_path = sys.argv[2]
    countries = sys.argv[3].split(',')

    logging.info(f"Filtering clients from countries: {', '.join(countries)}")

    # Load the datasets
    clients_df = spark.read.csv(clients_path, header=True, inferSchema=True)
    financial_df = spark.read.csv(financial_path, header=True, inferSchema=True)

    # Use generic functions for filtering data and renaming
    clients_df = filter_data(clients_df, "country", countries)

    # Remove personal identifiable information (excluding emails)
    clients_df = clients_df.drop("first_name", "last_name")

    # Remove credit card number from financial details
    financial_df = financial_df.drop("cc_n")

    # Join datasets on id field
    final_df = clients_df.join(financial_df, "id")

    # Rename columns for readability
    column_mapping = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type"
    }

    final_df = rename_columns(final_df, column_mapping)

    # Show the final dataset
    final_df.show()

    # Save the final dataset as a CSV file
    final_df.write.csv("client_data/final_dataset.csv", header=True)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
