import sys
import logging
from logging.handlers import RotatingFileHandler
from utils import filter_data, rename_columns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def filter_clients(spark, clients_path, financial_path, countries):
    # Load the datasets
    clients_df = spark.read.csv(clients_path, header=True, inferSchema=True)
    financial_df = spark.read.csv(financial_path, header=True, inferSchema=True)

    # Filter clients from specified countries
    clients_df = clients_df.filter(clients_df.country.isin(countries))

    # Remove personal identifiable information (excluding emails)
    clients_df = clients_df.drop("first_name", "last_name")

    # Remove credit card number from financial details
    financial_df = financial_df.drop("cc_n")

    # Join datasets on id field
    final_df = clients_df.join(financial_df, "id")

    # Rename columns for readability
    final_df = final_df.withColumnRenamed("id", "client_identifier") \
					   .withColumnRenamed("btc_a", "bitcoin_address") \
					   .withColumnRenamed("cc_t", "credit_card_type")

    return final_df

def main():
    logging.basicConfig(level=logging.INFO)

    # Initialize Spark session
    spark = SparkSession.builder.appName("KommatiPara").getOrCreate()

    # Get dataset path
    clients_path = sys.argv[1]
    financial_path = sys.argv[2]
    countries = sys.argv[3].split(',')

    logging.info(f"Filtering clients from countries: {', '.join(countries)}")

    # Filter and process clients
    final_df = filter_clients(spark, clients_path, financial_path, countries)

    # Show the final dataset
    final_df.show()

    # Save the final dataset as a CSV file
    final_df.write.csv("client_data/final_dataset.csv", header=True)

    # Stop the Spark session
    spark.stop()

def setup_logger(log_file):
    """
    setup_logger function configures a Rotating file policy to save log messages.
    Log file will rotate when it reaches a size of 1MB
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

if __name__ == "__main__":
    log_file = "kommatipara.log"
    setup_logger(log_file)
    main()


    
