import sys
import logging
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

if __name__ == "__main__":
    main()
