### KommatiPara Client Data Processor
## Overview
The KommatiPara Client Data Processor is a Python application built on PySpark for processing client data related to bitcoin trading. The goal of this application is to collate information from two separate datasets, clean and anonymize the data, and generate a new dataset suitable for targeted marketing efforts.

## Features
Filters clients based on their country (United Kingdom or Netherlands).
Removes credit card numbers from the financial details dataset.
Joins client information with financial details using a unique identifier.
Renames columns for improved readability to business users.

## Usage
The application accepts the following command-line arguments:

 --clients_file: Path to the clients dataset file.
 --financials_file: Path to the financial details dataset file.
 --countries: List of countries to filter clients (e.g., "UK,NL").

## Example usage:
 python path/main.py --clients_file path/dataset_one.csv --financials_file path/dataset_two.csv --countries United Kingdom,Netherlands
