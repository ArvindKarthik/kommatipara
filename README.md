# KommatiPara Client Data Processor
### Overview
The KommatiPara Client Data Processor is a Python application built on PySpark for processing client data related to bitcoin trading. The goal of this application is to collate information from two separate datasets, clean and anonymize the data, and generate a new dataset suitable for targeted marketing efforts.

### Features
Filters clients based on their country (United Kingdom or Netherlands).
Removes credit card numbers from the financial details dataset.
Joins client information with financial details using a unique identifier.
Renames columns for improved readability to business users.

### Usage
The application accepts the following command-line arguments:

 -  clients_file: Path to the clients dataset file.
 -  financials_file: Path to the financial details dataset file.
 -  countries: List of countries to filter clients (e.g., "UK,NL")

### Example usage
```console
python path/main.py --clients_file path/dataset_one.csv --financials_file path/dataset_two.csv --countries United Kingdom,Netherlands
```
### Installation
- Clone this repository to your local machine.
- Ensure you have Python 3.8 installed.
- Install the required packages using the following command
  ```console
  pip install -r requirements.txt
  ```
### Project Structure
- src/main/main.py: Main script for data processing.
- src/main/utils.py: Utility functions for data filtering and renaming.
- client_data/: Directory to input data and store the output data.

### Logging
The application uses logging to provide insights into the data processing steps. Logs are saved to the logs/ directory with a rotating policy.

### Bonus Features
- Packaging the code into a source distribution file.
- Project is automated with Github actions
- log a file with a rotating policy
- Requirement file is present
- Documentation with docstrings using reStructuredText (reST) format.

### Contributors
ArvindKarthik Gunapiragasam arvindkarthik242@gmail.com






