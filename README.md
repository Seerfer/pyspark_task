# Pyspark processing datasets app
This app is created to process two csv files. It contains following operations:
* Droping columns with personal operations
* Renaming columns for being more readable
* Performing a join operation by client_identifier
* Filterring files by given countries

At the end app is saving file with processed data at **client_data/output.csv**

## Installation
### Spark install
Application is basing on Spark and need full Spark installation to work. Here is a simple [tutorial](https://phoenixnap.com/kb/install-spark-on-windows-10) to do this in Windows OS.

### Install requirements pip package
Use pip command to install requirements
```bash
pip install -r requirements.txt
```

## Usage
### Arguments
Application is taking 3 arguments. We can see list of them with description by using `python main.py -h`. List of arguments:
* `--d1` - path to dataset with personal informations
* `--d2` - path to dataset with account informations
* `--c` - list of countries to filter divided by space. Countries that contains two words (for example **United Kingdom**) should be given without space but with upper case at beginning of next word(for example **UnitedStates**, not **United States**)

### Example usage
```bash
python main.py --d1 dataset_one.csv --d2 dataset_two.csv --c Netherlands UnitedKingdom
```