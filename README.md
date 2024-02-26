In this project an ETL process of banking data is demonstrated. The goal is to create a list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Furthermore, the dataset needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available as a CSV file. The processed information table is to be saved locally in a CSV format and as a database table. This procedure is performed through the banks_project.py file. 

The banking data are extracted from /https://en.wikipedia.org/wiki/List_of_largest_banks in the form of tabular information under the heading 'By market capitalization' and are then saved as a dataframe. Afterwards, they get transformed accordingly by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file. Finally, the outcome is saved in a new csv file with the usage of the function load_to_csv(). 

At the end of the file, the data are loaded onto a database called "Banks.db" and some queries are performed in order to showcase proper opperation. In every step of the process, we log everything that is being done in a log file with the function log_progress, so as to have a clear understanding of any possible problem. 


This project was developed as part of a graded project for the IBM Data Engineering Professional Certificate on Coursera. 
Source of the exchange rate csv: https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-
Coursera/labs/v2/exchange_rate.csv
