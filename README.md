# python-oracle-db-updater

This Python script updates a single column in an Oracle DB table with values from a CSV file. The script iterates over each record in the CSV file and performs an UPDATE statement on the DB. The CSV file will need to have at least 2 columns: an ID column used to find the DB records to update, and a column containing the new values to update the DB records with.

Ensure all configurable variables have been filled in before running the script.
