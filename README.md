# ETL-processes
Various scripts (created with Python, Pandas, SQLAlchemy) that allows to load data from various sources, modify them add/add new values and then load them to PostgreSQL DB, using SQLAlchemy.
Later on, the data transformed with these scripts are being used to analyse them in BI tools, such as PowerBI/Excel

:rocket: Tech stack:
**Python 3.1x |||
Pandas |||
SQLAlchemy |||
PostgreSQL DB**


**The project contains 2 folders:**

First directory: Uploading data onto PostgreSQL DB for the first time - scripts
In the directory, you can find scripts that I used in my test environment to create tables in PostgreSQL DB for the first time, and then transfer data from different sources (like xlsx files) onto that DB.

**Updating PostgreSQL DB - scripts**

The data is not still, so I have to update them on a regular basis. To do so, I prepared a bunch of scripts that do that work for me - they get data from the system, change them and add additional values, compare what should be added onto the DB and do so.
These scripts are usually much longer then the scripts in the previous directory, as they have to check multiple conditions and do some calculations if needed.
