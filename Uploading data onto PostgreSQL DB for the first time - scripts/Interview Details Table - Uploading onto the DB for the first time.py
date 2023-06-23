###The script allows to transfer data from excel file to PostgreSQL for the first time, creating new table in a DB


###BEFORE USING - include username, password, DB name + path to additional files/tables

import sqlalchemy as sql
#import psycopg2
import pandas as pd

###connect to PostgreSQL table
engine = sql.create_engine('postgresql+psycopg2://USERNAME:PASSWORD@localhost/DBNAME')
table_to_import = pd.read_excel(r"C:\path\file_name.xlsx")
print (f'A connection to PostgreSQL was successfully established')

###creates a table in PostgreSQL database
metadata = sql.MetaData()

table_name = "interview_details"

table = sql.Table(
    table_name,
    metadata,
    sql.Column("application_id", sql.String),
    sql.Column("interview_stage", sql.String),
    sql.Column("interview_date", sql.Date),
    sql.Column("interview_time", sql.DateTime),
    sql.Column("no_of_interviewers", sql.Integer),
    sql.Column("progress_in_interviews_number", sql.Integer),
    sql.Column("progress_in_interviews_names", sql.String),
    sql.Column("first_last_interview", sql.String)
)

metadata.create_all(engine)
print (f'A table {table_name} was succesfully created')



        if counter % 100 == 0:
            print (f'Added {counter} rows with interview details')
            pass
        else:
            pass
        counter +=1

print (f'The whole process is finished!')