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

###Rename the columns in table to import, so that their names match what we have in PostgreSQL DB

table_to_import = table_to_import.rename(columns=
                                        {"Application_Encrypted_ID": "application_id",
                                         "Stage_at_the_Interview": "interview_stage",
                                         "Interview_Date": "interview_date",
                                         "Interview_Time": "interview_time",
                                         "No_of_interviewers": "no_of_interviewers",
                                         "Progress_in_Interviews_Numbers": "progress_in_interviews_number",
                                         "Progress_in_Interviews_Names": "progress_in_interviews_names",
                                         "First_Last_Interview": 'first_last_interview'}
                          )

###Transfer values from Excel df to PostgreSQL DB

for row in range (len(table_to_import)):
    values_in_dict = table_to_import.loc[row].to_dict() ###change from DF to dictionary, which is then uploaded to PostgreSQL DB

    with engine.connect() as conn:
        conn.execute(sql
                    .insert(table)
                    .values([values_in_dict])
                    )
        conn.commit()


    if row % 100 == 0:
        print (f'Added {row} rows with interview details, out of {len(table_to_import)}')
    else:
        pass


print (f'The whole process is finished!')