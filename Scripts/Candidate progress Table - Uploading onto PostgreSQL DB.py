###A script that loads data exported directly from ATS with processes details, modify them if needed and then load them onto PostgreSQL DB
###It covers various candidate flows, where one row = one stage a candidate was at.


###BEFORE USING - include username, password, DB name + path to additional files/tables

import sqlalchemy
#import psycopg2
import pandas as pd

###connect to PostgreSQL table
engine = sqlalchemy.create_engine('postgresql+psycopg2://user_name:password@localhost/database_name')
raw_data = pd.read_excel(r"C:\path\file_name.xlsx")
print (f'A connection to PostgreSQL was successfully established')

###creates a table in PostgreSQL database
metadata = sqlalchemy.MetaData()

table_name = "candidates_progress"

table = sqlalchemy.Table(
    table_name,
    metadata,
    sqlalchemy.Column("application_id", sqlalchemy.String),
    sqlalchemy.Column("stage_name", sqlalchemy.String),
    sqlalchemy.Column("first_date_on_stage", sqlalchemy.Date),
    sqlalchemy.Column("first_time_on_stage", sqlalchemy.DateTime),
    sqlalchemy.Column("stage_order", sqlalchemy.Integer),
    sqlalchemy.Column("last_stage", sqlalchemy.String)
)

metadata.create_all(engine)
print (f'A table {table_name} was succesfully created')


###adding values into database

counter = 0

for counter in range (len(raw_data)):

        application_id = raw_data.iloc[counter,0]
        stage_name = raw_data.iloc[counter, 1]
        first_date_on_stage = raw_data.iloc[counter, 2]
        first_time_on_stage = raw_data.iloc[counter, 3]
        stage_order = raw_data.iloc[counter, 4]
        last_stage = raw_data.iloc[counter, 5]

        with engine.connect() as conn:
                conn.execute(sqlalchemy
                         .insert(table)
                         .values
                            (application_id=application_id,
                            stage_name=stage_name,
                            first_date_on_stage=first_date_on_stage,
                            first_time_on_stage=first_time_on_stage,
                            stage_order=int(stage_order),
                            last_stage=last_stage)
                     )
                conn.commit()

        if counter % 1000 == 0:
            print (f'Added {counter} rows with candidates, out of {len(raw_data)}')
            pass
        else:
            pass
        counter +=1

print (f'The whole process is finished!')