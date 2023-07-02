###The script allows to connect to database,
### create a table
### and add some values from Pandas dataframe,


###BEFORE USING - include username, password, DB name + path to additional files/tables

import sqlalchemy as sql
import pandas as pd

###connect to PostgreSQL table
engine = sql.create_engine('postgresql+psycopg2://USERNAME:PASSWORD@localhost/DB_Name')
table_to_import = pd.read_excel(r"C:file_path\file_name.xlsx")
print (f'A connection to PostgreSQL was successfully established')

###creates a table in PostgreSQL database
metadata = sql.MetaData()

table_name = "offer_hire_details"

table = sql.Table(
    table_name,
    metadata,
    sql.Column("application_id", sql.String),
    sql.Column("candidate_id", sql.String),
    sql.Column("job_title_name", sql.String),
    sql.Column("form_of_cooperation", sql.String),
    sql.Column("salary", sql.String),
    sql.Column("hired_date", sql.DateTime),
    sql.Column("start_date", sql.DateTime),
    sql.Column("hiring_entity", sql.String),
    sql.Column("work_model", sql.String),
    sql.Column("supervisory_organisation", sql.String),
    sql.Column("direct_supervisor", sql.String)
)

metadata.create_all(engine)
print (f'A table {table_name} was succesfully created')


###adding values into database


for row in range (len(table_to_import)):
    ###the below logic is because I check if there are any offer made or not - as it gets data from the general Candidate Details table
    ###if it's True, then it means there were no offer
    if pd.isna(table_to_import.iloc[row, 16]) is True and pd.isna(table_to_import.iloc[row, 23]) is True:
        continue
    else:
        application_id = table_to_import.iloc[row,0]
        candidate_id = table_to_import.iloc[row,1]
        job_title = table_to_import.iloc[row, 16]
        form_of_cooperation = table_to_import.iloc[row, 17]
        salary = table_to_import.iloc[row, 18]
        hiring_entity = table_to_import.iloc[row, 20]
        work_model = table_to_import.iloc[row, 21]
        supervisory_org = table_to_import.iloc[row, 24]
        direct_supervisor = table_to_import.iloc[row, 22]

        with engine.connect() as conn:
            conn.execute(sql
                     .insert(table)
                     .values
                     (application_id=application_id,
                      candidate_id=candidate_id,
                      job_title_name=job_title,
                      form_of_cooperation=form_of_cooperation,
                      salary=salary,
                      hiring_entity=hiring_entity,
                      work_model=work_model,
                      supervisory_organisation=supervisory_org,
                      direct_supervisor=direct_supervisor)
                     )
            conn.commit()

    if row % 25 == 0:
        print (f'Checked {row} rows, out of {len(table_to_import)}')
        pass
    else:
        pass

for row in range (len(table_to_import)):

    application_encrypted_id = table_to_import.iloc[row, 0]

    hired_date = table_to_import.iloc[row, 23]
    if pd.isna(hired_date) is True:
        continue
    else:
        with engine.connect() as conn:
            conn.execute(sql
                     .update(table)
                     .where(table.c.application_id==application_encrypted_id)
                     .values
                     (hired_date=hired_date)
                     )
            conn.commit()

    start_date = table_to_import.iloc[row, XXX] ###the position in table to be checked later on
    if pd.isna(start_date) is True:
        continue
    else:
        with engine.connect() as conn:
            conn.execute(sql
                     .update(table)
                     .where(table.c.application_id==application_encrypted_id)
                     .values
                     (start_date=start_date)
                     )
            conn.commit()

print (f'A table was sucessfully loaded from PostgreSQL to Pandas DF')
