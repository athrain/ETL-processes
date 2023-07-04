###It's still a pseudo code - some ideas to work on later on


###Step 1
#Connect to database, import Excel file

import pandas as pd
import sqlalchemy as sql

###load all the tables into the script - main table from PostgreSQL DB and raw data

engine = sql.create_engine('postgresql+psycopg2://USERNAME:PASSWORD@localhost/DATABASENAME')
main_table = pd.read_sql("offer_hire_details", con=engine.connect())
duplicate_values = pd.read_excel(r'C:\filepath\filename.xlsx')  ###excel with duplicate values to replace
raw_data = pd.read_excel(r'C:\filepath\filename.xlsx') ###new values to compare with the main table

print (f'Connections to PostgreSQL Table was established and all the tables were converted into dataframes')

###Step 2
#Connect to a table in PostgreSQL (without creating it once again)

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

print (f'A table {table_name} was succesfully prepared')

###Step
#Replace duplicates in raw data

for row in range(len(duplicate_values)):

    looked_duplicate_id = duplicate_values['Original_Value'].loc[row]

    if duplicate_values.loc[duplicate_values['Original_Value'] == looked_duplicate_id, 'To_Replace_With'].iloc[0] == "DONOTCHANGE":
        pass
    else:
        try:
            position_to_replace = raw_data[raw_data['Application Encrypted ID'] == looked_duplicate_id].index.values
            replaced_value = duplicate_values.loc[duplicate_values['Original_Value'] == looked_duplicate_id, 'To_Replace_With'].iloc[0]
            raw_data.at[int(position_to_replace), 'Application Encrypted ID'] = replaced_value
        except:
            pass
        ###it passess if it does not find a duplicate value in a data table


###Step 3
#Finding new, unique offers
#Firstly, get new unique IDs of applications by comparing both tables
#Then, add it to the table

raw_data = raw_data.rename(columns={"Application Encrypted ID": "application_id"})


main_table_to_compare = main_table.loc["application_id"]
raw_data_to_compare = raw_data.loc["application_id"]
new_values_df = pd.concat([main_table_to_compare,raw_data_to_compare]).drop_duplicates(keep=False) ###creates a unique values, that are not yet in the main table

###create unique values from new import, to prepare the data for upload to the system
unique_values_df = pd.DataFrame(columns=['application_id'])
uniques = new_values_df['application_id'].unique()
unique_values_df['application_id'] = uniques.tolist()

###add brand new offers to the PostgreSQL table

for row in range (len(unique_values_df)):

    application_id = unique_values_df['application_id'].loc[row]
    values_in_dict = raw_data[raw_data['application_id'] == application_id].to_dict()

    with engine.connect() as conn:
        conn.execute(sql
                    .insert(table)
                    .values([values_in_dict])
                    )
        conn.commit()

###Step 4
#Compare the offers that were made previously
#compare 2 dictionaries - if they are the same, then skip
#if not - upload a dictionary from the raw data

