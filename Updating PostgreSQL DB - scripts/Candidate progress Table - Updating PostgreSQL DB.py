###A script that allows to update the data that are already in the database - it checks brand new values/candidates, who never appeared before, later on it checks the candidate
###who already exist in the database to see if they were moved to a new stage, not included in the previous update


###Create a function that inserts values onto the table
def insert_into_table (values_to_insert):
    with engine.connect() as conn:
            conn.execute(sql
                         .insert(table)
                         .values
                            ([values_to_insert])
                     )
            conn.commit()

import pandas as pd
import sqlalchemy as sql

###Step
### Load all the tables

engine = sql.create_engine('postgresql+psycopg2://user_name:password@localhost/database_name')
main_table = pd.read_sql("candidate_progress", con=engine.connect())
duplicate_values = pd.read_excel(r'C:\path\Case_Sensitive_Duplicates.xlsx')  ###excel with duplicate values to replace - PowerBI is case insensitive, Python not. Therefore, to avoid duplicates, I've created a list of some values to replace, so that they are unique
raw_data = pd.read_excel(r"C:\path\file_name.xlsx")
print (f'Connections to PostgreSQL Table was established')

###loads a table from PostgreSQL DB )table that already exists)
metadata = sql.MetaData()

table_name = "candidate_progress"

table = sql.Table(
    table_name,
    metadata,
    sql.Column("application_id", sql.String),
    sql.Column("stage_name", sql.String),
    sql.Column("first_date_on_stage", sql.Date),
    sql.Column("first_time_on_stage", sql.DateTime),
    sql.Column("stage_order", sql.Integer),
    sql.Column("last_stage", sql.String)
)


###Rename some columns in the raw data table, so they are aligned with the name of the columns available in PostgreSQL table
###The raw data table has only 4 columns, originally
raw_data = raw_data.rename(columns=
                                        {"Application Encrypted ID": "application_id",
                                         "Next Workflow State Name (Workflow Sorting)": "stage_name",
                                         "App Next Workflow State Date": "first_date_on_stage",
                                         "App Next Workflow State Time": "first_time_on_stage"}
                          )


###Remove duplicates from raw_data - as PowerBI is case insensitive, and some app ids are case sensitive, I need to add some letters to make them unique. And that's the loop for it.

counter = 0

for counter in range(len(duplicate_values)):

    looked_duplicate_id = duplicate_values['Original_Value'].loc[counter]

    if duplicate_values.loc[duplicate_values['Original_Value'] == looked_duplicate_id, 'To_Replace_With'].iloc[0] == "DONOTCHANGE":
        pass
        ###if it's marked as DONOTCHANGE then just skip it and check the next value. Otherwise it means we should replace it and we should do so
    else:
        try:
            position_to_replace = raw_data[raw_data['application_id'] == looked_duplicate_id].index.values
            replaced_value = duplicate_values.loc[duplicate_values['Original_Value'] == looked_duplicate_id, 'To_Replace_With'].iloc[0]
            raw_data.at[int(position_to_replace), 'application_id'] = replaced_value
        except:
            pass
    counter += 1


###gets the same tables in size (columns) from main table in PostgreSQL db and raw data to see if there are any new unique candidates.
###If so, they have to be added entirely onto the DB

main_table_to_compare = main_table.loc[['application_id','stage_name','first_date_on_stage','first_time_on_stage']]
new_values_df = pd.concat([main_table_to_compare,raw_data]).drop_duplicates(keep=False) ###creates a unique values, that are not yet in the main table

###create unique values from new import, to prepare the data for upload to the system
unique_values_df = pd.DataFrame(columns=['application_id'])
uniques = new_values_df['application_id'].unique()
unique_values_df['application_id'] = uniques.tolist()

###Make a for loop - iterate through every unique ID, create a single DF with only one applicant, sort it, add values and then join to the final table
###one candidate can have more than onw row, so it's important to select all rows with specific application id, sort it etc. to prepare it to the upload.

columns_names = ['application_id',
                 "stage_name",
                 "first_date_on_stage",
                 "first_time_on_stage",
                 "stage_order",
                 "last_stage"]

ready_to_upload_df = pd.DataFrame(columns=columns_names)

counter = 0
for counter in range(len(unique_values_df)):


    application_id = unique_values_df['application_id'].loc[counter]

    one_candidate_dataframe = new_values_df[new_values_df['application_id'] == application_id]
    one_candidate_dataframe = one_candidate_dataframe.sort_values(by='first_time_on_stage', ascending=True)
    one_candidate_dataframe = one_candidate_dataframe.reset_index()
    one_candidate_dataframe = one_candidate_dataframe.drop(columns=['index'])  ###get rid of the unnecessary column in this table, created by reseting index

    counter_additional = 0

    for counter_additional in range(len(one_candidate_dataframe)):
        one_candidate_dataframe.at[counter_additional, "stage_order"] = str(counter_additional + 1)
        if int(counter_additional + 1) == len(one_candidate_dataframe):
            one_candidate_dataframe.at[counter_additional, "last_stage"] = "Yes"
        else:
            pass
        counter_additional += 1
    ready_to_upload_df = pd.concat([ready_to_upload_df, one_candidate_dataframe], ignore_index=True)

    if counter % 100 == 0:
        print(f'Added {counter} unique candidates, out of {len(unique_values_df)}')
        pass
    else:
        pass

    counter += 1


###Add new values to main PostgreSQL table, the unique ones which were not there before the update

for counter in range(len(ready_to_upload_df)):

    values_in_dict = ready_to_upload_df.loc[counter].to_dict()
    insert_into_table(values_in_dict)

    if counter % 1000 == 0:
        print(f'Added {counter} rows with candidates to PostgreSQL table, out of {len(ready_to_upload_df)}')
        pass
    else:
        pass
    counter += 1

###Step
###that's definitely too long loop - needs to be refactored one day. But it works ;)
###In here, we compare if for the previous candidates, who are already in the system, there are new steps.
###To do so, firstly I create one candidate df from Main table and then the same df with the same app id, but from raw data
###Later, I compare the length - if raw data is longer, it means that there are new stages for that candidate
###if there are new stages, which we can easily check by (len(one_candidate_df_raw_data) - len(one_candidate_df_main_table)) != 0:, we add new stages
###if not - we skip it

###create unique values from new import, to prepare the data for upload to the system
unique_values_main_table_df = pd.DataFrame(columns=['application_id']) ##prepare new blank df
uniques = main_table['application_id'].unique() ###get uniques in a list
unique_values_main_table_df['application_id'] = uniques.tolist() ###upload a list onto the df

counter = 0
for counter in range(len(unique_values_main_table_df)):

    application_id = unique_values_main_table_df.iloc[counter, 0]

    one_candidate_df_main_table = main_table.loc[main_table['application_id'] == application_id]
    one_candidate_df_main_table = one_candidate_df_main_table.sort_values(by='first_time_on_stage', ascending=True)
    one_candidate_df_main_table = one_candidate_df_main_table.reset_index()
    one_candidate_df_main_table = one_candidate_df_main_table.drop(columns=['index'])  ###get rid of the unnecessary column in this table, created by reseting index

    one_candidate_df_raw_data = raw_data.loc[raw_data['application_id'] == application_id]
    one_candidate_df_raw_data = one_candidate_df_raw_data.sort_values(by='first_time_on_stage', ascending=True)
    one_candidate_df_raw_data = one_candidate_df_raw_data.reset_index()
    one_candidate_df_raw_data = one_candidate_df_raw_data.drop(columns=['index'])  ###get rid of the unnecessary column in this table, created by reseting index

    length_one_candidate_df_main_table = len(one_candidate_df_main_table)
    length_one_candidate_df_raw_data = len(one_candidate_df_raw_data)

        ###here the magic happens - if the result of the subtraction is bigger than 0, then it means we have to add new rows (candidate was moved into a new stage since the last update)
    if (len(one_candidate_df_raw_data) - len(one_candidate_df_main_table)) != 0:

        counter_additional = 0
        for counter_additional in range(len(one_candidate_df_raw_data) - len(one_candidate_df_main_table)):
            values_in_dict = one_candidate_df_raw_data.loc[
                (counter_additional + 1 + len(one_candidate_df_main_table))].to_dict()
            with engine.connect() as conn:
                conn.execute(sql
                             .insert(table)
                             .values
                             ([values_in_dict])
                             )
                conn.commit()
            counter_additional += 1
    else:
        pass
        ### the candidate was not moved, so we skip that one.

    if counter % 1000 == 0:
        print(f'Checked {counter} rows with candidates, out of {len(unique_values_main_table_df)}')
        pass
    else:
        pass
    counter += 1

    print ('The whole process finished.')
