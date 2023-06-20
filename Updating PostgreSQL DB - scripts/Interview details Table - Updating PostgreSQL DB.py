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

###load all the tables into the script - main table from PostgreSQL DB and raw data

engine = sql.create_engine('postgresql+psycopg2://USERNAME:PASSWORD@localhost/DATABASENAME')
main_table = pd.read_sql("interview_details", con=engine.connect())
duplicate_values = pd.read_excel(r'C:\filepath\filename.xlsx')  ###excel with duplicate values to replace
raw_data = pd.read_excel(r'C:\filepath\filename.xlsx')

print (f'Connections to PostgreSQL Table was established and all the tables were converted into dataframes')


###creates a table, that already exists in PostgreSQL DB, so that we can insert new values there and update already existing ones
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

###rename columns in Raw data, so that they will be easy to import to PostgreSQL DB
raw_data = raw_data.rename(columns=
                                        {"Application Encrypted ID": "application_id",
                                         "Workflow State at Interview": "interview_stage",
                                         "Interview Scheduled Date": "interview_date",
                                         "Interview Scheduled Time": "interview_time",
                                         "# of Interviewers": "no_of_interviewers"}
                          )

###remove duplicate values from raw data

counter = 0

for counter in range(len(duplicate_values)):

    looked_duplicate_id = duplicate_values['Original_Value'].loc[counter]

    if duplicate_values.loc[duplicate_values['Original_Value'] == looked_duplicate_id, 'To_Replace_With'].iloc[0] == "DONOTCHANGE":
        pass
    else:
        try:
            position_to_replace = raw_data[raw_data['application_id'] == looked_duplicate_id].index.values
            replaced_value = duplicate_values.loc[duplicate_values['Original_Value'] == looked_duplicate_id, 'To_Replace_With'].iloc[0]
            raw_data.at[int(position_to_replace), 'application_id'] = replaced_value
        except:
            pass
    counter += 1


###delete non-interview stages like Shadowing, Phone Screen, blanks etc. from raw data
raw_data = raw_data.drop(raw_data[raw_data['interview_stage']=='Reference Check'].index)
raw_data = raw_data.drop(raw_data[raw_data['interview_stage']=='Shadowing'].index)
raw_data = raw_data.drop(raw_data[raw_data['interview_stage']=='Candidate Summary'].index)
raw_data = raw_data.drop(raw_data[raw_data['interview_stage']=='Phone Screen'].index)
raw_data = raw_data.drop(raw_data[raw_data['interview_stage']=='IN Recruiter Phone Screen'].index)
raw_data = raw_data.drop(raw_data[raw_data['interview_stage']==' '].index)

###create a DF that stores information about the level of the interview and the progress, that we will use further in analysis

d = {"progress_in_interviews_number": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
     "progress_in_interviews_names":
     ['First Interview', 'Second Interview', 'Third Interview', 'Fourth Interview', 'Fifth Interview','Sixth Interview', 'Seventh Interview',
     'Eighth Interview', 'Ninth Interview', 'Tenth Interview', 'Eleventh Interview', 'Twelfth Interview', 'Thirteenth Interview', 'Fourteenth Interview']}
interview_stages_names_df = pd.DataFrame(data=d)

###add completely new app ids with their interviews

unique_values_main_table = pd.DataFrame(columns=['application_id'])
uniques = main_table['application_id'].unique()
unique_values_main_table['application_id'] = uniques.tolist()

unique_values_raw_data = pd.DataFrame(columns=['application_id'])
uniques = raw_data['application_id'].unique()
unique_values_raw_data['application_id'] = uniques.tolist()

new_values_df = pd.concat([unique_values_main_table,unique_values_raw_data]).drop_duplicates(keep=False) ###creates a unique values, that are not yet in the main table

###delete app ids that no longer exist in raw data = meaning these candidates had their interviews canceled and never happened
###we do so by simple left outer join merge in pandas - so that we have only values that exist in new values df, but are not present in raw data


no_longer_existing_candidates_df = pd.merge(new_values_df, unique_values_raw_data, on=['application_id'], how="outer",
                                            indicator=True)
no_longer_existing_candidates_df = no_longer_existing_candidates_df[
    no_longer_existing_candidates_df['_merge'] == 'left_only']

print(f'The number of rows that we need to delete is: {len(no_longer_existing_candidates_df)}')

for row in range(len(no_longer_existing_candidates_df)):
    application_id = no_longer_existing_candidates_df.iloc[row, 0]
    with engine.connect() as conn:
        conn.execute(sql
                     .delete(table)
                     .where(table.c.application_id == application_id)
                     )
        conn.commit()
    print(f'Row deleted: {row} !')

    ###delete that apps also from new_values_df, so that they are not added to main table
    new_values_df = new_values_df.drop(new_values_df[new_values_df['application_id'] == application_id].index)
    new_values_df = new_values_df.reset_index()
    new_values_df = new_values_df.drop(columns=['index'])  ###get rid of the unnecessary column in this table, created by reseting index

###Create a loop to fill in the informations. Firstly, get the unique ID of the interview, then create ad hoc table with interviews for one candidate, based on App ID
###Later on, sort the table in one candidate by Interview Time, so that we could check which one was the last one, which one was the first one etc.
###Then create another for loop within the one candidate table and check how many interviews this person had and add necessary data
###Lastly, add that to PostgreSQL DB

for counter in range(len(new_values_df)):

    application_id = new_values_df.iloc[counter, 0]

    one_candidate_interviews_df = raw_data[raw_data['application_id'] == application_id]
    one_candidate_interviews_df = one_candidate_interviews_df.sort_values(by='interview_time', ascending=True)
    one_candidate_interviews_df = one_candidate_interviews_df.reset_index()
    one_candidate_interviews_df = one_candidate_interviews_df.drop(
        columns=['index'])  ###get rid of the unnecessary column in this table, created by reseting index

    counter_plus = 0

    for counter_plus in range(len(one_candidate_interviews_df)):

        try:
            one_candidate_interviews_df.at[counter_plus, "progress_in_interviews_names"] = \
            interview_stages_names_df.iloc[counter_plus, 1]
            one_candidate_interviews_df.at[counter_plus, "progress_in_interviews_number"] = str(
                interview_stages_names_df.iloc[counter_plus, 0])
        except:
            print('Out of range, more than 14 interviews!')
            pass
        counter_plus += 1

    if len(one_candidate_interviews_df) == 1:
        one_candidate_interviews_df.at[0, "first_last_interview"] = "Only one interview"
    else:
        one_candidate_interviews_df.at[0, "first_last_interview"] = "First interview"
        one_candidate_interviews_df.at[len(one_candidate_interviews_df) - 1, "first_last_interview"] = "Last interview"
        
    for dataframe in range(len(one_candidate_interviews_df)):
        values_in_dict = one_candidate_interviews_df.loc[dataframe].to_dict()
        insert_into_table(values_in_dict)

    counter += 1