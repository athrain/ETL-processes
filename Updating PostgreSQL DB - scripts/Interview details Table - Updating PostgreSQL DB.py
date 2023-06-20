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