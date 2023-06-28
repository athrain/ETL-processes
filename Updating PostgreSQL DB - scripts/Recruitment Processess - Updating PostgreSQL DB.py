###BEFORE USING - include username, password, DB name + path to additional files/tables

import sqlalchemy as sql
import pandas as pd


###connect to PostgreSQL table, load other necessary files
engine = sql.create_engine('postgresql+psycopg2://user_name:password@localhost/database_name')
raw_data = pd.read_excel(r"C:\path\file_name.xlsx")
main_table = pd.read_sql("processess_details", con=engine.connect())
print (f'A connection to PostgreSQL was successfully established')

###creates a table in PostgreSQL database
metadata = sql.MetaData()

table_name = "processess_details"

table = sql.Table(
    table_name,
    metadata,
    sql.Column("process_id", sql.String),
    sql.Column("status", sql.String),
    sql.Column("job_title", sql.String),
    sql.Column("contract_position_name", sql.String),
    sql.Column("furthest_candidate_status", sql.String),
    sql.Column("no_of_hires", sql.Integer),
    sql.Column("created_by", sql.String),
    sql.Column("country", sql.String),
    sql.Column("primary_recruiter", sql.String),
    sql.Column("all_recruiters", sql.String),
    sql.Column("primary_hiring_manager", sql.String),
    sql.Column("all_hiring_managers", sql.String),
    sql.Column("recruiting_leader", sql.String),
    sql.Column("department", sql.String),
    sql.Column("division", sql.String),
    sql.Column("category_career_page", sql.String),
    sql.Column("remote_type", sql.String),
    sql.Column("type_of_job", sql.String),
    sql.Column("estimated_start_date", sql.Date),
    sql.Column("new_headcount_replacement", sql.String),
    sql.Column("workflow_name", sql.String),
    sql.Column("evaluation_name", sql.String),
    sql.Column("created_on", sql.Date),
    sql.Column("first_time_open_date", sql.Date),
    sql.Column("most_recent_open_date", sql.Date),
    sql.Column("close_date", sql.Date),
    sql.Column("filled_date", sql.Date),
    sql.Column("hold_date", sql.Date),
    sql.Column("days_open", sql.Integer),
    sql.Column("days_on_hold", sql.Integer)
)

print (f'A table {table_name} was succesfully created')

###rename columns in Raw data, so that they will be easy to import to PostgreSQL DB
raw_data = raw_data.rename(columns=
                                        {
                                         "Req ID": "process_id",
                                         "Requisition Status": "status",
                                         "Job Title": "job_title",
                                         "Contract Position Name": "contract_position_name",
                                         "Furthest Candidate Status": "furthest_candidate_status",
                                         "# of Hires": "no_of_hires",
                                         "Requisition Created By Full Name": "created_by",
                                         "Requisition Country": "country",
                                         "Primary Recruiter Full Name": "primary_recruiter",
                                         "Recruiter List": "all_recruiters",
                                         "Primary Hiring Manager Full Name": "primary_hiring_manager",
                                         "Hiring Manager List": "all_hiring_managers",
                                         "Recruiting Leader": "recruiting_leader",
                                         "Department": "department",
                                         "Division": "division",
                                         "Category": "category_career_page",
                                         "Remote": "remote_type",
                                         "Requisition Type": "type_of_job",
                                         "Estimated start date": "estimated_start_date",
                                         "Headcount": "new_headcount_replacement",
                                         "Workflow Title": "workflow_name",
                                         "Evaluation Name": "evaluation_name",
                                         "Created On Date": "created_on",
                                         "Requisition Open Date": "first_time_open_date",
                                         "Most Recent Open Date": "most_recent_open_date",
                                         "Requisition Close Date": "close_date",
                                         "Requisition Filled Date": "filled_date",
                                         "Requisition Hold Date": "hold_date",
                                         "Requisition Days Open": "days_open",
                                         "Requisition Days on Hold": "days_on_hold"
                                         }
                          )
###adding new requisitions based on newest IDs

unique_values_main_table = pd.DataFrame(columns=['process_id'])
uniques = main_table['process_id'].unique()
unique_values_main_table['process_id'] = uniques.tolist()

unique_values_raw_data = pd.DataFrame(columns=['process_id'])
uniques = raw_data['process_id'].unique()
unique_values_raw_data['process_id'] = uniques.tolist()

new_values_df = pd.concat([unique_values_main_table,unique_values_raw_data]).drop_duplicates(keep=False) ###creates a unique values, that are not yet in the main table

for new_requisition in range (len(new_values_df)):
    req_id = new_values_df.iloc[new_requisition, 0]
    values_in_dict = raw_data[raw_data['process_id'] == req_id].to_dict()  ###change from DF to dictionary, which is then uploaded to PostgreSQL DB

    with engine.connect() as conn:
        conn.execute(sql
                     .insert(table)
                     .values([values_in_dict])
                     )
        conn.commit()

    ###delete that req ids also from raw data, so that they are not added to main table later on
    raw_data = raw_data.drop(raw_data[raw_data['process_id'] == req_id].index)
    raw_data = raw_data.reset_index()
    raw_data = raw_data.drop(columns=['index'])  ###get rid of the unnecessary column in this table, created by reseting index

###checking if there are changes in any dictionary in already existing requisitions. If not - skip. If yes - replace the whole row in a database

for requisition in range (len(raw_data)):
    req_id = raw_data.iloc[requisition, 0]
    raw_data_values_in_dict = raw_data.loc[requisition].to_dict()  ###change from DF to dictionary, which is then uploaded to PostgreSQL DB
    main_table_values_in_dict = main_table[main_table['process_id'] == req_id].to_dict()

    if main_table_values_in_dict == raw_data_values_in_dict:
        pass ###we pass here, as it means there are no changes in req details since the last time

    else:
        with engine.connect() as conn:
            conn.execute(sql
                         .insert(table)
                         .values([raw_data_values_in_dict])
                         )
            conn.commit()

    if requisition % 100 == 0:
        print(f'Changed {requisition} rows with recruitment process details, out of {len(raw_data)}')
    else:
        pass

print(f'The whole process is finished!')