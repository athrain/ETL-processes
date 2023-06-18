###A script that loads data exported directly from ATS with processes details, modify them if needed and then load them onto PostgreSQL DB


###BEFORE USING - include username, password, DB name + path to additional files/tables

import sqlalchemy
#import psycopg2
import pandas as pd


###connect to PostgreSQL table, load other necessary files
engine = sqlalchemy.create_engine('postgresql+psycopg2://user_name:password@localhost/database_name')
raw_data = pd.read_excel(r"C:\path\file_name.xlsx")
print (f'A connection to PostgreSQL was successfully established')

###creates a table in PostgreSQL database
metadata = sqlalchemy.MetaData()

table_name = "processess_details"

table = sqlalchemy.Table(
    table_name,
    metadata,
    sqlalchemy.Column("process_id", sqlalchemy.String),
    sqlalchemy.Column("status", sqlalchemy.String),
    sqlalchemy.Column("job_title", sqlalchemy.String),
    sqlalchemy.Column("contract_position_name", sqlalchemy.String),
    sqlalchemy.Column("furthest_candidate_status", sqlalchemy.String),
    sqlalchemy.Column("no_of_hires", sqlalchemy.Integer),
    sqlalchemy.Column("created_by", sqlalchemy.String),
    sqlalchemy.Column("country", sqlalchemy.String),
    sqlalchemy.Column("primary_recruiter", sqlalchemy.String),
    sqlalchemy.Column("all_recruiters", sqlalchemy.String),
    sqlalchemy.Column("primary_hiring_manager", sqlalchemy.String),
    sqlalchemy.Column("all_hiring_managers", sqlalchemy.String),
    sqlalchemy.Column("recruiting_leader", sqlalchemy.String),
    sqlalchemy.Column("department", sqlalchemy.String),
    sqlalchemy.Column("division", sqlalchemy.String),
    sqlalchemy.Column("category_career_page", sqlalchemy.String),
    sqlalchemy.Column("remote_type", sqlalchemy.String),
    sqlalchemy.Column("type_of_job", sqlalchemy.String),
    sqlalchemy.Column("estimated_start_date", sqlalchemy.Date),
    sqlalchemy.Column("new_headcount_replacement", sqlalchemy.String),
    sqlalchemy.Column("workflow_name", sqlalchemy.String),
    sqlalchemy.Column("evaluation_name", sqlalchemy.String),
    sqlalchemy.Column("created_on", sqlalchemy.Date),
    sqlalchemy.Column("first_time_open_date", sqlalchemy.Date),
    sqlalchemy.Column("most_recent_open_date", sqlalchemy.Date),
    sqlalchemy.Column("close_date", sqlalchemy.Date),
    sqlalchemy.Column("filled_date", sqlalchemy.Date),
    sqlalchemy.Column("hold_date", sqlalchemy.Date),
    sqlalchemy.Column("days_open", sqlalchemy.Integer),
    sqlalchemy.Column("days_on_hold", sqlalchemy.Integer)
)

metadata.create_all(engine)
print (f'A table {table_name} was succesfully created')


###adding values into database

counter = 0

###the part below can be definitely refactored - including dictionaries

for counter in range (len(raw_data)):

        ###finding the values by integers (table has 36 columns)
        process_id = raw_data.iloc[counter,0]
        wd_id = raw_data.iloc[counter,1]
        process_status = raw_data.iloc[counter, 2]
        job_title = raw_data.iloc[counter, 3]
        furthest_candidate = raw_data.iloc[counter, 5]
        no_of_hires = raw_data.iloc[counter, 35] ###last column in the table
        created_by = raw_data.iloc[counter, 7]
        primary_country = raw_data.iloc[counter, 8]
        primary_recruiter = raw_data.iloc[counter, 9]
        all_recruiters = raw_data.iloc[counter, 10]
        primary_hm = raw_data.iloc[counter, 11]
        all_hm = raw_data.iloc[counter, 12]
        recruiting_leader = raw_data.iloc[counter, 13]
        department = raw_data.iloc[counter, 14]
        division = raw_data.iloc[counter, 15]
        category_career_page = raw_data.iloc[counter, 16]
        remote_type = raw_data.iloc[counter, 19]
        job_type = raw_data.iloc[counter, 20]
        new_or_replacement = raw_data.iloc[counter, 22]
        workflow_name = raw_data.iloc[counter, 25]
        evaluation_name = raw_data.iloc[counter, 26]
        days_open = raw_data.iloc[counter, 33]
        days_on_hold = raw_data.iloc[counter, 34]

        with engine.connect() as conn:
            conn.execute(sqlalchemy
                         .insert(table)
                         .values
                            (process_id=process_id,
                             status=process_status,
                             job_title=job_title,
                             contract_position_name=wd_position_name,
                             furthest_candidate_status=furthest_candidate,
                             no_of_hires=int(no_of_hires),
                             created_by=created_by,
                             primary_country=primary_country,
                             primary_recruiter=primary_recruiter,
                             all_recruiters=all_recruiters,
                             primary_hiring_manager=primary_hm,
                             all_hiring_managers=all_hm,
                             recruiting_leader=recruiting_leader,
                             department=department,
                             division=division,
                             category_career_page=category_career_page,
                             remote_type=remote_type,
                             type_of_job=job_type,
                             new_headcount_replacement=new_or_replacement,
                             workflow_name=workflow_name,
                             evaluation_name=evaluation_name,
                             days_open=int(days_open),
                             days_on_hold=int(days_on_hold))
                     )
            conn.commit()

        if counter % 50 == 0:
            print (f'Added {counter} rows with unique (non date) values')
            pass
        else:
            pass
        counter +=1


###adding values that contains dates - in order to avoid including NaN values, generated by Pandas DF. Could be refactored by using dictionaries or replacing all NaN values with ""

for counter in range(len(raw_data)):
    process_id = raw_data.iloc[counter, 0]

    ###estimated start date
    date_to_import = raw_data.iloc[counter, 21]
    if pd.isna(date_to_import) is True:
        pass
    else:
        with engine.connect() as conn:
            conn.execute(sqlalchemy.update(table)
                         .where(table.c.req_id == process_id)
                         .values
                         (estimated_start_date=date_to_import)
                         )
            conn.commit()

    ###created on date
    date_to_import = raw_data.iloc[counter, 27]
    if pd.isna(date_to_import) is True:
        pass
    else:
        with engine.connect() as conn:
            conn.execute(sqlalchemy.update(table)
                             .where(table.c.req_id == process_id)
                             .values
                             (created_on=date_to_import)
                             )
            conn.commit()





    ###first time open date
    date_to_import = raw_data.iloc[counter, 28]
    if pd.isna(date_to_import) is True:
        pass
    else:
        with engine.connect() as conn:
            conn.execute(sqlalchemy.update(table)
                         .where(table.c.req_id == process_id)
                         .values
                         (first_time_open_date=date_to_import)
                         )
            conn.commit()

    ###most_recent open date
    date_to_import = raw_data.iloc[counter, 29]
    if pd.isna(date_to_import) is True:
        pass
    else:
        with engine.connect() as conn:
            conn.execute(sqlalchemy.update(table)
                         .where(table.c.req_id == process_id)
                         .values
                         (most_recent_open_date=date_to_import)
                         )
            conn.commit()
    ###close date
    date_to_import = raw_data.iloc[counter, 30]
    if pd.isna(date_to_import) is True:
        pass
    else:
        with engine.connect() as conn:
            conn.execute(sqlalchemy.update(table)
                             .where(table.c.req_id == process_id)
                             .values
                             (close_date=date_to_import)
                             )
            conn.commit()


    ###fill date
    date_to_import = raw_data.iloc[counter, 31]
    if pd.isna(date_to_import) is True:
        pass
    else:
        with engine.connect() as conn:
            conn.execute(sqlalchemy.update(table)
                             .where(table.c.req_id == process_id)
                             .values
                             (filled_date=date_to_import)
                             )
            conn.commit()

    ###hold date
    date_to_import = raw_data.iloc[counter, 32]
    if pd.isna(date_to_import) is True:
        pass
    else:
        with engine.connect() as conn:
            conn.execute(sqlalchemy.update(table)
                             .where(table.c.req_id == process_id)
                             .values
                             (hold_date=date_to_import)
                             )
            conn.commit()


    counter += 1
    if counter % 50 == 0:
        print(f'Added {counter} rows with unique date values')
        pass
    else:
        pass
    counter += 1

print(f'The whole process is finished!')