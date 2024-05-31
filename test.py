r"""
Purpose:
    copy data from snowflake to ssms
    I don't want to keep on paying for snowflake storage
    this way I can have it locally on my machine and I can query it on my own.

researched commands:
    pip install snowflake-cli-lab
    snow connection add
    snow connection set-default tomnfullerton
    snow connection test

reference:
    

date                name           comment
05/18/2024          TomN           initial creations


backlog:



"""
import streamlit as st
from snowflake.snowpark import Session

@st.cache_resource
def create_session():
    # st.write(st.secrets.snowflake) ## for debugging on streamlit cloud
    return Session.builder.configs(st.secrets.snowflake).create()

@st.cache_resource
def get_df():
    table_name = 'dwh.public.test'
    df = session.table(table_name)
    return df

## https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.FileOperation#snowflake.snowpark.FileOperation

session = create_session()
df = get_df()

## download useless files from staging snowflake to local.
# session.file.get('@my_stage',r'C:\Users\tommi\OneDrive\Desktop\New folder\test')

## load dataframe into database warehouse
# df = df.limit(10)
df = df.collect()
# print(df)


## connect to ssms
from SQL_loader import DWH_DB
server = 'LAPTOP-1O0A0PQD'
database = 'DWH'
database_connection = DWH_DB(verbose=True, servername=server, database=database)
for row in df:
    # print(row)
    ## replace ' with '' 
    database_connection.execute_query(f"""
                            INSERT INTO snowflake.[dbo].[free_companies]
                                ([COUNTRY]
                                ,[FOUNDED]
                                ,[ID]
                                ,[INDUSTRY]
                                ,[LINKEDIN_URL]
                                ,[LOCALITY]
                                ,[NAME]
                                ,[REGION]
                                ,[SIZE]
                                ,[WEBSITE])
                            VALUES
                                ('{row['COUNTRY'].replace("'","''") if row.COUNTRY else 'NULL'}'
                                ,{row['FOUNDED'] if row.FOUNDED else 0}
                                ,'{row['ID'].replace("'","''") if row.ID else 'NULL'}'
                                ,'{row['INDUSTRY'].replace("'","''") if row.INDUSTRY else 'NULL'}'
                                ,'{row['LINKEDIN_URL'].replace("'","''") if row.LINKEDIN_URL else 'NULL'}'
                                ,'{row['LOCALITY'].replace("'","''") if row.LOCALITY else 'NULL'}'
                                ,'{row['NAME'].replace("'","''") if row.NAME else 'NULL'}'
                                ,'{row['REGION'].replace("'","''") if row.REGION else 'NULL'}'
                                ,'{row['SIZE'].replace("'","''") if row.SIZE else 'NULL'}'
                                ,'{row['WEBSITE'].replace("'","''") if row.WEBSITE else 'NULL'}')
                            """)


## expected count : 22918323