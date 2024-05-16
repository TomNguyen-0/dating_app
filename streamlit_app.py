
r"""
Purpose:
    To Search free company dataset from Snowflake
    To show how to use snowflake with streamlit
    To show database connection with streamlit

reference:
    [connecting_to_snowflake]: https://www.youtube.com/watch?v=SgWxkAdjK78

date                name           comment
05/09/2024          TomN           initial creations
05/16/2024          TomN           load 10 rows at a time

backlog:
    download database from snowflake


"""
import streamlit as st
from snowflake.snowpark import Session

@st.cache_resource
def create_session():
    # st.write(st.secrets.snowflake) ## for debugging on streamlit cloud
    return Session.builder.configs(st.secrets.snowflake).create()

@st.cache_resource
def get_info():
    get_city_df = session.sql(f'SELECT DISTINCT locality FROM {table_name} order by locality')
    city = [row[0] for row in get_city_df.collect()]

    get_state_df = session.sql(f'SELECT DISTINCT region FROM {table_name} order by region')
    state = [row[0] for row in get_state_df.collect()]

    get_size_df = session.sql(f'SELECT DISTINCT size FROM {table_name} order by size')
    size = [row[0] for row in get_size_df.collect()]

    return (city,state,size)

@st.cache_resource
def get_df():
    table_name = 'free_company_dataset.public.freecompanydataset'
    df = session.table(table_name)
    return df


st.markdown('### Search Company Dataset from Snowflake')


table_name = 'free_company_dataset.public.freecompanydataset'

session = create_session()
city,state,size = get_info()

san_diego_index = city.index('san diego')
california_index = state.index('california')
## search for company name
company_name = st.text_input(label="Company Name")

# city_selected = st.selectbox(label="City",
#              options=city,
#              key="city",
#              index=san_diego_index)
city_selected = st.text_input(label="City", key="city", value="san diego")
# state_selected = st.selectbox(label="State",
#                 options=state,
#                 key="state",
#                 index=california_index)
state_selected = st.text_input(label="State", key="state", value="california")
size_selected = st.selectbox(label="Size",
                options=size,
                key="size")
df_original = get_df()
if company_name:
    df = df_original.filter(f"NAME ilike '%{company_name}%'") 
        # df = session.sql(f'''
        #             SELECT * 
        #             FROM {table_name}
        #             WHERE name like '%{company_name}%'
        #          ''')
else:
    df = df_original.where(f"locality ilike '%{city_selected}%' AND REGION iLIKE '%{state_selected}%' AND SIZE iLIKE '%{size_selected}%'") 
    # df = session.sql(f'''
    #                     SELECT * 
    #                     FROM {table_name}
    #                     WHERE locality like '%{city_selected}%'
    #                     AND REGION LIKE '%{state_selected}%'
    #                     AND SIZE LIKE '%{size_selected}%'
    #                 ''')



df_collect = df.collect() ## I can move this into get_df and have it collect only once

if 'index_load' not in st.session_state:
    st.session_state.index_load = 0
# st.write("index_load", st.session_state.index_load) ## for debugging
st.write(f"{st.session_state.index_load + 10} out of ", len(df_collect), "companies")

for index,row in enumerate(df_collect,1):
    if index <= st.session_state.index_load:
        continue
    if (index+st.session_state.index_load) % 10 == 0:
        break
        
            
    with st.container(border=True):
        st.markdown(f"**Company Name:** *{row.NAME}*")
        if row.FOUNDED:
            st.markdown(f"**Founded:** *{row.FOUNDED}*")
        else:
            st.markdown(f"**Founded:** *N/A*")
        if row.INDUSTRY:
            st.markdown(f"**Industry:** *{row.INDUSTRY}*")
        if row.WEBSITE:
            st.markdown(f"**Website:** *[{row.WEBSITE}](https://www.{row.WEBSITE})*")
        if row.LINKEDIN_URL:
            st.markdown(f"**LinkedIn:** *{row.LINKEDIN_URL}*")

if st.button("Load next 10 rows"):
    st.session_state.index_load += 10
    st.experimental_rerun()