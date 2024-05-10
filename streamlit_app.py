
r"""
Purpose:
    To Search free company dataset from Snowflake
    To show how to use snowflake with streamlit
    To show database connection with streamlit

reference:
    [connecting_to_snowflake]: https://www.youtube.com/watch?v=SgWxkAdjK78

date                name           comment
05/09/2024          TomN           initial creations


"""
import streamlit as st
from snowflake.snowpark import Session

@st.cache_resource
def create_session():
    # st.write(st.secrets.snowflake) ## for debugging on streamlit cloud
    return Session.builder.configs(st.secrets.snowflake).create()

st.markdown('### Search Company Dataset from Snowflake')



table_name = 'free_company_dataset.public.freecompanydataset'

session = create_session()

get_city_df = session.sql(f'SELECT DISTINCT locality FROM {table_name}')
city = [row[0] for row in get_city_df.collect()]
san_diego_index = city.index('san diego')

get_state_df = session.sql(f'SELECT DISTINCT region FROM {table_name}')
state = [row[0] for row in get_state_df.collect()]
california_index = state.index('california')

get_size_df = session.sql(f'SELECT DISTINCT size FROM {table_name}')
size = [row[0] for row in get_size_df.collect()]

## search for company name
company_name = st.text_input(label="Company Name")

city_selected = st.selectbox(label="City",
             options=city,
             key="city",
             index=san_diego_index)
state_selected = st.selectbox(label="State",
                options=state,
                key="state",
                index=california_index)
size_selected = st.selectbox(label="Size",
                options=size,
                key="size")
if company_name:
    df = session.sql(f'''
                    SELECT * 
                    FROM {table_name}
                    WHERE name like '%{company_name}%'
                 ''')
else:
    df = session.sql(f'''
                        SELECT * 
                        FROM {table_name}
                        WHERE locality like '%{city_selected}%'
                        AND REGION LIKE '%{state_selected}%'
                        AND SIZE LIKE '%{size_selected}%'
                    ''')


# st.write(df)
# st.dataframe(df)
# df = session.table(table_name)
# df_collect = df.collect()
df_collect = df.collect()
st.write("found", len(df_collect), "companies")
for row in df_collect:
    with st.container(border=True):
        st.markdown(f"**Company Name:** *{row.NAME}*")
        if row.FOUNDED:
            st.markdown(f"**Founded:** *{row.FOUNDED}*")
        else:
            st.markdown(f"**Founded:** *N/A*")
        if row.INDUSTRY:
            st.markdown(f"**Industry:** *{row.INDUSTRY}*")
        if row.WEBSITE:
            st.markdown(f"**Website:** *{row.WEBSITE}*")
        if row.LINKEDIN_URL:
            st.markdown(f"**LinkedIn:** *{row.LINKEDIN_URL}*")
