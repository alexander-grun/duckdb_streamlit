import streamlit as st
import duckdb
import pandas as pd

# Streamlit title
st.title("NYC Service Requests - January 2021")

# DuckDB connection
motherduck_token = st.secrets["motherduck_token"]
con = duckdb.connect(f'md:{motherduck_token}')

# SQL query to fetch the data
query = """
SELECT created_date, agency, complaint_type, landmark, resolution_description
FROM sample_data.nyc.service_requests 
WHERE created_date >= '2021-01-02' AND created_date <= '2021-01-31'
"""

# Execute the query and fetch the data
data = con.execute(query).fetchdf()

# Display the data in a table
st.write("Displaying service requests data from January 2021:")
st.dataframe(data)

# Optionally, you can display a summary or statistics
st.write("Summary Statistics:")
st.write(data.describe())









