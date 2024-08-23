import streamlit as st
import duckdb
import pandas as pd
from datetime import datetime

# DuckDB connection setup
def get_duckdb_connection():
    motherduck_token = st.secrets["motherduck_token"]
    return duckdb.connect(f'md:{motherduck_token}')

# Fetch data from DuckDB
@st.cache_data
def fetch_data():
    con = get_duckdb_connection()

    df_url = con.execute('''SELECT 
                                url.header,
                                url.document_release_date,
                                url.number_of_pages,
                                url.size,
                                url.url,
                                url.Predicted_Quartery_report,
                                url.issuer_code
                            FROM "URLS" url
                            WHERE url.issuer_code IS NOT NULL
                            AND url.header != 'error'
                            ''').fetchdf()

    df = con.execute('''SELECT sh1."Ticker",                          
                                com."Company Name",                                                     
                                sh1."Units/Currency",
                                sh1."Quarter Ended (current quarter)",
                                sh1."Net cash from / (used in) operating activities",
                                sh1."Net cash from / (used in) investing activities",
                                sh1."Net cash from / (used in) financing activities",
                                sh1."Cash and cash equivalents at quarter end",
                                sh1."IQ Cash",
                                sh1."IQ Cash Burn",
                                sh1."IQ Cash Cover",
                                com."GICS industry group" as Industry, 
                                sh1."Year-Quarter",   
                                sh1."Receipts from Customers",
                                sh1."Government grants and tax incentives",                            
                                sh1."Proceeds from issues of equity securities",
                                sh1."Proceeds from issue of convertible debt securities",
                                sh1."Proceeds from borrowings",
                                sh1."Repayment of borrowings",
                                sh1."Dividends paid",                            
                                sh1."Total Financing Facilities (Amount drawn at quarter end)",
                                sh1."Unused financing facilities available at quarter end",
                                sh1."Total relevant outgoings",
                                sh1."Total available funding",
                                sh1."Estimated quarters of funding available",
                                sh1."Section 8.8",
                                com."Business Description" 
                            FROM "Cash_data" sh1  
                            LEFT JOIN "Company" com on sh1.Ticker = com.Ticker 
                            WHERE sh1."Ticker" IS NOT NULL''').fetchdf()

    return df, df_url

# Data cleaning and conversion
def clean_data(df):
    def clean_and_track(value, column_name):
        if pd.isna(value):
            return None
        value = str(value).replace(',', '')
        try:
            return float(value)
        except ValueError as e:
            problematic_values.append((column_name, value, str(e)))
            return None

    # List to keep track of problematic values
    global problematic_values
    problematic_values = []

    # Columns to clean
    columns_to_clean = [
        "Receipts from Customers",
        "Government grants and tax incentives",
        "Net cash from / (used in) operating activities",
        "Net cash from / (used in) investing activities",
        "Proceeds from issues of equity securities",
        "Proceeds from issue of convertible debt securities",
        "Proceeds from borrowings",
        "Repayment of borrowings",
        "Dividends paid",
        "Net cash from / (used in) financing activities",
        "Total Financing Facilities (Amount drawn at quarter end)",
        "Unused financing facilities available at quarter end",
        "Total relevant outgoings",
        "Cash and cash equivalents at quarter end",
        "Total available funding",
        "Estimated quarters of funding available"
    ]

    for column in columns_to_clean:
        df[column] = df[column].apply(lambda x: clean_and_track(x, column))

    df['IQ Cash Cover'] = pd.to_numeric(df['IQ Cash Cover'], errors='coerce').round(1)
    df["Estimated quarters of funding available"] = df["Estimated quarters of funding available"].round(1)

    return df

# Display problematic values
def display_problematic_values():
    if problematic_values:
        st.subheader("Problematic Values")
        problematic_df = pd.DataFrame(problematic_values, columns=["Column", "Value", "Error"])
        st.dataframe(problematic_df, use_container_width=True, hide_index=True)

# Main Streamlit code
# Fetch and clean data
df, df_url = fetch_data()
df = clean_data(df)
display_problematic_values()

# Ticker selection and display
st.header(f"Select ticker from {df['Ticker'].nunique()} available ")
ticker = st.selectbox('Choose a ticker', df['Ticker'].sort_values().unique().tolist(), placeholder='start typing...')
df1 = df[df['Ticker'] == ticker]
st.dataframe(df1, key="ticker", use_container_width=True, hide_index=True)

# Report announcements
st.subheader("📄Reports/Announcements")
df_url['Predicted_Quartery_report'] = df_url['Predicted_Quartery_report'].fillna(0).astype(int)
df_url['number_of_pages'] = df_url['number_of_pages'].fillna(0).astype(int)
df_url['document_release_date'] = pd.to_datetime(df_url['document_release_date'], errors='coerce')
df_url['document_release_date'] = df_url['document_release_date'].apply(
    lambda x: x.strftime('%m-%d-%Y') if pd.notnull(x) else x)

on = st.toggle("Show all Announcements")
if on:
    df_url = df_url[df_url['issuer_code'] == ticker].copy()
else:
    st.caption("Reports only")
    df_url = df_url[(df_url['issuer_code'] == ticker) & (df_url['Predicted_Quartery_report'] == 1)].copy()

df_url = df_url.drop(columns=['Predicted_Quartery_report']).reset_index(drop=True)

st.dataframe(df_url, column_config={
    "url": st.column_config.LinkColumn("URL"),
    "document_release_date": st.column_config.DateColumn("Publication Date", format="DD-MM-YYYY"),
    "number_of_pages": "Pages",
    "issuer_code": "Ticker"
}, hide_index=True)

# Data Quality (DQ) Form
st.header("DQ Form")
con = get_duckdb_connection()
df_dq = con.execute('''SELECT "Ticker",
                        "Issue_type",
                        "Quarter",
                        "Notes",
                        "Timestamp"
                    FROM "DQ_Form" dq                              
                    WHERE dq."Ticker" IS NOT NULL''').fetchdf()

DQ_issue_type = ["missing report", "wrong data (GPT re-scrape)", "formatting", "other"]
DQ_quarter = ["March24", "June24", "Other"]

with st.form(key="dq_form"):
    ticker_field = st.text_input(label="Ticker", value=ticker)
    dq_issue_type = st.selectbox("Data issue type", options=DQ_issue_type)
    dq_quarter = st.multiselect("Quarter", options=DQ_quarter)
    additional_info = st.text_area(label="Notes")

    submit_button = st.form_submit_button(label="Report DQ issue")

    if submit_button:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dq_form_content = {
            "Ticker": ticker_field,
            "Issue_type": dq_issue_type,
            "Quarter": ','.join(dq_quarter),  # Convert list to comma-separated string if necessary
            "Notes": additional_info,
            "Timestamp": current_time,
        }

        # Insert new row into the DQ_Form table
        insert_query = '''
            INSERT INTO "DQ_Form" ("Ticker", "Issue_type", "Quarter", "Notes", "Timestamp")
            VALUES (?, ?, ?, ?, ?)
        '''
        con.execute(insert_query, (dq_form_content["Ticker"], dq_form_content["Issue_type"], dq_form_content["Quarter"], dq_form_content["Notes"], dq_form_content["Timestamp"]))
        con.commit()  # Commit the transaction

        # Clear cache and refresh df_dq
        st.cache_data.clear()  # Clear Streamlit cache
        df_dq = con.execute('''SELECT "Ticker",
                                "Issue_type",
                                "Quarter",
                                "Notes",
                                "Timestamp"
                            FROM "DQ_Form" dq                              
                            WHERE dq."Ticker" IS NOT NULL''').fetchdf()  # Refresh df_dq

        st.success("DQ issue added to the list")

st.subheader("Recently reported DQ issues")
st.dataframe(df_dq, use_container_width=True, hide_index=True)
