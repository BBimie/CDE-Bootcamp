import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Enterprise Survey Dashboard", layout="wide")

class BuildDashboard:
    def __init__(self):
        # --- DB connection details from docker-compose ---
        self.DB_HOST =  os.getenv("DB_HOST", "localhost")
        self.DB_USER = os.environ["DB_USER"]
        self.DB_PASSWORD = os.environ["DB_PASSWORD"]
        self.DB_NAME = os.environ["DB_NAME"]

        # self.engine = create_engine(f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}/{self.DB_NAME}")
        self.engine = create_engine(f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:5434/{self.DB_NAME}")
        
    def load_data(self, query) -> pd.DataFrame:
        with self.engine.connect() as conn:
            return pd.read_sql(sql=query, con=conn)
        
    def variable_line_chart(self):
        query = "SELECT * FROM enterprise_table_transforms;"

        df = self.load_data(query=query)

        #convert survey_year to string
        df['survey_year'] = df['survey_year'].astype(str)
        
        st.header("Line Chart: Variable Trends Over Years")

        # Sidebar filters
        industries = sorted(df["industry_name"].dropna().unique())
        variables = sorted(df["variable_name"].dropna().unique())

        selected_industry = st.sidebar.selectbox("Select Industry", industries)
        selected_variable = st.sidebar.selectbox("Select Variable Code", variables)

        # Filter data
        filtered = df[
            (df["industry_name"] == selected_industry) &
            (df["variable_name"] == selected_variable)
        ]

        # Aggregate (if needed)
        chart_data = filtered.groupby("survey_year")["value"].sum().reset_index()

        # Display chart
        st.line_chart(chart_data, x="survey_year", y="value")

    # def cards(self):
    #     query = "SELECT * FROM yearly_financials;"
    #     df = self.load_data(query=query)

    #     total_industries = df['industry_name'].nunique()
    #     total_records = df.shape[0]

    #     col1, col2 = st.columns(2)

    #     # Example: average, max, min Value
    #     avg_income = filtered_df["Value"].mean()
    #     max_income = filtered_df["Value"].max()
    #     min_income = filtered_df["Value"].min()

    #     col1.metric("Average Total Income", f"${avg_income:,.2f}")
    #     col2.metric("Highest Value", f"${max_income:,.2f}")
    #     col3.metric("Lowest Value", f"${min_income:,.2f}")

    def manage_dashboard(self):
        st.title("📊 Annual Enterprise 2023 Survey Dashboard")
        
        self.variable_line_chart()

BuildDashboard().manage_dashboard()