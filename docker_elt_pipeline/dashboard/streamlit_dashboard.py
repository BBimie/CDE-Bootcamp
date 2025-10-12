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

        self.engine = create_engine(f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}/{self.DB_NAME}")
        
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

    def format_value(self, value):
        if pd.isna(value):
            return "N/A"
        if abs(value) >= 1_000_000_000:
            return f"{value/1_000_000_000:.2f}B"
        elif abs(value) >= 1_000_000:
            return f"{value/1_000_000:.2f}M"
        elif abs(value) >= 1_000:
            return f"{value/1_000:.2f}K"
        else:
            return f"{value:.2f}"

    def charts(self):
        query = "SELECT * FROM yearly_financials;"

        df = self.load_data(query=query)

        st.title("📊 Annual Enterprise 2023 Survey Dashboard")

        industries = df["industry_name"].unique()
        years = sorted(df["survey_year"].unique())

        col1, col2 = st.columns(2)
        with col1:
            selected_industry = st.selectbox("Select Industry", options=industries)

        filtered_df = df[df["industry_name"] == selected_industry]

        # --------------------------
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)

        with kpi1:
            total_industries = df["industry_name"].nunique()
            st.metric("Total Industries", total_industries)

        with kpi2:
            avg_income = round(filtered_df["total_income"].mean(), 2)
            avg_income_formatted = self.format_value(avg_income)
            st.metric("Average Income", avg_income_formatted)

        with kpi3:
            avg_expenditure = round(filtered_df["total_expenditure"].mean(), 2)
            avg_expenditure_formatted = self.format_value(avg_expenditure)
            st.metric("Average Expenditure", avg_expenditure_formatted)

        with kpi4:
            avg_profit = round(filtered_df["profit_after_tax"].mean(), 2)
            avg_profit_formatted = self.format_value(avg_profit)
            st.metric("Avg Profit After Tax", avg_profit_formatted)

        # --------------------------
        # LINE CHART — Trend Analysis
        st.markdown(f"### 📈 Income Trend Over the Years in {selected_industry}")

        trend_df = (
            df[df["industry_name"] == selected_industry]
            .groupby("survey_year")["total_income"]
            .mean()
            .reset_index()
        )

        st.line_chart(trend_df.set_index("survey_year"))

        # --------------------------
        # TABLE — Raw Data Preview
        # --------------------------
        st.markdown("### 🧾 Data Preview")
        st.dataframe(filtered_df.head(20))

BuildDashboard().charts()
