# Use lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /streamlit_dashboard

COPY . /streamlit_dashboard

RUN pip install --no-cache-dir -r requirements.txt

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "streamlit_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
