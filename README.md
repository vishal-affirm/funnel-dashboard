# Checkout Funnel Analytics Dashboard

A Streamlit dashboard for analyzing term selection dropoff rates by FICO score, AOV, and 0% APR offers.

## Features

- **FICO Dropoff Analysis**: Term selection dropoff rates by credit score bucket
- **Term Selection Impact**: Comparison of confirmation rates with/without term selection
- **AOV Analysis**: Dropoff rates by order value with FICO x AOV heatmap
- **0% APR Impact**: How 0% APR term length affects conversion rates

## Local Setup

1. **Install dependencies:**
   ```bash
   cd funnel_dashboard
   pip install -r requirements.txt
   ```

2. **Configure Snowflake credentials:**
   ```bash
   cp .streamlit/secrets.toml.example .streamlit/secrets.toml
   # Edit secrets.toml with your credentials
   ```

3. **Run the dashboard:**
   ```bash
   streamlit run app.py
   ```

4. **Authenticate:** Your browser will open for Snowflake SSO authentication.

## Deployment to Streamlit Cloud

1. **Push to GitHub:**
   ```bash
   git init
   git add .
   git commit -m "Initial funnel dashboard"
   git remote add origin https://github.com/YOUR_USERNAME/funnel-dashboard.git
   git push -u origin main
   ```

2. **Deploy on Streamlit Cloud:**
   - Go to [share.streamlit.io](https://share.streamlit.io)
   - Connect your GitHub repo
   - Add secrets in the app settings (Settings > Secrets):
     ```toml
     [snowflake]
     account = "AFFIRM-AFFIRMUSEAST"
     user = "your.email@affirm.com"
     password = "your_password"  # Required for cloud deployment
     ```

   **Note:** For Streamlit Cloud, you'll need to modify `app.py` to use password auth instead of `externalbrowser`:
   ```python
   # Change authenticator="externalbrowser" to:
   # password=st.secrets["snowflake"]["password"]
   ```

## Data Sources

- **Table:** `PROD__US.DBT_ANALYTICS.CHECKOUT_FUNNEL_V5`
- **Warehouse:** `SHARED`
- **Refresh:** Data is cached for 1 hour (configurable via `@st.cache_data(ttl=3600)`)

## Key Metrics

| Metric | Description |
|--------|-------------|
| Dropoff Rate | % of approved users who didn't select a term |
| Completion Rate | % of approved users who selected a term |
| Confirm Rate | % of users who confirmed after term selection |
