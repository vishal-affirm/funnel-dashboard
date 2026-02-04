import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="Checkout Funnel Analytics",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("📊 Checkout Funnel Analytics Dashboard")
st.markdown("**Term Selection Dropoff Analysis** | Last 30 Days")

# Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    # Check if running locally (use browser auth) or cloud (use key-pair)
    if "private_key" in st.secrets.get("snowflake", {}):
        # Key-pair auth for Streamlit Cloud
        import base64
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        
        private_key_bytes = st.secrets["snowflake"]["private_key"].encode()
        private_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=None,
            backend=default_backend()
        )
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        return snowflake.connector.connect(
            account=st.secrets["snowflake"]["account"],
            user=st.secrets["snowflake"]["user"],
            private_key=private_key_bytes,
            warehouse="SHARED",
            database="PROD__US",
            schema="DBT_ANALYTICS"
        )
    else:
        # Browser auth for local development
        return snowflake.connector.connect(
            account=st.secrets["snowflake"]["account"],
            user=st.secrets["snowflake"]["user"],
            authenticator="externalbrowser",
            warehouse="SHARED",
            database="PROD__US",
            schema="DBT_ANALYTICS"
        )

@st.cache_data(ttl=3600)  # Cache for 1 hour
def run_query(query):
    conn = get_snowflake_connection()
    return pd.read_sql(query, conn)

# Query 1: FICO Dropoff Rates
FICO_DROPOFF_QUERY = """
SELECT 
    CASE 
        WHEN FICO_SCORE IS NULL THEN 'No Score'
        WHEN FICO_SCORE < 580 THEN 'Poor (<580)'
        WHEN FICO_SCORE >= 580 AND FICO_SCORE < 670 THEN 'Fair (580-669)'
        WHEN FICO_SCORE >= 670 AND FICO_SCORE < 740 THEN 'Good (670-739)'
        WHEN FICO_SCORE >= 740 AND FICO_SCORE < 800 THEN 'Very Good (740-799)'
        WHEN FICO_SCORE >= 800 THEN 'Exceptional (800+)'
    END as FICO_SCORE_BUCKET,
    COUNT(*) as TOTAL_CHECKOUTS,
    SUM(CASE WHEN IS_APPROVED = 1 THEN 1 ELSE 0 END) as APPROVED,
    SUM(CASE WHEN IS_APPROVED = 1 AND TERM_LENGTH IS NOT NULL THEN 1 ELSE 0 END) as TERM_SELECTED,
    SUM(CASE WHEN IS_APPROVED = 1 AND TERM_LENGTH IS NULL THEN 1 ELSE 0 END) as DROPPED_OFF,
    ROUND(SUM(CASE WHEN IS_APPROVED = 1 AND TERM_LENGTH IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN IS_APPROVED = 1 THEN 1 ELSE 0 END), 0), 2) as DROPOFF_PCT
FROM PROD__US.DBT_ANALYTICS.CHECKOUT_FUNNEL_V5
WHERE CHECKOUT_CREATED_DT >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 
    CASE FICO_SCORE_BUCKET
        WHEN 'Exceptional (800+)' THEN 1
        WHEN 'Very Good (740-799)' THEN 2
        WHEN 'Good (670-739)' THEN 3
        WHEN 'Fair (580-669)' THEN 4
        WHEN 'Poor (<580)' THEN 5
        WHEN 'No Score' THEN 6
    END
"""

# Query 2: Term Selection vs Confirmation
TERM_CONFIRM_QUERY = """
SELECT 
    CASE 
        WHEN FICO_SCORE IS NULL THEN 'No Score'
        WHEN FICO_SCORE < 580 THEN 'Poor (<580)'
        WHEN FICO_SCORE >= 580 AND FICO_SCORE < 670 THEN 'Fair (580-669)'
        WHEN FICO_SCORE >= 670 AND FICO_SCORE < 740 THEN 'Good (670-739)'
        WHEN FICO_SCORE >= 740 AND FICO_SCORE < 800 THEN 'Very Good (740-799)'
        WHEN FICO_SCORE >= 800 THEN 'Exceptional (800+)'
    END as FICO_SCORE_BUCKET,
    SUM(CASE WHEN TERM_LENGTH IS NOT NULL THEN 1 ELSE 0 END) as WITH_TERM_SELECTED,
    SUM(CASE WHEN TERM_LENGTH IS NOT NULL AND IS_CONFIRMED = 1 THEN 1 ELSE 0 END) as CONFIRMED_WITH_TERM,
    ROUND(SUM(CASE WHEN TERM_LENGTH IS NOT NULL AND IS_CONFIRMED = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN TERM_LENGTH IS NOT NULL THEN 1 ELSE 0 END), 0), 2) as CONFIRM_RATE_WITH_TERM,
    SUM(CASE WHEN TERM_LENGTH IS NULL THEN 1 ELSE 0 END) as WITHOUT_TERM_SELECTED,
    SUM(CASE WHEN TERM_LENGTH IS NULL AND IS_CONFIRMED = 1 THEN 1 ELSE 0 END) as CONFIRMED_WITHOUT_TERM,
    ROUND(SUM(CASE WHEN TERM_LENGTH IS NULL AND IS_CONFIRMED = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN TERM_LENGTH IS NULL THEN 1 ELSE 0 END), 0), 2) as CONFIRM_RATE_WITHOUT_TERM
FROM PROD__US.DBT_ANALYTICS.CHECKOUT_FUNNEL_V5
WHERE CHECKOUT_CREATED_DT >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 
    CASE FICO_SCORE_BUCKET
        WHEN 'Exceptional (800+)' THEN 1
        WHEN 'Very Good (740-799)' THEN 2
        WHEN 'Good (670-739)' THEN 3
        WHEN 'Fair (580-669)' THEN 4
        WHEN 'Poor (<580)' THEN 5
        WHEN 'No Score' THEN 6
    END
"""

# Query 3: AOV Dropoff
AOV_DROPOFF_QUERY = """
SELECT 
    CASE 
        WHEN TOTAL_AMOUNT < 150 THEN 'a. <$150'
        WHEN TOTAL_AMOUNT >= 150 AND TOTAL_AMOUNT < 500 THEN 'b. $150-$500'
        WHEN TOTAL_AMOUNT >= 500 AND TOTAL_AMOUNT < 1000 THEN 'c. $500-$1000'
        WHEN TOTAL_AMOUNT >= 1000 THEN 'd. $1000+'
    END as AOV_BUCKET,
    SUM(CASE WHEN IS_APPROVED = 1 THEN 1 ELSE 0 END) as APPROVED,
    SUM(CASE WHEN IS_APPROVED = 1 AND TERM_LENGTH IS NULL THEN 1 ELSE 0 END) as DROPPED_OFF,
    ROUND(SUM(CASE WHEN IS_APPROVED = 1 AND TERM_LENGTH IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN IS_APPROVED = 1 THEN 1 ELSE 0 END), 0), 1) as DROPOFF_PCT
FROM PROD__US.DBT_ANALYTICS.CHECKOUT_FUNNEL_V5
WHERE CHECKOUT_CREATED_DT >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 1
"""

# Query 4: 0% APR Impact
ZERO_APR_QUERY = """
SELECT 
    CASE 
        WHEN GREATEST(
            COALESCE(CASE WHEN OFFERED_APR1 = 0 THEN OFFERED_PLAN1_LENGTH ELSE 0 END, 0),
            COALESCE(CASE WHEN OFFERED_APR2 = 0 THEN OFFERED_PLAN2_LENGTH ELSE 0 END, 0),
            COALESCE(CASE WHEN OFFERED_APR3 = 0 THEN OFFERED_PLAN3_LENGTH ELSE 0 END, 0)
        ) = 0 THEN 'a. No 0% APR'
        WHEN GREATEST(
            COALESCE(CASE WHEN OFFERED_APR1 = 0 THEN OFFERED_PLAN1_LENGTH ELSE 0 END, 0),
            COALESCE(CASE WHEN OFFERED_APR2 = 0 THEN OFFERED_PLAN2_LENGTH ELSE 0 END, 0),
            COALESCE(CASE WHEN OFFERED_APR3 = 0 THEN OFFERED_PLAN3_LENGTH ELSE 0 END, 0)
        ) <= 6 THEN 'b. 0% for 1-6 mo'
        WHEN GREATEST(
            COALESCE(CASE WHEN OFFERED_APR1 = 0 THEN OFFERED_PLAN1_LENGTH ELSE 0 END, 0),
            COALESCE(CASE WHEN OFFERED_APR2 = 0 THEN OFFERED_PLAN2_LENGTH ELSE 0 END, 0),
            COALESCE(CASE WHEN OFFERED_APR3 = 0 THEN OFFERED_PLAN3_LENGTH ELSE 0 END, 0)
        ) <= 12 THEN 'c. 0% for 7-12 mo'
        ELSE 'd. 0% for 13+ mo'
    END as ZERO_APR_BUCKET,
    COUNT(*) as TOTAL_APPROVED,
    SUM(CASE WHEN TERM_LENGTH IS NOT NULL THEN 1 ELSE 0 END) as COMPLETED,
    SUM(CASE WHEN TERM_LENGTH IS NULL THEN 1 ELSE 0 END) as DROPPED_OFF,
    ROUND(SUM(CASE WHEN TERM_LENGTH IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as COMPLETION_RATE,
    ROUND(SUM(CASE WHEN TERM_LENGTH IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as DROPOFF_RATE
FROM PROD__US.DBT_ANALYTICS.CHECKOUT_FUNNEL_V5
WHERE CHECKOUT_CREATED_DT >= DATEADD(DAY, -30, CURRENT_DATE())
    AND TOTAL_AMOUNT >= 1000
    AND IS_APPROVED = 1
GROUP BY 1
ORDER BY 1
"""

# Query 5: FICO + AOV Matrix
FICO_AOV_MATRIX_QUERY = """
SELECT 
    CASE 
        WHEN FICO_SCORE >= 740 THEN 'High FICO (740+)'
        WHEN FICO_SCORE >= 670 AND FICO_SCORE < 740 THEN 'Good (670-739)'
        WHEN FICO_SCORE >= 580 AND FICO_SCORE < 670 THEN 'Fair (580-669)'
        WHEN FICO_SCORE < 580 THEN 'Poor (<580)'
        ELSE 'No Score'
    END as FICO_GROUP,
    CASE 
        WHEN TOTAL_AMOUNT < 150 THEN '<$150'
        WHEN TOTAL_AMOUNT >= 150 AND TOTAL_AMOUNT < 500 THEN '$150-$500'
        WHEN TOTAL_AMOUNT >= 500 AND TOTAL_AMOUNT < 1000 THEN '$500-$1000'
        WHEN TOTAL_AMOUNT >= 1000 THEN '$1000+'
    END as AOV_BUCKET,
    SUM(CASE WHEN IS_APPROVED = 1 THEN 1 ELSE 0 END) as APPROVED,
    SUM(CASE WHEN IS_APPROVED = 1 AND TERM_LENGTH IS NULL THEN 1 ELSE 0 END) as DROPPED_OFF,
    ROUND(SUM(CASE WHEN IS_APPROVED = 1 AND TERM_LENGTH IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN IS_APPROVED = 1 THEN 1 ELSE 0 END), 0), 1) as DROPOFF_PCT
FROM PROD__US.DBT_ANALYTICS.CHECKOUT_FUNNEL_V5
WHERE CHECKOUT_CREATED_DT >= DATEADD(DAY, -30, CURRENT_DATE())
AND FICO_SCORE IS NOT NULL
GROUP BY 1, 2
ORDER BY 
    CASE FICO_GROUP
        WHEN 'High FICO (740+)' THEN 1
        WHEN 'Good (670-739)' THEN 2
        WHEN 'Fair (580-669)' THEN 3
        WHEN 'Poor (<580)' THEN 4
    END, 
    CASE AOV_BUCKET
        WHEN '<$150' THEN 1
        WHEN '$150-$500' THEN 2
        WHEN '$500-$1000' THEN 3
        WHEN '$1000+' THEN 4
    END
"""

# Load data
try:
    with st.spinner("Loading data from Snowflake..."):
        df_fico = run_query(FICO_DROPOFF_QUERY)
        df_term_confirm = run_query(TERM_CONFIRM_QUERY)
        df_aov = run_query(AOV_DROPOFF_QUERY)
        df_zero_apr = run_query(ZERO_APR_QUERY)
        df_matrix = run_query(FICO_AOV_MATRIX_QUERY)
    
    # Summary metrics
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    
    total_approved = df_fico['APPROVED'].sum()
    total_dropped = df_fico['DROPPED_OFF'].sum()
    overall_dropoff = round(total_dropped / total_approved * 100, 1)
    
    with col1:
        st.metric("Total Approved", f"{total_approved:,.0f}")
    with col2:
        st.metric("Term Selected", f"{total_approved - total_dropped:,.0f}")
    with col3:
        st.metric("Dropped Off", f"{total_dropped:,.0f}")
    with col4:
        st.metric("Overall Dropoff Rate", f"{overall_dropoff}%")
    
    st.markdown("---")
    
    # Tab layout
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 FICO Dropoff", 
        "🔄 Term Selection Impact",
        "💰 AOV Analysis", 
        "🎯 0% APR Impact"
    ])
    
    # Tab 1: FICO Dropoff
    with tab1:
        st.subheader("Term Selection Dropoff by FICO Score")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Bar chart
            fig = px.bar(
                df_fico[df_fico['FICO_SCORE_BUCKET'] != 'No Score'],
                x='FICO_SCORE_BUCKET',
                y='DROPOFF_PCT',
                title='Dropoff Rate by FICO Bucket',
                labels={'DROPOFF_PCT': 'Dropoff %', 'FICO_SCORE_BUCKET': 'FICO Score'},
                color='DROPOFF_PCT',
                color_continuous_scale='Reds'
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Data table
            st.dataframe(
                df_fico[['FICO_SCORE_BUCKET', 'APPROVED', 'TERM_SELECTED', 'DROPPED_OFF', 'DROPOFF_PCT']].rename(columns={
                    'FICO_SCORE_BUCKET': 'FICO Bucket',
                    'APPROVED': 'Approved',
                    'TERM_SELECTED': 'Term Selected',
                    'DROPPED_OFF': 'Dropped Off',
                    'DROPOFF_PCT': 'Dropoff %'
                }),
                hide_index=True,
                use_container_width=True
            )
    
    # Tab 2: Term Selection Impact
    with tab2:
        st.subheader("Term Selection vs Loan Confirmation")
        st.markdown("**Key Insight:** Users who select a term confirm at ~99.8% rate vs ~1% without term selection")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Confirmation rate comparison
            confirm_data = df_term_confirm[df_term_confirm['FICO_SCORE_BUCKET'] != 'No Score'].melt(
                id_vars=['FICO_SCORE_BUCKET'],
                value_vars=['CONFIRM_RATE_WITH_TERM', 'CONFIRM_RATE_WITHOUT_TERM'],
                var_name='Type',
                value_name='Confirmation Rate'
            )
            confirm_data['Type'] = confirm_data['Type'].map({
                'CONFIRM_RATE_WITH_TERM': 'With Term Selected',
                'CONFIRM_RATE_WITHOUT_TERM': 'Without Term Selected'
            })
            
            fig = px.bar(
                confirm_data,
                x='FICO_SCORE_BUCKET',
                y='Confirmation Rate',
                color='Type',
                barmode='group',
                title='Confirmation Rate: With vs Without Term Selection'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.dataframe(
                df_term_confirm[['FICO_SCORE_BUCKET', 'WITH_TERM_SELECTED', 'CONFIRMED_WITH_TERM', 
                                'CONFIRM_RATE_WITH_TERM', 'WITHOUT_TERM_SELECTED', 'CONFIRMED_WITHOUT_TERM',
                                'CONFIRM_RATE_WITHOUT_TERM']].rename(columns={
                    'FICO_SCORE_BUCKET': 'FICO Bucket',
                    'WITH_TERM_SELECTED': 'With Term',
                    'CONFIRMED_WITH_TERM': 'Confirmed (Term)',
                    'CONFIRM_RATE_WITH_TERM': 'Confirm Rate (Term) %',
                    'WITHOUT_TERM_SELECTED': 'No Term',
                    'CONFIRMED_WITHOUT_TERM': 'Confirmed (No Term)',
                    'CONFIRM_RATE_WITHOUT_TERM': 'Confirm Rate (No Term) %'
                }),
                hide_index=True,
                use_container_width=True
            )
    
    # Tab 3: AOV Analysis
    with tab3:
        st.subheader("Term Selection Dropoff by Order Value")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                df_aov,
                x='AOV_BUCKET',
                y='DROPOFF_PCT',
                title='Dropoff Rate by AOV',
                labels={'DROPOFF_PCT': 'Dropoff %', 'AOV_BUCKET': 'Order Value'},
                color='DROPOFF_PCT',
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.dataframe(
                df_aov.rename(columns={
                    'AOV_BUCKET': 'AOV Bucket',
                    'APPROVED': 'Approved',
                    'DROPPED_OFF': 'Dropped Off',
                    'DROPOFF_PCT': 'Dropoff %'
                }),
                hide_index=True,
                use_container_width=True
            )
        
        # Heatmap
        st.subheader("FICO x AOV Dropoff Matrix")
        pivot = df_matrix.pivot(index='FICO_GROUP', columns='AOV_BUCKET', values='DROPOFF_PCT')
        pivot = pivot.reindex(['High FICO (740+)', 'Good (670-739)', 'Fair (580-669)', 'Poor (<580)'])
        pivot = pivot[['<$150', '$150-$500', '$500-$1000', '$1000+']]
        
        fig = px.imshow(
            pivot,
            labels=dict(x="AOV Bucket", y="FICO Group", color="Dropoff %"),
            color_continuous_scale='RdYlGn_r',
            aspect='auto',
            text_auto=True
        )
        fig.update_layout(title='Dropoff Rate by FICO and AOV')
        st.plotly_chart(fig, use_container_width=True)
    
    # Tab 4: 0% APR Impact
    with tab4:
        st.subheader("0% APR Offer Impact on Conversion ($1000+ Orders)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                df_zero_apr,
                x='ZERO_APR_BUCKET',
                y=['COMPLETION_RATE', 'DROPOFF_RATE'],
                title='Completion vs Dropoff by 0% APR Term',
                labels={'value': 'Rate %', 'ZERO_APR_BUCKET': '0% APR Offer'},
                barmode='stack'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.dataframe(
                df_zero_apr.rename(columns={
                    'ZERO_APR_BUCKET': '0% APR Term',
                    'TOTAL_APPROVED': 'Total Approved',
                    'COMPLETED': 'Completed',
                    'DROPPED_OFF': 'Dropped Off',
                    'COMPLETION_RATE': 'Completion %',
                    'DROPOFF_RATE': 'Dropoff %'
                }),
                hide_index=True,
                use_container_width=True
            )
        
        # Key insight
        st.info("""
        **Key Finding:** 
        - No 0% APR: 46.6% completion rate
        - 0% for 13+ months: 74.5% completion rate
        - Offering longer 0% terms increases conversion by ~28 percentage points
        """)

except Exception as e:
    st.error(f"Error connecting to Snowflake: {str(e)}")
    st.info("Please ensure your Snowflake credentials are configured in `.streamlit/secrets.toml`")

# Footer
st.markdown("---")
st.caption(f"Data refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Source: PROD__US.DBT_ANALYTICS.CHECKOUT_FUNNEL_V5")
