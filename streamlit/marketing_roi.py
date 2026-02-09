import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px

st.set_page_config(page_title="Marketing ROI & Expérience Client", layout="wide", page_icon="💰")

st.title("💰 Marketing ROI & Expérience Client")
st.markdown("Analyse de la rentabilité des campagnes marketing et de l'impact des avis / service client sur les ventes.")
st.markdown("---")

@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

conn = get_snowflake_connection()

# =========================================================
# 1) ROI par type de campagne (canal)
# =========================================================
st.header("📺 ROI par type de campagne marketing")

query_roi_canal = """
SELECT 
    CAMPAIGN_TYPE,
    ROUND(AVG(CONVERSION_RATE) * 100, 2) AS TAUX_CONV_THEORIQUE_PCT,
    ROUND(SUM(v.AMOUNT) / NULLIF(SUM(c.BUDGET), 0), 4) AS ROI_CALCULE,
    ROUND(AVG(v.AMOUNT), 2) AS PANIER_MOYEN_PAR_CANAL
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN c
LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN v 
    ON UPPER(TRIM(v.REGION)) = UPPER(TRIM(c.REGION))
    AND v.TRANSACTION_DATE BETWEEN c.START_DATE AND c.END_DATE
WHERE v.TRANSACTION_TYPE = 'Sale'
GROUP BY 1
ORDER BY TAUX_CONV_THEORIQUE_PCT DESC;
"""
df_roi_canal = pd.read_sql(query_roi_canal, conn)

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Résumé par canal")
    st.dataframe(df_roi_canal)

with col2:
    fig_roi = px.bar(
        df_roi_canal,
        x="CAMPAIGN_TYPE",
        y="ROI_CALCULE",
        title="ROI calculé par type de campagne (CA / Budget)",
        labels={"ROI_CALCULE": "ROI (€/€ investi)", "CAMPAIGN_TYPE": "Type de campagne"}
    )
    st.plotly_chart(fig_roi, use_container_width=True)

st.markdown("---")

# =========================================================
# 2) Comparaison taux de conversion théorique vs ROI réel
# =========================================================
st.header("🎯 Conversion théorique vs ROI réel")

fig_conv_vs_roi = px.scatter(
    df_roi_canal,
    x="TAUX_CONV_THEORIQUE_PCT",
    y="ROI_CALCULE",
    size="PANIER_MOYEN_PAR_CANAL",
    color="CAMPAIGN_TYPE",
    hover_name="CAMPAIGN_TYPE",
    title="Taux de conversion théorique vs ROI réel",
    labels={
        "TAUX_CONV_THEORIQUE_PCT": "Taux conv. théorique (%)",
        "ROI_CALCULE": "ROI réel (CA/Budget)"
    }
)
st.plotly_chart(fig_conv_vs_roi, use_container_width=True)

st.markdown("---")

# =========================================================
# 3) Impact des avis produits sur les ventes (global)
# =========================================================
st.header("⭐ Impact des avis produits sur les ventes")

query_reviews = """
WITH review_metrics AS (
    SELECT
        COUNT(review_id) AS total_reviews,
        AVG(rating) AS avg_rating
    FROM SILVER.product_reviews_clean
),
sales_metrics AS (
    SELECT
        COUNT(DISTINCT transaction_id) AS number_of_transactions,
        SUM(amount) AS total_sales_amount,
        AVG(amount) AS avg_transaction_amount
    FROM SILVER.financial_transactions_clean
    WHERE transaction_type = 'Sale'
)
SELECT
    r.total_reviews,
    r.avg_rating,
    s.number_of_transactions,
    s.total_sales_amount,
    s.avg_transaction_amount
FROM review_metrics r
CROSS JOIN sales_metrics s;
"""
df_reviews = pd.read_sql(query_reviews, conn)

col1, col2, col3 = st.columns(3)
row = df_reviews.iloc[0]

with col1:
    st.metric("Nombre total d'avis", f"{int(row['TOTAL_REVIEWS']):,}")
with col2:
    st.metric("Note moyenne produits", f"{row['AVG_RATING']:.2f} / 5")
with col3:
    st.metric("Montant total des ventes", f"{row['TOTAL_SALES_AMOUNT']:,.0f} €")

st.markdown("Nombre de transactions : **{:,}** – Panier moyen : **{:,.0f} €**".format(
    int(row["NUMBER_OF_TRANSACTIONS"]), row["AVG_TRANSACTION_AMOUNT"])
)

st.markdown("---")

# =========================================================
# 4) Influence des interactions service client (global)
# =========================================================
st.header("📞 Influence du service client sur les ventes")

query_service = """
WITH service_metrics AS (
    SELECT
        COUNT(interaction_id) AS total_interactions,
        COUNT(CASE WHEN resolution_status = 'Resolved' THEN 1 END) AS resolved_interactions
    FROM SILVER.customer_service_interactions_clean
),
sales_metrics AS (
    SELECT
        COUNT(DISTINCT transaction_id) AS number_of_transactions,
        SUM(amount) AS total_sales_amount,
        AVG(amount) AS avg_transaction_amount
    FROM SILVER.financial_transactions_clean
    WHERE transaction_type = 'Sale'
)
SELECT
    sm.total_interactions,
    sm.resolved_interactions,
    ROUND(
        sm.resolved_interactions / NULLIF(sm.total_interactions, 0) * 100,
        2
    ) AS resolution_rate_pct,
    s.number_of_transactions,
    s.total_sales_amount,
    s.avg_transaction_amount
FROM service_metrics sm
CROSS JOIN sales_metrics s;
"""
df_service = pd.read_sql(query_service, conn)
row_s = df_service.iloc[0]

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Interactions service client", f"{int(row_s['TOTAL_INTERACTIONS']):,}")
with col2:
    st.metric("Demandes résolues", f"{int(row_s['RESOLVED_INTERACTIONS']):,}")
with col3:
    st.metric("Taux de résolution", f"{row_s['RESOLUTION_RATE_PCT']:.1f} %")

st.markdown("Nombre de transactions : **{:,}** – CA total : **{:,.0f} €** – Panier moyen : **{:,.0f} €**".format(
    int(row_s["NUMBER_OF_TRANSACTIONS"]),
    row_s["TOTAL_SALES_AMOUNT"],
    row_s["AVG_TRANSACTION_AMOUNT"]
))

st.success("✅ Dashboard Marketing ROI & Expérience Client chargé avec succès !")
