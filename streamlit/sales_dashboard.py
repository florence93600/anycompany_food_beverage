import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px

# ---------------------------------------------------------
# Configuration générale
# ---------------------------------------------------------
st.set_page_config(page_title="Sales Dashboard", layout="wide", page_icon="📊")

st.title("📊 Sales Dashboard – AnyCompany Food & Beverage")
st.markdown("Vue d'ensemble des ventes, de la dynamique régionale, des profils clients et de l'effet des promotions.")
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
# 1) Ventes annuelles
# =========================================================
st.header("📈 Ventes annuelles")

query_ventes_annuelles = """
SELECT 
    YEAR(transaction_date) AS annee, 
    COUNT(*) AS nombre_vraies_ventes,
    SUM(amount) AS chiffre_affaires_total
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN
WHERE transaction_type = 'Sale'
GROUP BY 1
ORDER BY annee;
"""
df_ventes_annuelles = pd.read_sql(query_ventes_annuelles, conn)

col1, col2 = st.columns([1, 2])

with col1:
    st.metric("CA total (toutes années)", f"{df_ventes_annuelles['CHIFFRE_AFFAIRES_TOTAL'].sum():,.0f} €")
    st.metric("Nombre total de ventes", f"{df_ventes_annuelles['NOMBRE_VRAIES_VENTES'].sum():,}")

with col2:
    fig_ca_annee = px.bar(
        df_ventes_annuelles,
        x="ANNEE",
        y="CHIFFRE_AFFAIRES_TOTAL",
        title="Chiffre d'affaires par année",
        labels={"CHIFFRE_AFFAIRES_TOTAL": "CA (€)", "ANNEE": "Année"}
    )
    fig_ca_annee.update_layout(yaxis_tickformat=",")
    st.plotly_chart(fig_ca_annee, use_container_width=True)

st.markdown("---")

# =========================================================
# 2) Ventes mensuelles par région
# =========================================================
st.header("🌍 Ventes mensuelles par région")

query_ventes_region = """
SELECT 
    DATE_TRUNC('MONTH', transaction_date) AS mois,
    region,
    SUM(amount) AS chiffre_affaires,
    COUNT(transaction_id) AS nb_ventes,
    ROUND(
        (SUM(amount) 
         - LAG(SUM(amount)) OVER (PARTITION BY region ORDER BY DATE_TRUNC('MONTH', transaction_date)))
        / NULLIF(LAG(SUM(amount)) OVER (PARTITION BY region ORDER BY DATE_TRUNC('MONTH', transaction_date)), 0)
        * 100,
        2
    ) AS croissance_pct
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN
WHERE transaction_type = 'Sale'
GROUP BY 1, region
ORDER BY 1, region;
"""
df_ventes_region = pd.read_sql(query_ventes_region, conn)

regions = sorted(df_ventes_region["REGION"].unique())
region_sel = st.selectbox("Choisir une région", regions)

df_region_sel = df_ventes_region[df_ventes_region["REGION"] == region_sel].sort_values("MOIS").copy()

col1, col2 = st.columns(2)

with col1:
    fig_mois = px.line(
        df_region_sel,
        x="MOIS",
        y="CHIFFRE_AFFAIRES",
        title=f"CA mensuel – {region_sel}",
        markers=True,
        labels={"CHIFFRE_AFFAIRES": "CA (€)", "MOIS": ""}
    )
    fig_mois.update_layout(yaxis_tickformat=",")
    st.plotly_chart(fig_mois, use_container_width=True)

with col2:
    # on limite l'échelle pour mieux voir, sans regrouper les données
    df_region_sel["CROISSANCE_PCT_CLIPPED"] = df_region_sel["CROISSANCE_PCT"].clip(-100, 100)
    fig_croissance = px.bar(
        df_region_sel,
        x="MOIS",
        y="CROISSANCE_PCT_CLIPPED",
        title=f"Croissance mensuelle du CA – {region_sel} (entre -100% et +100%)",
        labels={"CROISSANCE_PCT_CLIPPED": "Croissance (%)", "MOIS": ""}
    )
    st.plotly_chart(fig_croissance, use_container_width=True)

st.markdown("---")

# =========================================================
# 3) Segmentation démographique clients
# =========================================================
st.header("👥 Segmentation démographique clients")

query_segmentation = """
WITH customer_age AS (
    SELECT
        customer_id,
        gender,
        marital_status,
        region,
        country,
        annual_income,
        DATE_PART('year', CURRENT_DATE) - DATE_PART('year', date_of_birth) AS age
    FROM SILVER.customer_demographics_clean
)
SELECT
    gender,
    marital_status,
    region,
    country,
    CASE
        WHEN age < 25 THEN 'Under 25'
        WHEN age BETWEEN 25 AND 34 THEN '25-34'
        WHEN age BETWEEN 35 AND 44 THEN '35-44'
        WHEN age BETWEEN 45 AND 54 THEN '45-54'
        WHEN age BETWEEN 55 AND 64 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    COUNT(customer_id) AS total_customers,
    AVG(annual_income) AS avg_annual_income
FROM customer_age
GROUP BY
    gender,
    marital_status,
    region,
    country,
    age_group
ORDER BY
    total_customers DESC;
"""
df_seg = pd.read_sql(query_segmentation, conn)

# Répartition par tranche d'âge et genre
ordre_age = ["Under 25", "25-34", "35-44", "45-54", "55-64", "65+"]

df_age = (
    df_seg.groupby(["AGE_GROUP", "GENDER"], as_index=False)
    .agg({"TOTAL_CUSTOMERS": "sum"})
)
df_age["AGE_GROUP"] = pd.Categorical(df_age["AGE_GROUP"], categories=ordre_age, ordered=True)
df_age = df_age.sort_values("AGE_GROUP")

st.subheader("Répartition par tranche d'âge et genre")
fig_age = px.bar(
    df_age,
    x="AGE_GROUP",
    y="TOTAL_CUSTOMERS",
    color="GENDER",
    barmode="group",
    title="Nombre de clients par tranche d'âge et genre",
    labels={"TOTAL_CUSTOMERS": "Nb clients", "AGE_GROUP": "Tranche d'âge"}
)
st.plotly_chart(fig_age, use_container_width=True)

# Revenu moyen par genre
st.subheader("Revenu moyen par genre")

df_revenu_genre = (
    df_seg.groupby("GENDER", as_index=False)
    .agg({"AVG_ANNUAL_INCOME": "mean"})
)

fig_revenu = px.bar(
    df_revenu_genre,
    x="GENDER",
    y="AVG_ANNUAL_INCOME",
    title="Revenu annuel moyen par genre",
    labels={"AVG_ANNUAL_INCOME": "Revenu moyen (€)"}
)
fig_revenu.update_layout(yaxis_tickformat=",")
st.plotly_chart(fig_revenu, use_container_width=True)

st.markdown("---")

# =========================================================
# 4) Rappel : Ventes avec vs sans promotion (vue globale)
# =========================================================
st.header("🛒 Rappel : Ventes avec vs sans promotion (vue globale)")

query_ventes_promo = """
WITH ventes_taggees AS (
    SELECT 
        v.TRANSACTION_ID,
        v.AMOUNT,
        v.REGION,
        v.TRANSACTION_DATE,
        CASE WHEN p.PROMOTION_ID IS NOT NULL 
             THEN 'Période Promo' 
             ELSE 'Période hors promo' 
        END AS SITUATION
    FROM FINANCIAL_TRANSACTIONS_CLEAN v
    LEFT JOIN PROMOTIONS_DATA_CLEAN p 
        ON UPPER(TRIM(v.REGION)) = UPPER(TRIM(p.REGION))
        AND v.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
    WHERE v.TRANSACTION_TYPE = 'Sale'
)
SELECT 
    SITUATION,
    COUNT(*) AS nombre_de_ventes,
    ROUND(SUM(AMOUNT), 2) AS chiffre_affaires_total,
    ROUND(AVG(AMOUNT), 2) AS panier_moyen
FROM ventes_taggees
GROUP BY 1;
"""
df_ventes_promo_sales = pd.read_sql(query_ventes_promo, conn)

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Résumé chiffré")
    st.dataframe(df_ventes_promo_sales)

with col2:
    fig_promo_sales = px.bar(
        df_ventes_promo_sales,
        x="SITUATION",
        y="PANIER_MOYEN",
        title="Panier moyen – périodes promo vs hors promo",
        labels={"PANIER_MOYEN": "Panier moyen (€)"}
    )
    fig_promo_sales.update_layout(yaxis_tickformat=",")
    st.plotly_chart(fig_promo_sales, use_container_width=True)

st.success("✅ Sales Dashboard chargé avec succès !")
