import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px

st.set_page_config(page_title="Promotion & Logistique", layout="wide", page_icon="🏷️")

st.title("🏷️ Analyses Promotions & Opérations")
st.markdown("Impact des promotions, sensibilité des catégories, ruptures de stock et performance logistique.")
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
# 1) Performance globale par région
# =========================================================
st.header("🌍 Performance globale par région")

query_region_perf = """
SELECT 
    REGION, 
    SUM(AMOUNT) AS TOTAL_VENTES,
    COUNT(*) AS NB_TRANSACTIONS
FROM FINANCIAL_TRANSACTIONS_CLEAN
WHERE TRANSACTION_TYPE = 'Sale'
GROUP BY 1 
ORDER BY 2 DESC;
"""
df_region_perf = pd.read_sql(query_region_perf, conn)

col1, col2 = st.columns([1, 2])
with col1:
    st.metric("Chiffre d'affaires total", f"{df_region_perf['TOTAL_VENTES'].sum():,.0f} €")
    st.metric("Nombre total de transactions", f"{df_region_perf['NB_TRANSACTIONS'].sum():,}")

with col2:
    fig_region_perf = px.bar(
        df_region_perf,
        x="REGION",
        y="TOTAL_VENTES",
        title="Chiffre d'affaires par région",
        labels={"TOTAL_VENTES": "CA total (€)"}
    )
    fig_region_perf.update_layout(xaxis_title="", yaxis_title="CA total (€)")
    st.plotly_chart(fig_region_perf, use_container_width=True)

st.markdown("---")

# =========================================================
# 2) Ventes avec vs sans promotion
# =========================================================
st.header("🚀 Ventes moyennes avec vs sans promotion")

query_ventes_promo = """
WITH ventes_journalieres AS (
    SELECT TRANSACTION_DATE::DATE AS jour, REGION, SUM(AMOUNT) AS total
    FROM FINANCIAL_TRANSACTIONS_CLEAN 
    WHERE TRANSACTION_TYPE = 'Sale' 
    GROUP BY 1, 2
)
SELECT 
    CASE WHEN p.PROMOTION_ID IS NOT NULL 
         THEN 'Période Promo' 
         ELSE 'Période Normale' 
    END AS type_periode,
    ROUND(AVG(total), 2) AS ventes_moyennes_par_jour
FROM ventes_journalieres v
LEFT JOIN PROMOTIONS_DATA_CLEAN p 
    ON v.REGION = p.REGION AND v.jour BETWEEN p.START_DATE AND p.END_DATE
GROUP BY 1;
"""
df_ventes_promo = pd.read_sql(query_ventes_promo, conn)

col1, col2 = st.columns([1, 2])

with col1:
    try:
        v_promo = df_ventes_promo.loc[
            df_ventes_promo["TYPE_PERIODE"] == "Période Promo",
            "VENTES_MOYENNES_PAR_JOUR"
        ].values[0]
        v_normale = df_ventes_promo.loc[
            df_ventes_promo["TYPE_PERIODE"] == "Période Normale",
            "VENTES_MOYENNES_PAR_JOUR"
        ].values[0]
        uplift_pct = ((v_promo - v_normale) / v_normale) * 100 if v_normale != 0 else 0
    except IndexError:
        v_promo, v_normale, uplift_pct = 0, 0, 0

    st.metric("Ventes moyennes (Période Promo)", f"{v_promo:,.0f} €")
    st.metric("Ventes moyennes (Période Normale)", f"{v_normale:,.0f} €")
    st.metric("Effet Boost (Uplift)", f"{uplift_pct:+.1f} %")

with col2:
    fig_ventes_promo = px.bar(
        df_ventes_promo,
        x="TYPE_PERIODE",
        y="VENTES_MOYENNES_PAR_JOUR",
        title="Ventes moyennes par jour : promo vs normal",
        labels={"VENTES_MOYENNES_PAR_JOUR": "Ventes moyennes/jour (€)"}
    )
    fig_ventes_promo.update_layout(xaxis_title="", yaxis_title="Ventes moyennes/jour (€)")
    st.plotly_chart(fig_ventes_promo, use_container_width=True)

st.markdown("---")

# =========================================================
# 3) Sensibilité des catégories aux promotions (Lift)
# =========================================================
st.header("📈 Sensibilité des catégories aux promotions (Lift)")

query_lift_categories = """
WITH stats_globales_region AS (
    SELECT 
        REGION,
        AVG(AMOUNT) AS PANIER_MOYEN_REGION_GLOBAL
    FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
    WHERE TRANSACTION_TYPE = 'Sale'
    GROUP BY 1
),
ventes_promo AS (
    SELECT 
        p.PRODUCT_CATEGORY,
        v.REGION,
        AVG(v.AMOUNT) AS PANIER_MOYEN_PROMO,
        COUNT(*) AS NB_VENTES_PROMO
    FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN v
    INNER JOIN ANYCOMPANY_LAB.SILVER.PROMOTIONS_DATA_CLEAN p 
        ON UPPER(TRIM(v.REGION)) = UPPER(TRIM(p.REGION))
        AND v.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
    WHERE v.TRANSACTION_TYPE = 'Sale'
    GROUP BY 1, 2
)
SELECT 
    vp.PRODUCT_CATEGORY,
    ROUND(AVG(vp.PANIER_MOYEN_PROMO), 2) AS PANIER_MOYEN_EN_PROMO,
    ROUND(AVG(sgr.PANIER_MOYEN_REGION_GLOBAL), 2) AS PANIER_MOYEN_BASE_REGION,
    ROUND(
        ((PANIER_MOYEN_EN_PROMO - PANIER_MOYEN_BASE_REGION)
         / NULLIF(PANIER_MOYEN_BASE_REGION, 0)) * 100, 
    2) AS LIFT_PERFORMANCE_PCT
FROM ventes_promo vp
JOIN stats_globales_region sgr ON vp.REGION = sgr.REGION
GROUP BY 1
ORDER BY LIFT_PERFORMANCE_PCT DESC;
"""
df_lift = pd.read_sql(query_lift_categories, conn)

df_lift["SENSIBILITE"] = df_lift["LIFT_PERFORMANCE_PCT"].apply(
    lambda x: "Forte" if x > 5 else ("Modérée" if x >= 0 else "Négative")
)

fig_lift = px.bar(
    df_lift,
    x="PRODUCT_CATEGORY",
    y="LIFT_PERFORMANCE_PCT",
    color="SENSIBILITE",
    title="Sensibilité aux promotions par catégorie (Lift %)",
    labels={"LIFT_PERFORMANCE_PCT": "Lift (%)"}
)
fig_lift.update_layout(xaxis_title="", yaxis_title="Lift (%)")
st.plotly_chart(fig_lift, use_container_width=True)

st.markdown("---")

# =========================================================
# 4) Ruptures de stock – visuel amélioré
# =========================================================
st.header("📦 Ruptures de stock par catégorie")

query_stock = """
SELECT
    product_category,
    COUNT(*) AS total_products,
    COUNT(
        CASE
            WHEN current_stock <= reorder_point THEN 1
        END
    ) AS products_in_stockout,
    ROUND(
        COUNT(
            CASE
                WHEN current_stock <= reorder_point THEN 1
            END
        ) / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS stockout_rate_pct
FROM SILVER.inventory_clean
GROUP BY product_category
ORDER BY stockout_rate_pct DESC;
"""
df_stock = pd.read_sql(query_stock, conn)

st.subheader("Vue détaillée des catégories")
st.dataframe(df_stock)

top_stock = df_stock.sort_values("STOCKOUT_RATE_PCT", ascending=False).head(5)

col1, col2 = st.columns(2)

with col1:
    fig_stock_rate = px.bar(
        top_stock,
        x="PRODUCT_CATEGORY",
        y="STOCKOUT_RATE_PCT",
        title="Top 5 catégories – taux de rupture (%)",
        labels={"STOCKOUT_RATE_PCT": "Taux de rupture (%)"}
    )
    fig_stock_rate.update_layout(xaxis_title="", yaxis_title="Taux de rupture (%)")
    st.plotly_chart(fig_stock_rate, use_container_width=True)

with col2:
    fig_stock_count = px.bar(
        top_stock,
        x="PRODUCT_CATEGORY",
        y="PRODUCTS_IN_STOCKOUT",
        title="Top 5 catégories – nb de produits en rupture",
        labels={"PRODUCTS_IN_STOCKOUT": "Nb produits en rupture"}
    )
    fig_stock_count.update_layout(xaxis_title="", yaxis_title="Nb produits en rupture")
    st.plotly_chart(fig_stock_count, use_container_width=True)

st.markdown("---")

# =========================================================
# 5) Impact des délais de livraison – visuel amélioré
# =========================================================
st.header("🚚 Impact des délais de livraison et retours")

query_logistique = """
WITH delivery_performance AS (
    SELECT 
        shipment_id,
        order_id,
        destination_region,
        shipping_method,
        ship_date,
        estimated_delivery,
        DATEDIFF('day', ship_date, estimated_delivery) AS planned_duration,
        status,
        shipping_cost
    FROM SILVER.LOGISTICS_AND_SHIPPING_CLEAN
),
delivery_impact_analysis AS (
    SELECT 
        lp.destination_region,
        lp.shipping_method,
        AVG(lp.planned_duration) AS avg_delivery_time,
        AVG(lp.shipping_cost) AS avg_cost,
        COUNT(CASE WHEN lp.status = 'Returned' THEN 1 END) AS total_returns,
        (COUNT(CASE WHEN lp.status = 'Returned' THEN 1 END) / COUNT(lp.shipment_id)) * 100 AS return_rate_percentage
    FROM delivery_performance lp
    GROUP BY 1, 2
)
SELECT 
    destination_region,
    shipping_method,
    ROUND(avg_delivery_time, 2) AS delai_moyen_jours,
    ROUND(avg_cost, 2) AS cout_moyen_transport,
    total_returns,
    ROUND(return_rate_percentage, 2) AS taux_de_retour_pourcent
FROM delivery_impact_analysis
ORDER BY taux_de_retour_pourcent DESC;
"""
df_logistique = pd.read_sql(query_logistique, conn)

st.subheader("Vue détaillée des performances logistiques")
st.dataframe(df_logistique)

top_log = df_logistique.sort_values("TAUX_DE_RETOUR_POURCENT", ascending=False).head(10)

col1, col2 = st.columns(2)

with col1:
    fig_retours = px.bar(
        top_log,
        x="DESTINATION_REGION",
        y="TAUX_DE_RETOUR_POURCENT",
        color="SHIPPING_METHOD",
        title="Top régions – taux de retour (%) par méthode",
        labels={"TAUX_DE_RETOUR_POURCENT": "Taux de retour (%)"}
    )
    fig_retours.update_layout(xaxis_title="", yaxis_title="Taux de retour (%)")
    st.plotly_chart(fig_retours, use_container_width=True)

with col2:
    fig_delai = px.bar(
        top_log,
        x="DESTINATION_REGION",
        y="DELAI_MOYEN_JOURS",
        color="SHIPPING_METHOD",
        title="Top régions – délai moyen de livraison (jours)",
        labels={"DELAI_MOYEN_JOURS": "Délai moyen (jours)"}
    )
    fig_delai.update_layout(xaxis_title="", yaxis_title="Délai moyen (jours)")
    st.plotly_chart(fig_delai, use_container_width=True)

st.success("✅ Dashboard Promotions & Logistique chargé avec succès !")
