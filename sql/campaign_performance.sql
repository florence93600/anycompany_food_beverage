USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;

-- 2.3 ANALYSE TRANSVERSE 
-------------------------------------------------------------
--2.3.2-- MARKETING ET PERFORMANCE COMMERCIALE 
USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;
---- ==========================================================
-- 2.3.2.1 - Lien campagnes ↔ ventes Identification des campagnes les plus efficaces
--RENTABILITÉ PAR CANAL (ROI GLOBAL)
-- Pourquoi : Identifier quels médias (TV, Social, Email...) sont les plus rentables.
-- Objectif : Comparer le CA généré par euro investi pour chaque canal.
-- ==========================================================

SELECT 
    CAMPAIGN_TYPE,
    -- Performance déclarée dans la table marketing
    ROUND(AVG(CONVERSION_RATE) * 100, 2) AS TAUX_CONV_THEORIQUE_PCT,
    -- Performance calculée (ce qui donne 0.02 actuellement)
    ROUND(SUM(v.AMOUNT) / NULLIF(SUM(c.BUDGET), 0), 4) AS ROI_CALCULE,
    -- Regardons le panier moyen par canal pour voir une différence
    ROUND(AVG(v.AMOUNT), 2) AS PANIER_MOYEN_PAR_CANAL
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN c
LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN v 
    ON UPPER(TRIM(v.REGION)) = UPPER(TRIM(c.REGION))
    AND v.TRANSACTION_DATE BETWEEN c.START_DATE AND c.END_DATE
WHERE v.TRANSACTION_TYPE = 'Sale'
GROUP BY 1
ORDER BY TAUX_CONV_THEORIQUE_PCT DESC;

------------------------------------------------------------------
--2.3.3-- EXPERIENCE CLIENT
---============================================================
  --2.3.3.1 – Impact des avis produits sur les ventes (global)

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

 --2.3.3.2 Influence des interactions service client (global)
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