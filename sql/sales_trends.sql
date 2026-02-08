USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;

-- ==========================================================
-- 2.1 ANALYSE EXPLORATOIRE : ÉVOLUTION DES VENTES DANS LE TEMPS
-- Utilité : Suivre la santé financière de l'entreprise mois par année 
-- Objectif : Détecter les périodes de croissance ou de baisse saisonnière.
-- ==========================================================

SELECT 
    YEAR(transaction_date) as annee, 
    COUNT(*) as nombre_vraies_ventes,
    SUM(amount) as chiffre_affaires_total
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN
WHERE transaction_type = 'Sale'
GROUP BY 1
ORDER BY annee DESC;

-- ==========================================================
-- 2.2 ANALYSE EXPLORATOIRE : ÉVOLUTION DES VENTES PAR REGION DANS LE TEMPS
-- Utilité : Suivre la santé financière de l'entreprise mois par mois.
-- Objectif : Détecter les périodes de croissance ou de baisse saisonnière.
-- ==========================================================
SELECT 
    DATE_TRUNC('MONTH', transaction_date) AS mois,
    region,
    SUM(amount) AS chiffre_affaires,
    COUNT(transaction_id) AS nb_ventes,
    -- Calcul de la croissance par rapport au mois précédent
    ROUND(((chiffre_affaires - LAG(chiffre_affaires) OVER (ORDER BY mois)) 
          / NULLIF(LAG(chiffre_affaires) OVER (ORDER BY mois), 0)) * 100, 2) AS croissance_pct
FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN
WHERE transaction_type = 'Sale'
GROUP BY 1, region
ORDER BY 1, region;

-- ==========================================================
-- 2.2 ANALYSE EXPLORATOIRE : SEGMENTATION DÉMOGRAPHIQUE
-- Utilité : Connaître le profil de revenu moyen selon le genre.
-- Objectif : Adapter la communication marketing au pouvoir d'achat des segments.
-- ==========================================================
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



-- ======Partie 2.3 – Analyses business transverses : Ventes et promotions=====================
-- 2.3.1.1 - Ventes et promotions

---COMPARAISON DES VENTES : AVEC vs SANS PROMOTION
-- Pourquoi : Savoir si nos promos génèrent un "boost" de chiffre d'affaires.
-- Objectif : Calculer le panier moyen et le volume de ventes selon la période.
-- =====================================================================

WITH ventes_taggees AS (
    SELECT 
        v.TRANSACTION_ID,
        v.AMOUNT,
        v.REGION,
        v.TRANSACTION_DATE,
        -- On regarde s'il existe une promo pour cette région à cette date
        CASE WHEN p.PROMOTION_ID IS NOT NULL THEN 'Période Promo' ELSE 'Période hors promo' END AS SITUATION
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


-- ==========================================================
--2.3.1.2 ANALYSE DE SENSIBILITÉ : CATEGORIES vs PROMOTIONS
-- Pourquoi : Comparer le succès d'une catégorie en promo face au reste du marché.
-- Objectif : Calculer le Lift (boost) réel généré par catégorie.
-- ==========================================================

WITH stats_globales_region AS (
    -- On calcule le panier moyen "normal" par région pour avoir une base de comparaison
    SELECT 
        REGION,
        AVG(AMOUNT) AS PANIER_MOYEN_REGION_GLOBAL
    FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
    WHERE TRANSACTION_TYPE = 'Sale'
    GROUP BY 1
),
ventes_promo AS (
    -- On isole les ventes qui ont bénéficié d'une promo par catégorie
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
    -- Calcul du Lift : Performance de la catégorie promo vs moyenne habituelle de la région
    ROUND(
        ((PANIER_MOYEN_EN_PROMO - PANIER_MOYEN_BASE_REGION) / NULLIF(PANIER_MOYEN_BASE_REGION, 0)) * 100, 
    2) AS LIFT_PERFORMANCE_PCT,
    SUM(vp.NB_VENTES_PROMO) AS TOTAL_VENTES_PROMO
FROM ventes_promo vp
JOIN stats_globales_region sgr ON vp.REGION = sgr.REGION
GROUP BY 1
ORDER BY LIFT_PERFORMANCE_PCT DESC;