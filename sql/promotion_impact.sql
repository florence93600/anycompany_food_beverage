USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;

-- ==========================================================
-- 2.2 ANALYSE EXPLORATOIRE : PERFORMANCE GLOBALE PAR RÉGION
-- Utilité : Identifier les zones géographiques générant le plus de revenus.
-- Objectif : Prioriser les marchés clés pour les futures stratégies commerciales.
-- ==========================================================
SELECT 
    REGION, 
    SUM(AMOUNT) AS TOTAL_VENTES,
    COUNT(*) AS NB_TRANSACTIONS
FROM FINANCIAL_TRANSACTIONS_CLEAN
WHERE TRANSACTION_TYPE = 'Sale'
GROUP BY 1 ORDER BY 2 DESC;

--USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;

-- =====2.3 ANALYSES BUSINESS TRANSVERSES===============================
-- ANALYSE : Comparaison des ventes avec et sans promo
-- Pourquoi : Voir si on vend plus par jour quand il y a une offre.
-- Objectif : Vérifier si l'effort financier de la promo vaut le coup.
-- ==========================================================
WITH ventes_journalieres AS (
    SELECT TRANSACTION_DATE::DATE AS jour, REGION, SUM(amount) AS total
    FROM FINANCIAL_TRANSACTIONS_CLEAN 
    WHERE transaction_type = 'Sale' 
    GROUP BY 1, 2
)
SELECT 
    CASE WHEN p.PROMOTION_ID IS NOT NULL THEN 'Période Promo' ELSE 'Période Normale' END AS type_periode,
    ROUND(AVG(total), 2) AS ventes_moyennes_par_jour
FROM ventes_journalieres v
LEFT JOIN PROMOTIONS_DATA_CLEAN p 
    ON v.REGION = p.REGION AND v.jour BETWEEN p.START_DATE AND p.END_DATE
GROUP BY 1;

-- ==========================================================
-- ANALYSE : Sensibilité des catégories aux promos
-- Pourquoi : Quelles catégories on brade le plus ?
-- Objectif : Voir où sont concentrés nos rabais.
-- ==========================================================
SELECT 
    PRODUCT_CATEGORY,
    ROUND(AVG(DISCOUNT_PERCENTAGE) * 100, 1) AS remise_moyenne_pct,
    COUNT(PROMOTION_ID) AS nombre_de_promos
FROM PROMOTIONS_DATA_CLEAN
GROUP BY 1 
ORDER BY remise_moyenne_pct DESC;

-- ==========-- 2.3.4. - Opérations et logistiques ================================================
-- 2.3.4.1 - Rupture de stock
-- Objectif : Identifier les ruptures de stock par catégorie
-- ==========================================================
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
-- ==========================================================
-- 2.3.4.2 - IMPACT DES DÉLAIS DE LIVRAISON 
-- Objectif : Mesurer la ponctualité des transporteurs et les coûts associés.
-- Tables : ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
-- ==========================================================
WITH delivery_performance AS (
    SELECT 
        shipment_id,
        order_id,
        destination_region,
        shipping_method,
        ship_date,
        estimated_delivery,
        -- Date de livraison réelle (simulée ici par la fin de période si 'Delivered')
        -- Dans un cas réel, on utiliserait une colonne 'actual_delivery_date'
        DATEDIFF('day', ship_date, estimated_delivery) AS planned_duration,
        status,
        shipping_cost
    FROM SILVER.LOGISTICS_AND_SHIPPING_CLEAN
),

-- 2. Corrélation avec la satisfaction client (si disponible via les avis)
delivery_impact_analysis AS (
    SELECT 
        lp.destination_region,
        lp.shipping_method,
        AVG(lp.planned_duration) AS avg_delivery_time,
        AVG(lp.shipping_cost) AS avg_cost,
        COUNT(CASE WHEN lp.status = 'Returned' THEN 1 END) AS total_returns,
        -- Calcul du taux de retour par rapport au volume total
        (COUNT(CASE WHEN lp.status = 'Returned' THEN 1 END) / COUNT(lp.shipment_id)) * 100 AS return_rate_percentage
    FROM delivery_performance lp
    GROUP BY 1, 2
)
-- 3. Affichage des résultats pour le marketing et les opérations
SELECT 
    destination_region,
    shipping_method,
    ROUND(avg_delivery_time, 2) AS delai_moyen_jours,
    ROUND(avg_cost, 2) AS cout_moyen_transport,
    total_returns,
    ROUND(return_rate_percentage, 2) AS taux_de_retour_pourcent
FROM delivery_impact_analysis
ORDER BY taux_de_retour_pourcent DESC;