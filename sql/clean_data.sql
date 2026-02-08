
-- Phase 1– Data Preparation & Ingestion-------------------------------------------------------------------------------------------

--Etape 4- Nettoyage de données-----------------------------------------------------------------------------------------------------
USE DATABASE ANYCOMPANY_LAB;

--Etape 4- Nettoyage de données-----------------------------------------------------------------------------------------------------
--1.Nettoyage de la table BRONZE.customer_demographics------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN AS
SELECT 
    customer_id::NUMBER AS customer_id,
    name::TEXT AS name,
    date_of_birth::DATE AS date_of_birth,
    gender::TEXT AS gender,
    region::TEXT AS region,
    country::TEXT AS country,
    city::TEXT AS city,
    marital_status::TEXT AS marital_status,
    -- Nettoyage symboles ($) et typage strict
    REGEXP_REPLACE(annual_income, '[^0-9.]', '')::NUMBER(12,2) AS annual_income
FROM BRONZE.CUSTOMER_DEMOGRAPHICS
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) = 1;
--Affichage de la table SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
SELECT * FROM SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN LIMIT 10;

--2.Nettoyage de la table BRONZE.customer_service_interactions-------------------------------------------------
CREATE OR REPLACE TABLE SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN AS
SELECT 
    interaction_id::TEXT AS interaction_id,
    interaction_date::DATE AS interaction_date,
    interaction_type::TEXT AS interaction_type,
    issue_category::TEXT AS issue_category,
    description::TEXT AS description,
    duration_minutes::NUMBER AS duration_minutes,
    resolution_status::TEXT AS resolution_status,
    follow_up_required::TEXT AS follow_up_required,
    customer_satisfaction::NUMBER AS customer_satisfaction
FROM BRONZE.CUSTOMER_SERVICE_INTERACTIONS
QUALIFY ROW_NUMBER() OVER (PARTITION BY interaction_id ORDER BY interaction_date DESC) = 1;

--Affichage de la table SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN
SELECT * FROM SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN LIMIT 10;

--3.Nettoyage de la table BRONZE.financial_transactions-----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.FINANCIAL_TRANSACTIONS_CLEAN AS
SELECT 
    transaction_id::TEXT AS transaction_id,
    transaction_date::DATE AS transaction_date,
    transaction_type::TEXT AS transaction_type,
    REGEXP_REPLACE(amount, '[^0-9.]', '')::NUMBER(12,2) AS amount,
    payment_method::TEXT AS payment_method,
    entity::TEXT AS entity,
    region::TEXT AS region,
    account_code::TEXT AS account_code
FROM BRONZE.FINANCIAL_TRANSACTIONS
WHERE transaction_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_date) = 1;

--Affichage de la table SILVER.FINANCIAL_TRANSACTIONS_CLEAN
SELECT * FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN LIMIT 10;

--4.Nettoyage de la table BRONZE.promotions_data-----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.PROMOTIONS_DATA_CLEAN AS
SELECT 
    promotion_id::TEXT AS promotion_id,
    product_category::TEXT AS product_category,
    promotion_type::TEXT AS promotion_type,
    REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')::NUMBER(5,4) AS discount_percentage,
    start_date::DATE AS start_date,
    end_date::DATE AS end_date,
    region::TEXT AS region
FROM BRONZE.PROMOTIONS_DATA
QUALIFY ROW_NUMBER() OVER (PARTITION BY promotion_id ORDER BY start_date) = 1;
--Affichage de la table SILVER.PROMOTIONS_DATA_CLEAN
SELECT * FROM SILVER.PROMOTIONS_DATA_CLEAN LIMIT 10;

--5.Nettoyage de la table BRONZE.marketing_campaigns-----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.MARKETING_CAMPAIGNS_CLEAN AS
SELECT 
    campaign_id::TEXT AS campaign_id,
    campaign_name::TEXT AS campaign_name,
    campaign_type::TEXT AS campaign_type,
    product_category::TEXT AS product_category,
    target_audience::TEXT AS target_audience,
    start_date::DATE AS start_date,
    end_date::DATE AS end_date,
    region::TEXT AS region,
    REGEXP_REPLACE(budget, '[^0-9.]', '')::NUMBER(12,2) AS budget,
    reach::NUMBER AS reach,
    REGEXP_REPLACE(conversion_rate, '[^0-9.]', '')::NUMBER(6,4) AS conversion_rate
FROM BRONZE.MARKETING_CAMPAIGNS
QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY start_date) = 1;

--Affichage de la table SILVER.MARKETING_CAMPAIGNS_CLEAN
SELECT * FROM SILVER.MARKETING_CAMPAIGNS_CLEAN LIMIT 10;

--6.Nettoyage de la table BRONZE.product_reviews----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.PRODUCT_REVIEWS_CLEAN AS
SELECT 
    -- 1. REVIEW ID
    -- On capture la première série de chiffres consécutifs (l'identifiant unique).
    CAST(REGEXP_SUBSTR(raw_line, '^(\\d+)') AS INTEGER) AS review_id,

    -- 2. PRODUCT ID
    -- \S+ signifie "tout sauf un espace", parfait pour un ID
    REGEXP_SUBSTR(raw_line, '^\\d+\\s+(\\S+)', 1, 1, 'e', 1) AS product_id,

    -- 3. REVIEWER ID
    -- On saute l'ID ligne, l'espace, l'ID produit, l'espace, et on prend le suivant
    REGEXP_SUBSTR(raw_line, '^\\d+\\s+\\S+\\s+(\\S+)', 1, 1, 'e', 1) AS reviewer_id,

    -- 4. RATING
    -- On cherche le chiffre unique juste avant la date (YYYY-MM-DD)
    CAST(REGEXP_SUBSTR(raw_line, '(\\d+)\\s+\\d{4}-\\d{2}-\\d{2}', 1, 1, 'e', 1) AS INTEGER) AS rating,

    -- 5. REVIEW DATE
    CAST(REGEXP_SUBSTR(raw_line, '\\d{4}-\\d{2}-\\d{2}') AS DATE) AS review_date,

    -- 6. REVIEW TITLE (Heuristique : Première phrase terminée par ponctuation)
    TRIM(REGEXP_SUBSTR(raw_line, '\\d{2}:\\d{2}:\\d{2}\\s+([^\\.\\!\\?]+[\\.\\!\\?])', 1, 1, 'e', 1)) AS review_title,

    -- 7. REVIEW TEXT (Tout le reste après l'heure)
    TRIM(REGEXP_SUBSTR(raw_line, '\\d{2}:\\d{2}:\\d{2}\\s+(.*)$', 1, 1, 'e', 1)) AS review_text

FROM BRONZE.PRODUCT_REVIEWS;
--Affichage de la table SILVER.PRODUCT_REVIEWS_CLEAN
SELECT * FROM SILVER.PRODUCT_REVIEWS_CLEAN LIMIT 10;
--Nous avons décidé de supprimer la colonne reviewer_name pour ne conserver que le reviewer_id. Cette décision supprime la redondance d'information et fiabilise notre processus d'ingestion en éliminant les risques d'erreurs liés aux caractères spéciaux souvent présents dans les noms d'utilisateurs.
--Vérification de la présence des doublons
WITH verification_doublons AS (
    -- Votre requête d'origine (mise dans une "boîte" temporaire)
    SELECT 
        REVIEW_ID,
        COUNT(*) as nombre_apparitions
    FROM SILVER.PRODUCT_REVIEWS_CLEAN
    GROUP BY REVIEW_ID
    HAVING COUNT(*) > 1
)
SELECT 
    CASE 
        WHEN COUNT(*) > 0 THEN 'ATTENTION : Il y a ' || COUNT(*) || ' doublons dans la table !'
        ELSE 'SUCCÈS : Aucun doublon trouvé, la table est propre.'
    END AS MESSAGE_DE_CONTROLE
FROM verification_doublons;

--7.Nettoyage de la table BRONZE.inventory----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.INVENTORY_CLEAN AS
SELECT 
    -- 1. Textes
    $1:product_id::TEXT       AS product_id,
    $1:product_category::TEXT AS product_category,
    $1:warehouse::TEXT        AS warehouse,
    $1:region::TEXT           AS region,
    $1:country::TEXT          AS country,
    $1:current_stock::INTEGER AS current_stock,
    $1:reorder_point::INTEGER AS reorder_point,
    $1:lead_time::INTEGER     AS lead_time,
    $1:last_restock_date::DATE AS last_restock_date

FROM BRONZE.INVENTORY
WHERE product_id IS NOT NULL
-- Dédoublonnage strict
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, warehouse ORDER BY last_restock_date DESC) = 1;

--Affichage de la table SILVER.INVENTORY_CLEAN
SELECT * FROM SILVER.INVENTORY_CLEAN LIMIT 10;
--8.Nettoyage de la table BRONZE.store_locations----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.STORE_LOCATIONS_CLEAN AS
SELECT 
    $1:store_id::TEXT    AS store_id,
    $1:store_name::TEXT  AS store_name,
    $1:store_type::TEXT  AS store_type,
    $1:address::TEXT     AS address,
    $1:city::TEXT        AS city,
    $1:postal_code::TEXT AS postal_code,
    $1:region::TEXT      AS region,
    $1:country::TEXT     AS country,
    $1:square_footage::NUMBER(10, 2) AS square_footage,
    $1:employee_count::INTEGER       AS employee_count

FROM BRONZE.STORE_LOCATIONS
WHERE store_id IS NOT NULL
-- Dédoublonnage strict
QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) = 1;

WITH verification_doublons AS (
    SELECT store_id, COUNT(*) as nombre_apparitions
    FROM SILVER.STORE_LOCATIONS_CLEAN
    GROUP BY store_id HAVING COUNT(*) > 1
)
SELECT 
    CASE 
        WHEN COUNT(*) > 0 THEN 'ATTENTION : Il y a ' || COUNT(*) || ' doublons d''ID magasin !'
        ELSE 'SUCCÈS : Aucun doublon trouvé.'
    END AS RAPPORT_DOUBLONS
FROM verification_doublons;

SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 'ATTENTION : La table est vide !'
        WHEN COUNT_IF(store_id IS NULL OR city IS NULL OR square_footage IS NULL) = 0 
        THEN 'Aucune valeur manquante, le dataset est complet à 100%.'
        
        ELSE 'VALEURS MANQUANTES : ' ||
             'ID ('     || TO_VARCHAR(ROUND(COUNT_IF(store_id IS NULL)       / NULLIF(COUNT(*),0) * 100, 1)) || '%) | ' ||
             'Ville ('  || TO_VARCHAR(ROUND(COUNT_IF(city IS NULL)           / NULLIF(COUNT(*),0) * 100, 1)) || '%) | ' ||
             'Surface ('|| TO_VARCHAR(ROUND(COUNT_IF(square_footage IS NULL) / NULLIF(COUNT(*),0) * 100, 1)) || '%)'
    END AS RAPPORT_COMPLETUDE
FROM SILVER.STORE_LOCATIONS_CLEAN;

SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 'Aucune anomalie détectée'
        ELSE 'QUALITÉ MÉTIER : ' || COUNT(*) || ' lignes bizarres (Surface <= 0 ou Employés < 0).'
    END AS RAPPORT_QUALITE_METIER
FROM SILVER.STORE_LOCATIONS_CLEAN
WHERE 
    square_footage <= 0 
    OR employee_count < 0;

--Affichage de la table SILVER.STORE_LOCATIONS_CLEAN
SELECT * FROM SILVER.STORE_LOCATIONS_CLEAN LIMIT 10;

--9. Nettoyage de la table BRONZE.logistics_and_shipping----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.LOGISTICS_AND_SHIPPING_CLEAN AS
SELECT 
    shipment_id::TEXT AS shipment_id,
    order_id::INTEGER AS order_id,
    ship_date::DATE AS ship_date,
    estimated_delivery::DATE AS estimated_delivery,
    shipping_method::TEXT AS shipping_method,
    status::TEXT AS status,
    REGEXP_REPLACE(shipping_cost, '[^0-9.]', '')::FLOAT AS shipping_cost,
    destination_region::TEXT AS destination_region,
    destination_country::TEXT AS destination_country,
    carrier::TEXT AS carrier
FROM BRONZE.LOGISTICS_AND_SHIPPING
QUALIFY ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY ship_date) = 1;

--Affichage de la table SILVER.LOGISTICS_AND_SHIPPING_CLEAN
SELECT * FROM SILVER.LOGISTICS_AND_SHIPPING_CLEAN LIMIT 10;

--10. Nettoyage de la table BRONZE.supplier_information----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.SUPPLIER_INFORMATION_CLEAN AS
SELECT 
    supplier_id::TEXT AS supplier_id,
    supplier_name::TEXT AS supplier_name,
    product_category::TEXT AS product_category,
    region::TEXT AS region,
    country::TEXT AS country,
    city::TEXT AS city,
    lead_time::INTEGER AS lead_time,
    reliability_score::FLOAT AS reliability_score,
    quality_rating::TEXT AS quality_rating
FROM BRONZE.SUPPLIER_INFORMATION
QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY supplier_id) = 1;

--Affichage de la table SILVER.SUPPLIER_INFORMATION_CLEAN
SELECT * FROM SILVER.SUPPLIER_INFORMATION_CLEAN LIMIT 10;
--11. Nettoyage de la table BRONZE.employee_records---------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.EMPLOYEE_RECORDS_CLEAN AS
SELECT 
    employee_id::TEXT AS employee_id,
    name::TEXT AS name,
    date_of_birth::DATE AS date_of_birth,
    hire_date::DATE AS hire_date,
    department::TEXT AS department,
    job_title::TEXT AS job_title,
    REGEXP_REPLACE(salary, '[^0-9.]', '')::FLOAT AS salary,
    region::TEXT AS region,
    country::TEXT AS country,
    email::TEXT AS email
FROM BRONZE.EMPLOYEE_RECORDS
QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY hire_date DESC) = 1;

--Affichage de la table SILVER.EMPLOYEE_RECORDS_CLEAN
SELECT * FROM SILVER.EMPLOYEE_RECORDS_CLEAN LIMIT 10;
-----------------------------------------------------------------
