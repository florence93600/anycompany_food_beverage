-- Phase 1– Data Preparation & Ingestion-------------------------------------------------------------------------------------------

--Etape 4- Nettoyage de données-----------------------------------------------------------------------------------------------------
--1.Nettoyage de la table BRONZE.customer_demographics------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.customer_demographics_clean AS
WITH deduplicated AS (
    SELECT
        customer_id,
        name,
        date_of_birth,
        gender,
        region,
        country,
        city,
        marital_status,
        annual_income,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY customer_id
        ) AS rn
    FROM BRONZE.customer_demographics
    WHERE customer_id IS NOT NULL
)
SELECT
    customer_id,
    -- Nettoyage du nom 
    INITCAP(TRIM(name)) AS customer_name,
    -- Date de naissance
    date_of_birth,
    -- Âge 
    DATEDIFF(year, date_of_birth, CURRENT_DATE()) AS age,
    -- Genre
    CASE
        WHEN UPPER(gender) IN ('MALE', 'M') THEN 'MALE'
        WHEN UPPER(gender) IN ('FEMALE', 'F') THEN 'FEMALE'
        ELSE 'OTHER'
    END AS gender,
    -- Localisation
    UPPER(TRIM(region)) AS region,
    UPPER(TRIM(country)) AS country,
    UPPER(TRIM(city)) AS city,
    -- Statut marital
    INITCAP(TRIM(marital_status)) AS marital_status,
    -- Revenu annuel
    CASE
        WHEN annual_income > 0 THEN annual_income
        ELSE NULL
    END AS annual_income
FROM deduplicated
WHERE rn = 1;
--Test affichage de la table SILVER.customer_demographics------------------------------------------------------------------
SELECT * FROM SILVER.customer_demographics_clean LIMIT 10;
--2.Nettoyage de la table BRONZE.customer_service_interactions-------------------------------------------------
CREATE OR REPLACE TABLE SILVER.customer_service_interactions_clean AS
WITH deduplicated AS (
    SELECT
        interaction_id,
        interaction_date,
        interaction_type,
        issue_category,
        description,
        duration_minutes,
        resolution_status,
        follow_up_required,
        customer_satisfaction,
        ROW_NUMBER() OVER (
            PARTITION BY interaction_id
            ORDER BY interaction_date DESC
        ) AS rn
    FROM BRONZE.customer_service_interactions
    WHERE interaction_id IS NOT NULL
)
SELECT
    interaction_id,
    -- Interaction date
    interaction_date,
    -- interaction type
    CASE
        WHEN UPPER(interaction_type) IN ('PHONE', 'CALL') THEN 'PHONE'
        WHEN UPPER(interaction_type) = 'EMAIL' THEN 'EMAIL'
        WHEN UPPER(interaction_type) = 'CHAT' THEN 'CHAT'
        ELSE 'OTHER'
    END AS interaction_type,
     -- Nettoyage de la colonne issue category
    INITCAP(TRIM(issue_category)) AS issue_category,
    --  description
    TRIM(description) AS description,
    -- Duration quality 
    CASE
        WHEN duration_minutes BETWEEN 1 AND 240 THEN duration_minutes
        ELSE NULL
    END AS duration_minutes,
    --  resolution status
    CASE
        WHEN UPPER(resolution_status) = 'RESOLVED' THEN 'RESOLVED'
        WHEN UPPER(resolution_status) = 'PENDING' THEN 'PENDING'
        WHEN UPPER(resolution_status) = 'ESCALATED' THEN 'ESCALATED'
        ELSE 'UNKNOWN'
    END AS resolution_status,
    -- Normalisation de la colonne Follow_up_required 
    CASE
        WHEN UPPER(follow_up_required) IN ('YES', 'Y', 'TRUE') THEN TRUE
        WHEN UPPER(follow_up_required) IN ('NO', 'N', 'FALSE') THEN FALSE
        ELSE NULL
    END AS follow_up_required,
    -- Customer_satisfaction 
    CASE
        WHEN customer_satisfaction BETWEEN 1 AND 5 THEN customer_satisfaction
        ELSE NULL
    END AS customer_satisfaction
FROM deduplicated
WHERE rn = 1;
--Test d'affichage de la table SILVER.customer_service_interactions_clean---------------------------------
SELECT * FROM SILVER.customer_service_interactions_clean LIMIT 10;

--3.Nettoyage de la table BRONZE.financial_transactions-----------------------------------------------------------------------------------
CREATE TABLE SILVER.financial_transactions_clean AS
SELECT DISTINCT *
FROM BRONZE.financial_transactions
WHERE amount > 0;
--Test d'affichage de la table SILVER.fnancial_transactions_clean
SELECT * FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN LIMIT 10;
--4.Nettoyage de la table BRONZE.promotions_data-----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.promotions_data_clean AS
WITH deduplicated AS (
    SELECT
        promotion_id,
        product_category,
        promotion_type,
        discount_percentage,
        start_date,
        end_date,
        region,
        ROW_NUMBER() OVER (
            PARTITION BY promotion_id
            ORDER BY start_date DESC
        ) AS rn
    FROM BRONZE.promotions_data
    WHERE promotion_id IS NOT NULL
)
SELECT
    promotion_id,
    -- Normalisation de product_category
    INITCAP(TRIM(product_category)) AS product_category,
    -- Normalisation de promotion type
    INITCAP(TRIM(promotion_type)) AS promotion_type,
    -- Validation Discount 
    CASE
        WHEN discount_percentage BETWEEN 0 AND 1
        THEN discount_percentage
        ELSE NULL
    END AS discount_percentage,
    -- dates de Promotion 
    start_date,
    end_date,
    -- Durée de Promotion 
    DATEDIFF(day, start_date, end_date) AS promotion_duration_days,
    -- Indicateur de promotion
    CASE
        WHEN CURRENT_DATE() BETWEEN start_date AND end_date THEN TRUE
        ELSE FALSE
    END AS is_active,
    -- Normalisation de la region
    UPPER(TRIM(region)) AS region
FROM deduplicated
WHERE rn = 1
  AND start_date IS NOT NULL
  AND end_date IS NOT NULL
  AND start_date <= end_date;
--Test d'affichage de la table SILVER.promotions_data_clean---------------------------------------
SELECT * FROM SILVER.promotions_data_clean LIMIT 10;

--5.Nettoyage de la table BRONZE.marketing_campaigns-----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.marketing_campaigns_clean AS
WITH deduplicated AS (
    SELECT
        campaign_id,
        campaign_name,
        campaign_type,
        product_category,
        target_audience,
        start_date,
        end_date,
        region,
        budget,
        reach,
        conversion_rate,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id
            ORDER BY start_date DESC
        ) AS rn
    FROM BRONZE.marketing_campaigns
    WHERE campaign_id IS NOT NULL
)
SELECT
    campaign_id,
    -- Nettoyage campaign name
    INITCAP(TRIM(campaign_name)) AS campaign_name,
    -- Normalisation campaign type
    INITCAP(TRIM(campaign_type)) AS campaign_type,
    -- Normalisation product category
    INITCAP(TRIM(product_category)) AS product_category,
    -- Normalisation target audience
    INITCAP(TRIM(target_audience)) AS target_audience,
    -- dates Campaign 
    start_date,
    end_date,
    -- durée Campaign
    DATEDIFF(day, start_date, end_date) AS campaign_duration_days,
    -- Normalisation region
    UPPER(TRIM(region)) AS region,
    -- Règles qualité budget 
    CASE
        WHEN budget > 0 THEN budget
        ELSE NULL
    END AS budget,
    -- Règles qualité Reach 
    CASE
        WHEN reach > 0 THEN reach
        ELSE NULL
    END AS reach,
    -- Conversion_rate 
    CASE
        WHEN conversion_rate BETWEEN 0 AND 1
        THEN conversion_rate
        ELSE NULL
    END AS conversion_rate,
    -- Estimation conversions
    CASE
        WHEN reach > 0 AND conversion_rate BETWEEN 0 AND 1
        THEN reach * conversion_rate
        ELSE NULL
    END AS estimated_conversions,
    -- Coût par contact
    CASE
        WHEN budget > 0 AND reach > 0
        THEN budget / reach
        ELSE NULL
    END AS cost_per_contact
FROM deduplicated
WHERE rn = 1
  AND start_date IS NOT NULL
  AND end_date IS NOT NULL
  AND start_date <= end_date;
--Test d'affichage de la table SILVER.marketing_campaigns_clean---------------------------------------------------------------
SELECT * FROM SILVER.marketing_campaigns_clean LIMIT 10;

--6.Nettoyage de la table BRONZE.product_reviews----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.product_reviews_clean AS
WITH deduplicated AS (
    SELECT
        review_id,
        product_id,
        reviewer_id,
        reviewer_name,
        rating,
        review_date,
        review_title,
        review_text,
        product_category,
        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY review_date DESC
        ) AS rn
    FROM BRONZE.product_reviews
    WHERE review_id IS NOT NULL
)
SELECT
    review_id,
    -- Identifiants 
    product_id,
    reviewer_id,
    -- Normalisation reviewer_name
    INITCAP(TRIM(reviewer_name)) AS reviewer_name,
    -- Review date
    review_date,
    -- Validation "Rating" 
    CASE
        WHEN rating BETWEEN 1 AND 5 THEN rating
        ELSE NULL
    END AS rating,
    -- Review title
    INITCAP(TRIM(review_title)) AS review_title,
    -- Nettoyage Review_text
    TRIM(review_text) AS review_text,
    -- Text length (useful for sentiment proxy)
    LENGTH(TRIM(review_text)) AS review_text_length,
    -- Indicateur d'avis positif/négatif
    CASE
        WHEN rating >= 4 THEN 'POSITIVE'
        WHEN rating <= 2 THEN 'NEGATIVE'
        ELSE 'NEUTRAL'
    END AS review_sentiment,
    -- Normalisation product_category
    INITCAP(TRIM(product_category)) AS product_category
FROM deduplicated
WHERE rn = 1
  AND review_date IS NOT NULL;
--Test d'affichage de la table SILVER.product_reviews_clean--------------------------
SELECT * FROM SILVER.product_reviews_clean LIMIT 10;

--7.Nettoyage de la table BRONZE.inventory----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.inventory_clean AS
WITH deduplicated AS (
    SELECT
        product_id,
        product_category,
        region,
        country,
        warehouse,
        current_stock,
        reorder_point,
        lead_time,
        last_restock_date,
        ROW_NUMBER() OVER (
            PARTITION BY product_id, region, warehouse
            ORDER BY last_restock_date DESC
        ) AS rn
    FROM BRONZE.inventory
    WHERE product_id IS NOT NULL
)
SELECT
    product_id,
    -- Normalisation product_category
    INITCAP(TRIM(product_category)) AS product_category,
    -- Normalisation region
    UPPER(TRIM(region)) AS region,
    UPPER(TRIM(country)) AS country,
    INITCAP(TRIM(warehouse)) AS warehouse,
    -- Validation du Stock 
    CASE
        WHEN current_stock >= 0 THEN current_stock
        ELSE NULL
    END AS current_stock,
    -- validation Reorder_point 
    CASE
        WHEN reorder_point >= 0 THEN reorder_point
        ELSE NULL
    END AS reorder_point,
    -- Validation Lead_time 
    CASE
        WHEN lead_time > 0 THEN lead_time
        ELSE NULL
    END AS lead_time,
    -- Last restock date
    last_restock_date,
    -- Indicateur statut Stock
    CASE
        WHEN current_stock = 0 THEN 'OUT_OF_STOCK'
        WHEN current_stock <= reorder_point THEN 'LOW_STOCK'
        ELSE 'IN_STOCK'
    END AS stock_status,
    -- Indicateur réapprovisionnement 
    CASE
        WHEN current_stock <= reorder_point THEN TRUE
        ELSE FALSE
    END AS needs_restock
FROM deduplicated
WHERE rn = 1;
--Test d'affichage de la table SILVER.inventory_clean-----------------------------------------------
SELECT * FROM SILVER.inventory_clean LIMIT 10;

--8.Nettoyage de la table BRONZE.store_locations----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.store_locations_clean AS
WITH deduplicated AS (
    SELECT
        store_id,
        store_name,
        store_type,
        region,
        country,
        city,
        address,
        postal_code,
        square_footage,
        employee_count,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY store_id
        ) AS rn
    FROM BRONZE.store_locations
    WHERE store_id IS NOT NULL
)
SELECT
    store_id,
    -- Store_name
    INITCAP(TRIM(store_name)) AS store_name,
    -- Normalisation store_type
    INITCAP(TRIM(store_type)) AS store_type,
    -- Normalisation region
    UPPER(TRIM(region)) AS region,
    INITCAP(TRIM(country)) AS country,
    INITCAP(TRIM(city)) AS city,
    -- Nettoyage address
    TRIM(address) AS address,
    -- Postal_code
    postal_code,
    -- Validation Square_footage 
    CASE
        WHEN square_footage > 0 THEN square_footage
        ELSE NULL
    END AS square_footage,
    -- Validation Employee_count 
    CASE
        WHEN employee_count >= 0 THEN employee_count
        ELSE NULL
    END AS employee_count,
    -- Catégorisation de la taille du magasin 
    CASE
        WHEN square_footage < 3000 THEN 'SMALL'
        WHEN square_footage BETWEEN 3000 AND 7000 THEN 'MEDIUM'
        ELSE 'LARGE'
    END AS store_size_category,
    -- Employees 
    CASE
        WHEN square_footage > 0 AND employee_count >= 0
        THEN (employee_count / square_footage) * 1000
        ELSE NULL
    END AS employees_per_1000_sqft
FROM deduplicated
WHERE rn = 1;
--Test d'affichage de la table SILVER.store_locations_clean-------------------------------------------------------------------------------
SELECT * FROM SILVER.store_locations_clean LIMIT 10;

--9. Nettoyage de la table BRONZE.logistics_and_shipping----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.logistics_and_shipping_clean AS
WITH deduplicated AS (
    SELECT
        shipment_id,
        order_id,
        ship_date,
        estimated_delivery,
        shipping_method,
        status,
        shipping_cost,
        destination_region,
        destination_country,
        carrier,
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id
            ORDER BY ship_date DESC
        ) AS rn
    FROM BRONZE.logistics_and_shipping
    WHERE shipment_id IS NOT NULL
)
SELECT
    shipment_id,
    order_id,
    -- Dates d'expédition
    ship_date,
    estimated_delivery,
    -- Delai de livraison estimé (jours)
    DATEDIFF(day, ship_date, estimated_delivery) AS estimated_delivery_days,
    -- Normalisation shipping_method
    INITCAP(TRIM(shipping_method)) AS shipping_method,
    -- Normalisation status
    CASE
        WHEN UPPER(status) = 'DELIVERED' THEN 'DELIVERED'
        WHEN UPPER(status) = 'SHIPPED' THEN 'SHIPPED'
        WHEN UPPER(status) = 'IN TRANSIT' THEN 'IN_TRANSIT'
        WHEN UPPER(status) = 'RETURNED' THEN 'RETURNED'
        ELSE 'UNKNOWN'
    END AS status,
    -- Validation Shipping_cost 
    CASE
        WHEN shipping_cost >= 0 THEN shipping_cost
        ELSE NULL
    END AS shipping_cost,
    -- Normalisation destination
    UPPER(TRIM(destination_region)) AS destination_region,
    INITCAP(TRIM(destination_country)) AS destination_country,
    -- Nettoyage carrier 
    INITCAP(TRIM(carrier)) AS carrier,
    -- Indicateur retours
    CASE
        WHEN UPPER(status) = 'RETURNED' THEN TRUE
        ELSE FALSE
    END AS is_returned,
    -- Indicateur problème de livraison
    CASE
        WHEN estimated_delivery IS NULL
             OR ship_date IS NULL
             OR estimated_delivery < ship_date
        THEN TRUE
        ELSE FALSE
    END AS delivery_issue
FROM deduplicated
WHERE rn = 1;
--Test d'affichage de la table SILVER.logistics_and_shipping_clean------------------------------------------------------------------------
SELECT * FROM SILVER.logistics_and_shipping_clean LIMIT 10;

--10. Nettoyage de la table BRONZE.supplier_information----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.supplier_information_clean AS
WITH deduplicated AS (
    SELECT
        supplier_id,
        supplier_name,
        product_category,
        region,
        country,
        city,
        lead_time,
        reliability_score,
        quality_rating,
        ROW_NUMBER() OVER (
            PARTITION BY supplier_id
            ORDER BY reliability_score DESC
        ) AS rn
    FROM BRONZE.supplier_information
    WHERE supplier_id IS NOT NULL
)
SELECT
    supplier_id,
    -- Supplier_name
    INITCAP(TRIM(supplier_name)) AS supplier_name,
    -- Normalisation product_category
    INITCAP(TRIM(product_category)) AS product_category,
    -- Normalisation region
    UPPER(TRIM(region)) AS region,
    INITCAP(TRIM(country)) AS country,
    INITCAP(TRIM(city)) AS city,
    -- Validation Lead_time (jours)
    CASE
        WHEN lead_time > 0 THEN lead_time
        ELSE NULL
    END AS lead_time,
    -- Validation reliability_score 
    CASE
        WHEN reliability_score BETWEEN 0 AND 1
        THEN reliability_score
        ELSE NULL
    END AS reliability_score,
    --Normalisation quality_rating 
    CASE
        WHEN UPPER(quality_rating) IN ('A', 'B', 'C')
        THEN UPPER(quality_rating)
        ELSE 'UNKNOWN'
    END AS quality_rating,
    -- Category reliability 
    CASE
        WHEN reliability_score >= 0.85 THEN 'HIGH'
        WHEN reliability_score BETWEEN 0.70 AND 0.84 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS reliability_category,
    -- Score mondial des fournisseurs 
    CASE
        WHEN reliability_score IS NOT NULL AND lead_time IS NOT NULL
        THEN reliability_score * (1 / lead_time)
        ELSE NULL
    END AS supplier_global_score
FROM deduplicated
WHERE rn = 1;
--Test d'affichage de la table SILVER.supplier_information_clean-------------------------------------------------------------------
SELECT * FROM SILVER.supplier_information_clean LIMIT 10;

--11. Nettoyage de la table BRONZE.employee_records---------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.employee_records_clean AS
WITH deduplicated AS (
    SELECT
        employee_id,
        name,
        date_of_birth,
        hire_date,
        department,
        job_title,
        salary,
        region,
        country,
        email,
        ROW_NUMBER() OVER (
            PARTITION BY employee_id
            ORDER BY hire_date DESC
        ) AS rn
    FROM BRONZE.employee_records
    WHERE employee_id IS NOT NULL
)
SELECT
    employee_id,
    -- Employee_name
    INITCAP(TRIM(name)) AS employee_name,
    -- Dates
    date_of_birth,
    hire_date,
    -- Age calculation
    DATEDIFF(year, date_of_birth, CURRENT_DATE()) AS age,
    -- Calcul de la titularisation (ans)
    DATEDIFF(year, hire_date, CURRENT_DATE()) AS tenure_years,
    -- Normalisation department
    INITCAP(TRIM(department)) AS department,
    -- Normalisation job_title
    INITCAP(TRIM(job_title)) AS job_title,
    -- Validation salaire
    CASE
        WHEN salary > 0 THEN salary
        ELSE NULL
    END AS salary,
    -- Catégorisation salaire
    CASE
        WHEN salary < 50000 THEN 'LOW'
        WHEN salary BETWEEN 50000 AND 100000 THEN 'MEDIUM'
        ELSE 'HIGH'
    END AS salary_category,
    -- Normalisation region
    UPPER(TRIM(region)) AS region,
    INITCAP(TRIM(country)) AS country,
    -- Nettoyage email 
    LOWER(TRIM(email)) AS email
FROM deduplicated
WHERE rn = 1
  AND hire_date >= date_of_birth;
--Test d'affichage de la table SILVER.employee_records_clean----------------------------------------------------------------------
SELECT * FROM SILVER.employee_records_clean LIMIT 10 ;
