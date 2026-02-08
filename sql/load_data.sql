-- Phase 1– Data Preparation & Ingestion-------------------------------------------------------------------------------------------

--Etape 1- Préparation de l'environnement Snowflake-------------------------------------------------------------------------------

--1. Création de la base de données ANYCOMPANY_LAB
CREATE OR REPLACE DATABASE ANYCOMPANY_LAB;
USE DATABASE ANYCOMPANY_LAB;

--2. Création des schémas Bronze, Silver et Analytics
--(Pour les données brutes)--
CREATE SCHEMA IF NOT EXISTS BRONZE;
--(Pour les données nettoyées)--
CREATE SCHEMA IF NOT EXISTS SILVER;
--(Pour les tables analytiques)--
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
--3. Création du Stage Snowflake
CREATE OR REPLACE STAGE BRONZE.food_beverage_stage
URL = 's3://logbrain-datalake/datasets/food-beverage/' ;
-- Vérification : Liste les fichiers pour être sûre que la connexion marche
LIST @BRONZE.food_beverage_stage;

--On sait que notre jeu de données contient 9 fichiers csv mais on ne connait pas a priori leurs "delimiters". Alors avant toute chose nous allons assayer de les détecter.
SELECT 
    METADATA$FILENAME as "Nom du Fichier",
    $1 as "Aperçu (Regarde le séparateur)"
FROM @BRONZE.food_beverage_stage
WHERE METADATA$FILE_ROW_NUMBER = 1; -- On affiche la 1ère ligne
---4. Définition des formats de fichiers CSV et JSON------
CREATE OR REPLACE FILE FORMAT BRONZE.CSV_COMMA
   TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'      -- Gère les textes avec virgules
    NULL_IF = ('NULL', 'null', '')          -- Nettoie les variantes de vide
    EMPTY_FIELD_AS_NULL = TRUE              -- Uniformise les vides
    ENCODING = 'UTF8'                       -- Standard international
    REPLACE_INVALID_CHARACTERS = TRUE       -- Évite les crashs sur accents
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE; -- Force le passage même si ligne cassée
   
CREATE OR REPLACE FILE FORMAT BRONZE.CSV_SPACE
    TYPE = 'CSV'
    FIELD_DELIMITER = NONE -- Aucun séparateur, on prend tout !
    SKIP_HEADER = 0;

CREATE OR REPLACE FILE FORMAT BRONZE.json
  type='json'
  strip_outer_array=true; -- Utile si le JSON commence par [ ... ]
  
--Etape 2- Création des tables-------------------------------------------------------------------------------
--Ici, nous définissons toutes les colonnes au format texte afin d’éviter tout échec de chargement, même lorsqu’une valeur est incorrectement formatée (par exemple une date indiquée « N/A »).
--A. Création des tables csv-----------------------------------------------------------------------------------
--1.Création de la table BRONZE.customer_demographics----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.customer_demographics (
    customer_id TEXT,
    name TEXT,
    date_of_birth TEXT,
    gender TEXT,
    region TEXT,
    country TEXT,
    city TEXT,
    marital_status TEXT,
    annual_income TEXT
);

--2.Création de la table BRONZE.customer_service_interactions----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CUSTOMER_SERVICE_INTERACTIONS (
    interaction_id TEXT,
    interaction_date TEXT,
    interaction_type TEXT,
    issue_category TEXT,
    description TEXT,
    duration_minutes TEXT,
    resolution_status TEXT,
    follow_up_required TEXT,
    customer_satisfaction TEXT
);

--3.Création de la table BRONZE.financial_transactions----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.financial_transactions (
    transaction_id TEXT,
    transaction_date TEXT,
    transaction_type TEXT,
    amount TEXT,
    payment_method TEXT,
    entity TEXT,
    region TEXT,
    account_code TEXT
    );

--4.Création de la table BRONZE.promotions_data----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.promotions_data (
    promotion_id TEXT,
    product_category TEXT,
    promotion_type TEXT,
    discount_percentage TEXT,
    start_date TEXT,
    end_date TEXT,
    region TEXT
    );
    
--5.Création de la table BRONZE.marketing_campaigns----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.marketing_campaigns (
    campaign_id TEXT,
    campaign_name TEXT,
    campaign_type  TEXT,
    product_category TEXT,
    target_audience TEXT,
    start_date TEXT,
    end_date TEXT,
    region TEXT,
    budget TEXT,
    reach TEXT,
    conversion_rate TEXT
    );

--6.Création de la table BRONZE.product_reviews----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.PRODUCT_REVIEWS (
   raw_line TEXT
);

----7.Création de la table BRONZE.logistics_and_shipping----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.logistics_and_shipping(
    shipment_id TEXT,
    order_id TEXT,
    ship_date TEXT,
    estimated_delivery TEXT,
    shipping_method TEXT,
    status TEXT,
    shipping_cost TEXT,
    destination_region TEXT,
    destination_country TEXT,
    carrier  TEXT
   );
   
----8.Création de la table BRONZE.supplier_information----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.supplier_information(
    supplier_id TEXT,
    supplier_name TEXT,
    product_category TEXT,
    region  TEXT,
    country  TEXT,
    city TEXT,
    lead_time TEXT,
    reliability_score TEXT,
    quality_rating TEXT
    );
   
----9.Création de la table BRONZE.employee_records----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.employee_records(
    employee_id TEXT,
    name TEXT,
    date_of_birth TEXT,
    hire_date TEXT,
    department TEXT,
    job_title TEXT,
    salary TEXT,
    region TEXT,
    country TEXT,
    email TEXT
    );
--B. Création des tables json-----------------------------------------------------------------------------------
----10.Création de la table BRONZE.inventory----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.inventory (
    raw_data VARIANT
    );
----11.Création de la table BRONZE.store_locations----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.store_locations (
    raw_data VARIANT
    );
--Etape 3- Chargement des données-------------------------------------------------------------------------------
-- ====================================================================
-- GROUPE A : LES 8 TABLES CSV STANDARDS (Séparateur Virgule)
-- Format : BRONZE.CSV_COMMA
-- Source : @BRONZE_FOOD_BEVERAGE_STAGE
-- ====================================================================
 --1.Chargement de la table BRONZE.customer_demographics---------------------------------------------------------------------------
COPY INTO BRONZE.customer_demographics
FROM @BRONZE.food_beverage_stage/customer_demographics.csv
FILE_FORMAT = (FORMAT_name='BRONZE.CSV_COMMA');

--2.Chargement de la table BRONZE.customer_service_interactions --------------------------------------------------------------------
COPY INTO BRONZE.CUSTOMER_SERVICE_INTERACTIONS
FROM @BRONZE.food_beverage_stage/customer_service_interactions.csv
FILE_FORMAT=(FORMAT_name='BRONZE.CSV_COMMA');
 
--3.Chargement de la table BRONZE.financial_transactions-----------------------------------------------------------------------------------------
COPY INTO BRONZE.financial_transactions
FROM @BRONZE.food_beverage_stage/financial_transactions.csv
FILE_FORMAT=(FORMAT_name='BRONZE.CSV_COMMA');

--4.Chargement de la table BRONZE.promotions_data ---------------------------------------------------------------------------
COPY INTO BRONZE.promotions_data
FROM @BRONZE.food_beverage_stage/promotions-data.csv
FILE_FORMAT=(FORMAT_name='BRONZE.CSV_COMMA');

--5.Chargement de la table BRONZE.marketing_campaigns ---------------------------------------------------------------------------
COPY INTO BRONZE.marketing_campaigns
FROM @BRONZE.food_beverage_stage/marketing_campaigns.csv
FILE_FORMAT=(FORMAT_name='BRONZE.CSV_COMMA');
 
--6.Chargement de la table BRONZE.logistics_and_shipping ---------------------------------------------------------------------------
COPY INTO BRONZE.logistics_and_shipping
FROM @BRONZE.food_beverage_stage/logistics_and_shipping.csv
FILE_FORMAT=(FORMAT_name='BRONZE.CSV_COMMA');

--7.Chargement de la table BRONZE.supplier_information ---------------------------------------------------------------------------
COPY INTO BRONZE.supplier_information
FROM @BRONZE.food_beverage_stage/supplier_information.csv
FILE_FORMAT=(FORMAT_name='BRONZE.CSV_COMMA');

--8.Chargement de la table BRONZE.supplier_information ---------------------------------------------------------------------------
COPY INTO BRONZE.employee_records
FROM @BRONZE.food_beverage_stage/employee_records.csv
FILE_FORMAT=(FORMAT_name='BRONZE.CSV_COMMA');

-- GROUPE B : LE CAS SPÉCIAL (Séparateur Espace)
-- Format : BRONZE.CSV_SPACE
--9.Chargement de la table BRONZE.product_reviews---------------------------------------------------------------------------
COPY INTO BRONZE.PRODUCT_REVIEWS
FROM @BRONZE.food_beverage_stage/product_reviews.csv
FILE_FORMAT = (FORMAT_name='BRONZE.CSV_SPACE')
ON_ERROR = 'CONTINUE';

-- GROUPE C : LES 2 TABLES JSON
-- Format : BRONZE.JSON
 --10.Chargement de la table BRONZE.inventory ---------------------------------------------------------------------------
COPY INTO BRONZE.inventory
FROM @BRONZE.food_beverage_stage/inventory.json
FILE_FORMAT = BRONZE.json;

---11.Chargement de la table BRONZE.store_locations ---------------------------------------------------------------------------
COPY INTO BRONZE.store_locations
FROM @BRONZE.food_beverage_stage/store_locations.json
FILE_FORMAT = BRONZE.json;




