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


---4. Définition des formats de fichiers CSV et JSON------
CREATE OR REPLACE FILE FORMAT BRONZE.csv
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    ENCODING = 'UTF8'
    REPLACE_INVALID_CHARACTERS = TRUE  
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE FILE FORMAT BRONZE.json
  type='json'
  strip_outer_array=true;

  
--Etape 2- Création des tables-------------------------------------------------------------------------------
--1.Création de la table BRONZE.customer_demographics----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.customer_demographics (
    customer_id NUMBER,
    name VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(20),
    region VARCHAR (50),
    country VARCHAR (50),
    city VARCHAR (100),
    marital_status VARCHAR(20),
    annual_income NUMBER (12,2)
);

--2.Création de la table BRONZE.customer_service_interactions----------------------------------------------------

CREATE OR REPLACE TABLE BRONZE.CUSTOMER_SERVICE_INTERACTIONS (
    interaction_id VARCHAR(20),
    interaction_date DATE,
    interaction_type VARCHAR(20),
    issue_category VARCHAR(50),
    description VARCHAR(5000),
    duration_minutes NUMBER,
    resolution_status VARCHAR(20),
    follow_up_required VARCHAR(10),
    customer_satisfaction NUMBER
);

--3.Création de la table BRONZE.financial_transactions----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.financial_transactions (
    transaction_id VARCHAR(20),
    transaction_date DATE,
    transaction_type VARCHAR(30),
    amount NUMBER(12,2),
    payment_method VARCHAR(30),
    entity   VARCHAR(100),
    region VARCHAR(50),
    account_code VARCHAR(20)
    );
    
--4.Création de la table BRONZE.promotions_data----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.promotions_data (
    promotion_id VARCHAR(20),
    product_category VARCHAR(50),
    promotion_type VARCHAR(50),
    discount_percentage NUMBER(5,4),
    start_date DATE,
    end_date DATE,
    region VARCHAR(50)
    );
--5.Création de la table BRONZE.marketing_campaigns----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.marketing_campaigns (
    campaign_id VARCHAR(20),
    campaign_name VARCHAR(100),
    campaign_type  VARCHAR(30),
    product_category VARCHAR(50),
    target_audience VARCHAR(30),
    start_date DATE,
    end_date DATE,
    region VARCHAR(50),
    budget NUMBER(12,2),
    reach NUMBER,
    conversion_rate NUMBER(6,4)
    );
   
--6.Création de la table BRONZE.product_reviews----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.PRODUCT_REVIEWS (
    review_id VARCHAR,
    product_id VARCHAR,
    reviewer_id VARCHAR,
    reviewer_name VARCHAR,
    rating VARCHAR,
    review_date VARCHAR,
    review_title VARCHAR,
    review_text VARCHAR,
    product_category VARCHAR -- La fameuse 9ème colonne (qui sera vide)
);

----7.Création de la table BRONZE.inventory----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.inventory (
    raw_data VARIANT
    );

----8.Création de la table BRONZE.store_locations----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.store_locations (
    raw_data VARIANT
    );

----9.Création de la table BRONZE.logistics_and_shipping----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.logistics_and_shipping(
    shipment_id VARCHAR,
    order_id INTEGER,
    ship_date DATE,
    estimated_delivery DATE,
    shipping_method VARCHAR,
    status VARCHAR,
    shipping_cost FLOAT,
    destination_region VARCHAR,
    destination_country STRING,
    carrier  VARCHAR
   );
----10.Création de la table BRONZE.supplier_information----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.supplier_information(
    supplier_id VARCHAR,
    supplier_name VARCHAR,
    product_category VARCHAR,
    region  VARCHAR,
    country  STRING,
    city VARCHAR,
    lead_time INTEGER,
    reliability_score FLOAT,
    quality_rating STRING
    );
    
----11.Création de la table BRONZE.employee_records----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.employee_records(
    employee_id VARCHAR,
    name  VARCHAR,
    date_of_birth DATE,
    hire_date  DATE,
    department STRING,
    job_title VARCHAR,
    salary   FLOAT,
    region VARCHAR,
    country STRING,
    email VARCHAR
    );

--Etape 3- Chargement des données-------------------------------------------------------------------------------
 --1.Chargement de la table BRONZE.customer_demographics---------------------------------------------------------------------------
COPY INTO BRONZE.customer_demographics
FROM @BRONZE.food_beverage_stage/customer_demographics.csv
FILE_FORMAT = BRONZE.csv;

--2.Chargement de la table BRONZE.customer_service_interactions --------------------------------------------------------------------
COPY INTO BRONZE.CUSTOMER_SERVICE_INTERACTIONS
FROM @BRONZE.food_beverage_stage/customer_service_interactions.csv
FILE_FORMAT=BRONZE.csv;
 
--3.Chargement de la table BRONZE.financial_transactions-----------------------------------------------------------------------------------------
COPY INTO BRONZE.financial_transactions
FROM @BRONZE.food_beverage_stage/financial_transactions.csv
FILE_FORMAT=BRONZE.csv;

--4.Chargement de la table BRONZE.promotions_data ---------------------------------------------------------------------------
COPY INTO BRONZE.promotions_data
FROM @BRONZE.food_beverage_stage/promotions-data.csv
FILE_FORMAT =BRONZE.csv;

--5.Chargement de la table BRONZE.marketing_campaigns ---------------------------------------------------------------------------
COPY INTO BRONZE.marketing_campaigns
FROM @BRONZE.food_beverage_stage/marketing_campaigns.csv
FILE_FORMAT = BRONZE.csv;
 
--6.Chargement de la table BRONZE.product_reviews---------------------------------------------------------------------------
COPY INTO BRONZE.PRODUCT_REVIEWS
FROM @BRONZE.food_beverage_stage/product_reviews.csv
FILE_FORMAT = BRONZE.csv
ON_ERROR = 'CONTINUE';

 --7.Chargement de la table BRONZE.inventory ---------------------------------------------------------------------------
COPY INTO BRONZE.inventory
FROM @BRONZE.food_beverage_stage/inventory.json
FILE_FORMAT = BRONZE.json;
select * from BRONZE.inventory;
 --8.Chargement de la table BRONZE.store_locations ---------------------------------------------------------------------------
COPY INTO BRONZE.store_locations
FROM @BRONZE.food_beverage_stage/store_locations.json
FILE_FORMAT = BRONZE.json;
 
--9.Chargement de la table BRONZE.logistics_and_shipping ---------------------------------------------------------------------------
COPY INTO BRONZE.logistics_and_shipping
FROM @BRONZE.food_beverage_stage/logistics_and_shipping.csv
FILE_FORMAT = BRONZE.csv;

--10.Chargement de la table BRONZE.supplier_information ---------------------------------------------------------------------------
COPY INTO BRONZE.supplier_information
FROM @BRONZE.food_beverage_stage/supplier_information.csv
FILE_FORMAT = BRONZE.csv;

--11.Chargement de la table BRONZE.supplier_information ---------------------------------------------------------------------------
COPY INTO BRONZE.employee_records
FROM @BRONZE.food_beverage_stage/employee_records.csv
FILE_FORMAT = BRONZE.csv;




