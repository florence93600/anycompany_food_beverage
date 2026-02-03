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


----Définition des formats de fichiers CSV et JSON------
create or replace file format BRONZE.csv
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE FILE FORMAT BRONZE.json
  type='json'
  strip_outer_array=true;
  
--Etape 2- Création des tables-------------------------------------------------------------------------------
--1.Création de la table customer_demographics----------------------------------------------------
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

--2.Création de la table customer_service_interactions----------------------------------------------------

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

--3.Création de la table financial_transactions----------------------------------------------------
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
    
--4.Création de la table promotions_data----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.promotions_data (
    promotion_id VARCHAR(20),
    product_category VARCHAR(50),
    promotion_type VARCHAR(50),
    discount_percentage NUMBER(5,4),
    start_date DATE,
    end_date DATE,
    region VARCHAR(50)
    );
--5.Création de la table marketing_campaigns----------------------------------------------------
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
   
--6.Création de la table product_reviews----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.PRODUCT_REVIEWS (
    review_id NUMBER,
    product_id VARCHAR(20),
    reviewer_id VARCHAR(30),
    reviewer_name VARCHAR(100),
    rating NUMBER,
    review_date DATE,
    review_title VARCHAR(200),
    review_text VARCHAR(5000),
    product_category VARCHAR(50)
);

----7.Création de la table inventory----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.inventory (
    product_id  VARCHAR(20),
    product_category VARCHAR(50),
    region VARCHAR(50),
    country VARCHAR(50),
    warehouse VARCHAR(100),
    current_stock NUMBER,
    reorder_point NUMBER,
    lead_time NUMBER,
    last_restock_date DATE
   );

----8.Création de la table store_locations----------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.store_locations(
    store_id  VARCHAR(20),
    store_name VARCHAR(100),
    store_type VARCHAR(30),
    region VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(100),
    address VARCHAR(200),
    postal_code NUMBER,
    square_footage NUMBER(10,2),
    employee_count NUMBER
   );

----9.Création de la table logistics_and_shipping----------------------------------------------------
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
----10.Création de la table supplier_information----------------------------------------------------
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
----11.Création de la table employee_records----------------------------------------------------
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
 --1.Chargement de la table customer_demographics---------------------------------------------------------------------------
COPY INTO BRONZE.customer_demographics
FROM @BRONZE.food_beverage_stage/customer_demographics.csv
FILE_FORMAT = BRONZE.csv;
--Test de chargement de la table customer_demographics
 select * from BRONZE.customer_demographics;

--2.Chargement de la table customer_service_interactions --------------------------------------------------------------------
COPY INTO BRONZE.CUSTOMER_SERVICE_INTERACTIONS
FROM @BRONZE.FOOD_BEVERAGE_STAGE/customer_service_interactions.csv
FILE_FORMAT=BRONZE.csv;
--Test de chargement de la table customer_service_interactions
 select * from BRONZE.customer_service_interactions;
 
--3.Chargement de la table financial_transactions-----------------------------------------------------------------------------------------
COPY INTO BRONZE.financial_transactions
FROM @BRONZE.FOOD_BEVERAGE_STAGE/financial_transactions.csv
FILE_FORMAT=BRONZE.csv;
--Test de chargement de la table financial_transactions-----------------------------------------------------------------------------------
 select * from BRONZE.financial_transactions;

--4.Chargement de la table promotions_data ---------------------------------------------------------------------------
COPY INTO BRONZE.promotions_data
FROM @BRONZE.food_beverage_stage/promotions-data.csv
FILE_FORMAT =BRONZE.csv;

--Test de chargement de la table promotions_data-----------------------------------------------------------------------------------
 select * from BRONZE.promotions_data;
--5.Chargement de la table marketing_campaigns ---------------------------------------------------------------------------
COPY INTO BRONZE.marketing_campaigns
FROM @BRONZE.food_beverage_stage/marketing_campaigns.csv
FILE_FORMAT = BRONZE.csv;
--Test de chargement de la table marketing_campaigns -----------------------------------------------------------------------------------
 select * from BRONZE.promotions_data;
 
--6.Chargement de la table product_reviews---------------------------------------------------------------------------
COPY INTO BRONZE.PRODUCT_REVIEWS
FROM @BRONZE.FOOD_BEVERAGE_STAGE/product_reviews.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';


--Test de chargement de la table product_reviews -----------------------------------------------------------------------------------
 select * from BRONZE.product_reviews;
 
 --7.Chargement de la table inventory ---------------------------------------------------------------------------
COPY INTO BRONZE.inventory
FROM @BRONZE.food_beverage_stage/inventory.json
FILE_FORMAT = BRONZE.json
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
--Test de chargement de la table logistics_and_shipping ----------------------------------------------------------------------------------
select * from BRONZE.inventory;

 --8.Chargement de la table store_locations ---------------------------------------------------------------------------
COPY INTO BRONZE.store_locations
FROM @BRONZE.food_beverage_stage/store_locations.json
FILE_FORMAT = BRONZE.json
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
--Test de chargement de la table logistics_and_shipping ----------------------------------------------------------------------------------
select * from BRONZE.store_locations;
 
--9.Chargement de la table logistics_and_shipping ---------------------------------------------------------------------------
COPY INTO BRONZE.logistics_and_shipping
FROM @BRONZE.food_beverage_stage/logistics_and_shipping.csv
FILE_FORMAT = BRONZE.csv;

--Test de chargement de la table logistics_and_shipping ----------------------------------------------------------------------------------
select * from BRONZE.logistics_and_shipping;

--10.Chargement de la table supplier_information ---------------------------------------------------------------------------
COPY INTO BRONZE.supplier_information
FROM @BRONZE.food_beverage_stage/supplier_information.csv
FILE_FORMAT = BRONZE.csv;
--Test de chargement de la table supplier_information ----------------------------------------------------------------------------------
select * from BRONZE.supplier_information;

--11.Chargement de la table supplier_information ---------------------------------------------------------------------------
COPY INTO BRONZE.employee_records
FROM @BRONZE.food_beverage_stage/employee_records.csv
FILE_FORMAT = BRONZE.csv;
--Test de chargement de la table employee_records ----------------------------------------------------------------------------------
select * from BRONZE.employee_records;
