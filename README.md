# anycompany_food_beverage – Data Analytics Project


## Description du Projet
Ce projet vise à analyser les performances commerciales, marketing, logistiques et l’expérience client de l’entreprise **AnyCompany Food & Beverage** à partir de données hétérogènes stockées dans **Snowflake**.  

L’architecture suit une approche **Data Lake / Medallion** avec des couches **Bronze** et **Silver**, et les résultats sont exploités via des requêtes SQL analytiques et des **dashboards interactifs Streamlit**.

## 🏗️ Architecture des données

### 🔹 BRONZE
Création de 11 tables définies toutes avec les colonnes au format texte afin d’éviter tout échec de chargement, même lorsqu’une valeur est incorrectement formatée (par exemple une date indiquée « N/A »). Les données brutes sont chargées depuis des fichiers **CSV**et  **JSON**

### 🔸 SILVER
* Données nettoyées, structurées et typées
* Suppression des valeurs nulles incohérentes
* Normalisation des dates et champs numériques
* Tables prêtes pour l’analyse

## Tables créées et utilisées dans SILVER

* SILVER.financial_transactions_clean
* SILVER.inventory_clean
* SILVER.logistics_and_shipping_clean
* SILVER.customer_demographics_clean
* SILVER.customer_service_interactions_clean
* SILVER.marketing_campaigns_clean
* SILVER.promotions_data_clean
* SILVER.store_locations_clean
* SILVER.employee_records_clean
* SILVER.supplier_information_clean
* SILVER.product_reviews_clean

 ⚠️ Certaines tables ne partagent pas de clés communes exploitables. Les analyses ont donc été réalisées à un **niveau global ou agrégé**, conformément aux bonnes pratiques en data analytics.

## 📈 Analyses réalisées

### 💰 Performance commerciale
- Évolution des ventes dans le temps
- Performance par produit, catégorie et région
- Comparaison ventes avec / sans promotion
- Sensibilité des catégories aux promotions
-L'examen des cycles de transactions met en évidence une base client solide mais une réactivité promotionnelle hétérogène.

### 📢 Marketing
- Lien entre campagnes marketing et ventes
- Identification des campagnes les plus efficaces

### 👥 Clients & expérience client
- Répartition des clients par segments démographiques
- Impact global des avis produits sur les ventes
- Influence des interactions avec le service client

### 🚚 Supply Chain & logistique
- Analyse des ruptures de stock
- Identification des catégories les plus touchées
- Impact des délais de livraison
- Indicateurs globaux de risque logistique

##Interprétation des analyses réalisées

### Analyse des Ventes (fichier sales_trends.sql)
* Analyse 2.3.1.1 – Comparaison Avec vs Sans Promotion
-Interprétation : Les données confirment que les campagnes promotionnelles tirent le panier moyen vers le haut. On passe de 5 009,16 $ en période normale à 5 308,83 $ sous promotion.
-Constat : Cela représente une hausse de 5,9% de la valeur des transactions. Les promotions ne servent pas seulement à vendre plus en volume, elles incitent les clients à monter en gamme ou à ajouter des articles au panier.

* Analyse 2.3.1.2 – Lift par Catégorie (Sensibilité)
- Interprétation : Le "Lift" mesure l'efficacité réelle. La catégorie Organic Snacks est la grande gagnante avec un Lift de +11,50%. C'est ici que l'élasticité-prix est la plus forte.
-Constat : À l'inverse, les Organic Beverages affichent un score négatif (-1,32%). Cela signifie que faire une promotion sur les boissons est contre-productif : on baisse le prix mais le panier moyen ne décolle pas, ce qui détruit de la marge sans gain de performance.

### Analyse marketing et performance commerciale (fichier campaign_performance.sql)
* Analyse 2.3.2.1 – Rentabilité par Canal (ROI)
-Interprétation : Pour respecter la baisse de 30% du budget, l'analyse identifie les canaux prioritaires. La Radio et l'Emailing sont les plus efficaces pour convertir avec des taux proches de 5,75%.
-Constat : Cependant, en termes de valeur brute, les Social Media attirent les clients à plus fort pouvoir d'achat (Panier moyen de 5 043,88 $). Le ROI calculé étant homogène, la stratégie doit basculer sur un mix "Radio pour le volume" et "Social pour la valeur".

### Analyse 2.3.3.1 & 2 – Expérience Client (Avis et SAV)
-Interprétation : Les produits plaisent (note de 4,08/5), mais la fidélisation est en danger. Le taux de résolution du Service Après-Vente est critique : seulement 32,08% des problèmes sont résolus.
-Constat : Ce faible taux de résolution est probablement la cause principale de la chute de part de marché (28% à 22%). Un client mécontent dont le problème n'est pas résolu partira chez la concurrence, peu importe la qualité du produit.-


## 🧮 Technologies utilisées

- **Snowflake** (Data Warehouse)
- **SQL** (Snowflake SQL)
- **Python 3**
- **Streamlit**
- **Plotly**
- **Git & GitHub**
- **VS Code**

## 📊 Dashboards Streamlit

Les dashboards interactifs permettent :
* la visualisation des ventes dans le temps
* l’analyse par région et catégorie
* l’exploration des indicateurs supply chain
* le suivi de l’expérience client

📦 Installation & lancement 

1. Cloner le projet
git clone https://github.com/florence93600/anycompany_food_beverage
cd anycompany-lab

2. Installer les dépendances
pip install streamlit snowflake-connector-python pandas plotly

3. Configurer la connexion Snowflake
Créer le fichier .streamlit/secrets.toml à la racine du projet :
[snowflake]
user = "FJCMMBAESG"
password = "Fjcmmbaesg020226!"
account = "bphegzs-ehb57068"
warehouse = "COMPUTE_WH"
database = "ANYCOMPANY_LAB"
schema = "SILVER"

⚠️ Ce fichier contient des identifiants : il ne doit jamais être ajouté sur GitHub.

4. Lancer les dashboards
Depuis le dossier anycompany_food_beverage :

# Dashboard Ventes & clients
streamlit run streamlit/sales_dashboard.py

# Dashboard Promotions, Stock & Logistique
streamlit run streamlit/promotion_analysis.py

# Dashboard Marketing ROI & Expérience client
streamlit run streamlit/marketing_roi.py

Les dashboards s’ouvrent sur : http://localhost:8501


