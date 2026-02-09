

# 📋 Présentation du Projet

Ce projet consiste en la transformation complète de la chaîne de valeur des données d'AnyCompany. Nous avons mis en place une architecture robuste sur **Snowflake** et développé des solutions d'intelligence analytique avec **Python**. Le pipeline couvre l'ingestion (**Bronze**), le nettoyage (**Silver**), la structuration métier (**Analytics**) et l'analyse prédictive (**ML**).

# 👥 L'Équipe & Répartition des Tâches

Ce travail est le fruit d'une collaboration structurée :

# 📂 Structure du Répertoire

# ⚙️ Installation & Prérequis

# 🚀 Workflow Technique

* **Ingestion & Nettoyage** : Passage des données brutes vers un état "Ready-to-use" (SILVER).
* **Audit Automatisé** : Contrôle d'intégrité via une procédure stockée Python dans Snowflake.
* **Analytics Engineering** : Création de tables orientées métier (CUSTOMER_360, SALES_HISTORY).
* **Intelligence Artificielle** : Extraction de la polarité des avis clients par traitement du langage naturel (NLP).

**Parties Communes** : Design de l'architecture Cloud, Gouvernance des données et Standardisation du schéma SILVER.

**Technologies utilisées**

* *Base de données* : Snowflake (SQL)
* *Langage* : Python 3.x
* VS Code
* Git et GitHub
* Google Meet (pour des réunions)

**Librairies principales** - Pandas (Manipulation de données)

* Matplotlib & Seaborn (Visualisations avancées)
* SQLAlchemy (Moteur de connexion)
* VADER (Analyse de sentiment lexicale)

# Répartition des tâches

# Florence : Data Preparation & Ingestion

## Activités réalisées

1. Préparer l’environnement Snowflake.
2. Créer les différentes tables.
3. Charger les données.
4. Vérifications et nettoyage des données (data cleaning) pour la fiabilité des données.

## Mode opératoire :

1. Se connecter au compte Snowflake du projet :
[snowflake]
user = "FJCMMBAESG"
password = "Fjcmmbaesg020226!"
account = "BPHEGZS-EHB57068"
warehouse = "COMPUTE_WH"
database = "ANYCOMPANY_LAB"
schema = "SILVER"
2. Ouvrir le fichier SQL `load_data.sql` et lancer les codes bloc par bloc pour assurer une bonne exécution des requêtes.
3. Ouvrir le fichier SQL `Clean_data.sql` et lancer les codes bloc par bloc pour assurer le bon chargement des données.

# Carole : Exploration des données et analyses business

## Activités réalisées

1. Assurer la compréhension des jeux de données.
2. Faire des analyses exploratoires descriptives (Les 11 tables du schéma SILVER).
3. Faire des analyses business transverses (fichier `campaign_performance.sql`, `promotion_impact.sql`, fichier `sales_trends.sql`).

## Mode opératoire

Pour arriver à voir l'ensemble des analyses, il faut :

1. Lancer les fichiers dédiés à la préparation de données.
2. Lancer chaque fichier et les codes bloc par bloc pour visualiser chaque analyse séparément.

# Marie Paule : Visualisations Streamlit

## Objectif

Les dashboards interactifs permettent :

* La visualisation des ventes dans le temps.
* L’analyse par région et catégorie.
* L’exploration des indicateurs supply chain.
* Le suivi de l’expérience client.

## Mode opératoire

1. Cloner le projet : `git clone https://github.com/florence93600/anycompany_food_beverage`
2. Installer les dépendances : `pip install streamlit snowflake-connector-python pandas plotly`
3. Configurer la connexion Snowflake :
* Créer le fichier `.streamlit/secrets.toml` à la racine du projet :
[snowflake]
user = "FJCMMBAESG"
password = "Fjcmmbaesg020226!"
account = "bphegzs-ehb57068"
warehouse = "COMPUTE_WH"
database = "ANYCOMPANY_LAB"
schema = "SILVER"
*P.S.* Ce fichier contient des identifiants : il ne doit jamais être ajouté sur GitHub.


4. Lancer les dashboards depuis le dossier `anycompany_food_beverage` :
* Dashboard Ventes & clients : `streamlit run streamlit/sales_dashboard.py`
* Dashboard Promotions, Stock & Logistique : `streamlit run streamlit/promotion_analysis.py`
* Dashboard Marketing ROI & Expérience client : `streamlit run streamlit/marketing_roi.py`
*P.S.* Les dashboards s’ouvrent sur : http://localhost:8501

# Missael : Data Products & Machine Learning

## Activités réalisées

1. Enrichir et renommer 4 tables de Data Product (CUSTOMER_360, SALES_HISTORY, MARKETING_INITIATIVES, PRODUCT_SENTIMENT).
2. Réaliser l'analyse de sentiment afin de découvrir le ressenti ou l'expérience globale des clients par rapport à l'entreprise.

*P.S.* Ces activités ont été réalisées sur VS Code en raison des contraintes rencontrées avec Snowflake, surtout lors de l'installation des packages Python nécessaires pour les analyses.

## Mode opératoire :

1. Préparer l'environnement de VS Code pour supporter les trois langages (Python, SQL et Markdown).
2. Installer les packages (`pandas`, `numpy`, `matplotlib`, `seaborn`, `vaderSentiment`, `sqlalchemy`, `snowflake-sqlalchemy`, `ipython-sql`).
3. S'assurer que les tables sont disponibles dans le schéma SILVER sur Snowflake.
4. Connexion de VS Code aux données sources de Snowflake (tables dans SILVER).
5. Lancer le notebook (Run all).