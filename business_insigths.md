# Business Insights
## Interprétation des analyses réalisées

### Analyse des Ventes (fichier sales_trends.sql)
* Analyse 2.3.1.1 – Comparaison Avec vs Sans Promotion
**Interprétation : Les données confirment que les campagnes promotionnelles tirent le panier moyen vers le haut. On passe de 5 009,16 $ en période normale à 5 308,83 $ sous promotion.----
**Constat : Cela représente une hausse de 5,9% de la valeur des transactions. Les promotions ne servent pas seulement à vendre plus en volume, elles incitent les clients à monter en gamme ou à ajouter des articles au panier.
* Analyse 2.3.1.2 – Lift par Catégorie (Sensibilité)
** Interprétation : Le "Lift" mesure l'efficacité réelle. La catégorie Organic Snacks est la grande gagnante avec un Lift de +11,50%. C'est ici que l'élasticité-prix est la plus forte.
** Constat : À l'inverse, les Organic Beverages affichent un score négatif (-1,32%). Cela signifie que faire une promotion sur les boissons est contre-productif : on baisse le prix mais le panier moyen ne décolle pas, ce qui détruit de la marge sans gain de performance.

### Analyse marketing et performance commerciale (fichier campaign_performance.sql)
* Analyse 2.3.2.1 – Rentabilité par Canal (ROI)
**Interprétation : Pour respecter la baisse de 30% du budget, l'analyse identifie les canaux prioritaires. La Radio et l'Emailing sont les plus efficaces pour convertir avec des taux proches de 5,75%.
**Constat : Cependant, en termes de valeur brute, les Social Media attirent les clients à plus fort pouvoir d'achat (Panier moyen de 5 043,88 $). Le ROI calculé étant homogène, la stratégie doit basculer sur un mix "Radio pour le volume" et "Social pour la valeur".
### Analyse 2.3.3.1 & 2 – Expérience Client (Avis et SAV)
**Interprétation : Les produits plaisent (note de 4,08/5), mais la fidélisation est en danger. Le taux de résolution du Service Après-Vente est critique : seulement 32,08% des problèmes sont résolus.
**Constat : Ce faible taux de résolution est probablement la cause principale de la chute de part de marché (28% à 22%). Un client mécontent dont le problème n'est pas résolu partira chez la concurrence, peu importe la qualité du produit.

### Analyse des Ventes (fichier : promotion_impact.sql
/* ================================================================================
INTERPRÉTATION BUSINESS : OPÉRATIONS & LOGISTIQUE
================================================================================

2.3.1 - IMPACT ET INTENSITÉ DES RABAIS
--------------------------------------
* CONSTAT : Les efforts financiers sont concentrés sur les "Organic Snacks" (remise moyenne de 15,7%).
* RÉSULTAT : Cette intensité promotionnelle valide les excellents scores de Lift observés 
  sur cette catégorie spécifique.

2.3.4.1 - DIAGNOSTIC DES RUPTURES DE STOCK
------------------------------------------
* RISQUE : Une fragilité critique est identifiée sur le "Baby Food" (2,85%) et les "Beverages" (2,82%).
* ALERTE : Le nombre élevé d'alertes de réapprovisionnement (21 sur les boissons) suggère que la 
  baisse des ventes est liée à un défaut de disponibilité en rayon.

2.3.4.2 - PERFORMANCE DE LA CHAÎNE LOGISTIQUE
---------------------------------------------
* TRANSPORT : Les transporteurs sont irréprochables avec un taux d'erreur de 0,00%.
* BLOCAGE : Le problème est en amont : le délai fournisseur (Lead Time) moyen de 55 jours est 
  trop long pour l'agilité requise par le marché.
* ACTION : Réduire ce délai à 30 jours est impératif pour sécuriser la croissance.
==================================================================================

