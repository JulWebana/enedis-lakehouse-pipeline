# Pipeline de donnees Enedis - Architecture Lakehouse Bronze / Silver / Gold

Pipeline de données complet sur les donnees Open Data Enedis, construit sur
Databricks Community Edition avec PySpark et Delta Lake.

---

## Contexte

Enedis est le gestionnaire du reseau de distribution d'électricite en France.
Ce projet exploite les donnees Open Data d'Enedis pour construire un pipeline
analytique de bout en bout depuis l'ingestion des données brutes jusqu'à la
production de KPIs métier et de visualisations.

---

## Architecture

```
API Open Data Enedis
        |
        v
BRONZE  : données brutes + metadonnees d'ingestion (Delta Lake)
        |
        v
SILVER  : données nettoyees + UDFs Python + Data Quality + RGPD (Delta Lake)
        |
        v
GOLD    : KPIs métier prêts pour exploitation BI / dashboards (Delta Lake)
        |
        v
VISUALISATIONS : graphiques matplotlib par KPI
```

---

## Stack technique

| Composant         | Technologie                          |
|-------------------|--------------------------------------|
| Plateforme        | Databricks Community Edition         |
| Moteur de calcul  | Apache Spark (PySpark)               |
| Format de stockage| Delta Lake (tables managees)         |
| Langage principal | Python 3                             |
| Source de donnees | API Open Data Enedis (REST / CSV)    |
| Visualisations    | Matplotlib                           |

---

## Dataset

**Source** : [Open Data Enedis](https://opendata.enedis.fr)

**Dataset utilise** : `conso-inf36-region`
Consommation électrique des consommateurs dont la puissance souscrite
est inferieure ou égale a 36 kVA (périmetre residentiel et petits professionnels),
agregée par region, par profil de consommation et par période.

**Volume ingéré** : 15 000 lignes couvrant 12 regions métropolitaines sur 3 ans (2023-2025)

---

## Structure du projet

```
enedis_pipeline/
|-- 01_Bronze_Ingestion.py       Téléchargement API et stockage brut Delta Lake
|-- 02_Silver_Transformation.py  Nettoyage, UDFs Python, Data Quality, RGPD
|-- 03_Gold_Aggregations.py      Calcul des KPIs métier et interpretations
|-- 04_Visualisations.py         Graphiques matplotlib par KPI
|-- README.md                    Documentation du projet
```

---

## Notebooks

### 01 - Bronze : Ingestion

- Appel de l'API Open Data Enedis via `requests`
- Conversion Pandas -> Spark (`createDataFrame`)
- Ajout des métadonnées d'ingestion : `_ingestion_timestamp`, `_ingestion_date`, `_source_url`
- Ecriture en table Delta Lake managee via `saveAsTable`
- Rapport d'ingestion consolide (statut, nb lignes, duree)

### 02 - Silver : Transformation

- Lecture depuis la table Bronze (`spark.read.table`)
- Standardisation des noms de colonnes (suppression accents, snake_case)
- Détection et cast des types métier (DoubleType, LongType)
- **UDF Python 1** : `categorize_consumption_udf` - catégorise la consommation en MODEREE / ELEVEE / TRES ELEVEE
- **UDF Python 2** : `compute_carbon_score_udf` - calcule un score carbone normalisé par point de soutirage
- Contrôles Data Quality : nulls, doublons, valeurs négatives
- Conformité RGPD par k-anonymisation (seuil : minimum 5 sites par ligne)
- Ajout des métadonnées Silver : `_silver_timestamp`, `_silver_date`, `_rgpd_min_sites`
- Ecriture en table Delta Lake managee

### 03 - Gold : KPIs métier

- **KPI 1** : Consommation totale, moyenne et par site par région et par année
- **KPI 2** : Evolution nationale avec variation Year-over-Year (Window Function `lag`)
- **KPI 3** : Répartition des volumes par catégorie de consommation (Window Function `partitionBy`)
- **KPI 4** : Classement des régions les plus consommatrices (Window Function `rank`)
- Interprétation métier de chaque KPI

### 04 - Visualisations

- KPI 1 : Barres groupées par région et par année
- KPI 2 : Courbe de tendance nationale + barres YoY (vert/rouge selon signe)
- KPI 3 : Barres horizontales + camembert de répartition
- KPI 4 : Barres horizontales classées avec ligne de moyenne nationale

---

## Compétences techniques illustrées

- Pipeline d'ingestion batch avec appel API REST et gestion des erreurs
- Architecture Lakehouse Bronze / Silver / Gold avec Delta Lake
- UDFs Python enrégistrées et appelées via `spark.udf.register` et `expr()`
- Contrôles qualité programmatiques avec rapport consolidé
- Conformité RGPD par k-anonymisation implémentée en PySpark
- Window Functions : `lag()`, `rank()`, `partitionBy()` pour les agregations avancées
- Détection dynamique du schema et assignation manuelle en fallback
- Compatibilite Serverless : `saveAsTable()` au lieu de chemins DBFS
- Visualisations matplotlib avec double axe Y et palette metier

---

## Lancement

Importer les 4 fichiers `.py` dans Databricks (Workspace > Import), puis executer
dans l'ordre :

```
01_Bronze_Ingestion.py       -> cree les tables bronze_*
02_Silver_Transformation.py  -> cree les tables silver_*
03_Gold_Aggregations.py      -> cree les tables kpi_*
04_Visualisations.py         -> affiche les graphiques
```

---

## Tables produites

| Couche | Nom de la table               | Description                              |
|--------|-------------------------------|------------------------------------------|
| Bronze | bronze_conso_inf36_region     | Données brutes Enedis + métadonnées      |
| Silver | silver_conso_inf36_region     | Données nettoyées + colonnes UDF         |
| Gold   | kpi_conso_par_region          | Consommation par région et par année     |
| Gold   | kpi_tendance_nationale        | Evolution nationale YoY                  |
| Gold   | kpi_categorie_distribution    | Répartition par catégorie                |
| Gold   | kpi_top_regions               | Classement des regions 2025              |

---

## Limites du projet

### Volume de donnees
Le pipeline est configure avec `limit=15000` pour rester compatible avec les
contraintes de Databricks Community Edition. Cela couvre 12 regions sur 13
(la Corse est absente, trop peu representee dans l'echantillon). Sur le jeu
complet (`limit=-1`), les 13 regions metropolitaines seraient toutes visibles.

### Perimetre du dataset
Seul le dataset `conso-inf36-region` (consommateurs <= 36 kVA) est analyse.
Le dataset `conso-sup36-region` (grands consommateurs industriels) n'est pas
inclus, ce qui explique notamment le classement inattendu de l'Ile-de-France
dans certaines analyses partielles.

### UDF de categorisation
L'UDF `categorize_consumption_udf` a ete concue pour categoriser des
consommations individuelles (compteurs residentiels en kWh). Appliquee sur
des agregats regionaux (milliards de Wh), elle classe quasi systematiquement
toutes les lignes en TRES ELEVEE. En production, les seuils seraient calibres
sur la granularite reelle des donnees, ou la categorisation serait appliquee
avant l'agregation.

### Biais temporel
Le nombre de points de mesure par annee n'est pas homogene (492 en 2023 et 2025,
656 en 2024), ce qui amplifie artificiellement la consommation agregee de 2024.
Les variations YoY observees sont donc partiellement liees a la structure des
donnees et non uniquement a des tendances reelles de consommation.

### Environnement
Ce projet tourne sur Databricks Community Edition (mode Serverless), ce qui
implique certaines contraintes : pas d'acces DBFS root, pas de support Scala,
et des ressources de calcul limitees. En environnement de production, le pipeline
beneficierait de partitionnement Delta, de workflows orchestres (Databricks Jobs)
et d'un catalogue Unity Catalog pour la gouvernance des donnees.


Remarque : Les visualisations Matplotlib que j’ai produites sont uniquement destinées à la démonstration. En production, les tables Gold seraient directement connectées à Power BI, Tableau ou tout autre outil de visualisation via le connecteur Databricks, sans passer par une génération de graphiques côté Python
