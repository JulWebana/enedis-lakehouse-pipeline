# Data Lakehouse Pipeline on Databricks – Enedis Energy Open Data

Conception et développement d’un pipeline scalable en architecture Lakehouse (Bronze / Silver / Gold) :
- Ingestion via API REST (Open Data Enedis)
- Transformations métier et structuration des données
- Mise en place de contrôles de Data Quality (profiling, intégrité, complétude)
- Conformité RGPD
- Automatisation CI/CD avec tests unitaires

Production de datasets analytiques fiables pour le suivi des indicateurs énergétiques.

---

## Architecture

```
API Open Data Enedis
        |
        v
BRONZE  : données brutes + métadonnées d'ingestion (Delta Lake)
        |
        v
SILVER  : données nettoyées + UDFs Python + contrôles qualité + conformité RGPD (Delta Lake)
        |
        v
GOLD    : KPIs métier prêts pour exploitation analytique (Delta Lake)
        |
        v
VISUALISATIONS : graphiques matplotlib par KPI
```

---

## Stack technique

| Composant          | Technologie                       |
|--------------------|-----------------------------------|
| Plateforme         | Databricks Community Edition      |
| Moteur de calcul   | Apache Spark (PySpark)            |
| Format de stockage | Delta Lake (tables managées)      |
| Langage principal  | Python 3                          |
| Source de données  | API Open Data Enedis (REST / CSV) |
| Visualisations     | Matplotlib                        |
| CI/CD              | GitHub Actions (flake8, pytest)   |

---

## Dataset

**Source** : [Open Data Enedis](https://opendata.enedis.fr)

**Dataset utilisé** : `conso-inf36-region`

Consommation électrique des consommateurs dont la puissance souscrite est inférieure
ou égale à 36 kVA (résidentiel et petits professionnels), agrégée par région, profil
de consommation et période.

**Volume ingéré** : 15 000 lignes couvrant 12 régions métropolitaines sur 3 ans (2023-2025)

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

### 01 - Couche Bronze : Ingestion

- Appel de l'API Open Data Enedis via `requests`
- Conversion Pandas → Spark (`createDataFrame`)
- Ajout des métadonnées d'ingestion : `_ingestion_timestamp`, `_ingestion_date`, `_source_url`
- Écriture en table Delta Lake managée via `saveAsTable`

### 02 - Couche Silver : Transformation

- Standardisation des noms de colonnes (suppression des accents, snake_case)
- Cast des types métier (`DoubleType`, `LongType`)
- **UDF Python 1** : `categorize_consumption_udf` — catégorisation de la consommation électrique
- **UDF Python 2** : `compute_carbon_score_udf` — calcul d'un score carbone normalisé
- Contrôles Data Quality : valeurs nulles, doublons, valeurs négatives
- Conformité RGPD par k-anonymisation (seuil minimum : 5 sites par ligne)
- Écriture en table Delta Lake managée

### 03 - Couche Gold : KPIs métier

- **KPI 1** : Consommation totale, moyenne et par site, par région et par année
- **KPI 2** : Évolution nationale avec variation Year-over-Year (Window Function `lag`)
- **KPI 3** : Répartition des volumes par catégorie de consommation (Window Function `partitionBy`)
- **KPI 4** : Classement des régions les plus consommatrices (Window Function `rank`)

### 04 - Visualisations

- KPI 1 : Barres groupées par région et par année
- KPI 2 : Courbe de tendance nationale + barres YoY (vert/rouge selon signe)
- KPI 3 : Barres horizontales + camembert de répartition
- KPI 4 : Barres horizontales classées avec ligne de moyenne nationale

---

## Tests unitaires

Les fonctions métier (UDFs) sont testées avec **pytest** :

- `test_categorize_none` — valeur nulle retourne "INCONNU"
- `test_categorize_tres_faible` — consommation < 1 000 MWh
- `test_categorize_faible` — consommation < 5 000 MWh
- `test_categorize_moderee` — consommation < 20 000 MWh
- `test_categorize_elevee` — consommation < 100 000 MWh
- `test_categorize_tres_elevee` — consommation > 100 000 MWh
- `test_carbon_score_none_consumption` — consommation nulle
- `test_carbon_score_none_sites` — nombre de sites nul
- `test_carbon_score_zero_sites` — division par zéro
- `test_carbon_score_valide` — résultat positif attendu
  
---

## CI/CD

Un pipeline **GitHub Actions** se déclenche automatiquement à chaque push sur `main` :

- Vérification de la syntaxe avec **flake8**
- Exécution des **tests unitaires pytest**

---

## Tables produites

| Couche | Nom de la table            | Description                          |
|--------|----------------------------|--------------------------------------|
| Bronze | bronze_conso_inf36_region  | Données brutes Enedis + métadonnées  |
| Silver | silver_conso_inf36_region  | Données nettoyées + colonnes UDF     |
| Gold   | kpi_conso_par_region       | Consommation par région et par année |
| Gold   | kpi_tendance_nationale     | Évolution nationale YoY              |
| Gold   | kpi_categorie_distribution | Répartition par catégorie            |
| Gold   | kpi_top_regions            | Classement des régions               |

---

## Lancement

Importer les fichiers `.py` dans Databricks (Workspace > Import), puis exécuter dans l'ordre :

```
01_Bronze_Ingestion.py       : crée les tables bronze_*
02_Silver_Transformation.py  : crée les tables silver_*
03_Gold_Aggregations.py      : crée les tables kpi_*
04_Visualisations.py         : affiche les graphiques
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

## Limites

### Volume de données
Le pipeline est configuré avec `limit=15000` pour rester compatible avec les contraintes de Databricks Community Edition. Cela couvre 12 régions sur 13 (la Corse est absente, trop peu représentée dans l'échantillon). Sur le jeu complet (`limit=-1`), les 13 régions métropolitaines seraient toutes visibles.

### Périmètre du dataset
Seul le dataset `conso-inf36-region` (consommateurs ≤ 36 kVA) est analysé. Le dataset `conso-sup36-region` (grands consommateurs industriels) n'est pas inclus, ce qui explique notamment le classement inattendu de l'Île-de-France dans certaines analyses partielles.

### UDF de catégorisation
L'UDF `categorize_consumption_udf` a été conçue pour catégoriser des consommations individuelles (compteurs résidentiels en kWh). Appliquée sur des agrégats régionaux (milliards de Wh), elle classe quasi systématiquement toutes les lignes en TRES ELEVEE. En production, les seuils seraient calibrés sur la granularité réelle des données, ou la catégorisation serait appliquée avant l'agrégation.

### Biais temporel
Le nombre de points de mesure par année n'est pas homogène (492 en 2023 et 2025, 656 en 2024), ce qui amplifie artificiellement la consommation agrégée de 2024. Les variations YoY observées sont donc partiellement liées à la structure des données et non uniquement à des tendances réelles de consommation.

### Environnement
Ce projet tourne sur Databricks Community Edition (mode Serverless), ce qui implique certaines contraintes : pas d'accès DBFS root, pas de support Scala, et des ressources de calcul limitées. En environnement de production, le pipeline bénéficierait d'un partitionnement Delta, de workflows orchestrés (Databricks Jobs) et d'un catalogue Unity Catalog pour la gouvernance des données.

---

## Améliorations possibles

- Ajout du streaming (Kafka / ingestion temps réel)
- Intégration d'un dashboard BI (Power BI / Looker)
- Ajout d'un modèle ML de prédiction de consommation
- Exposition via API (FastAPI)
