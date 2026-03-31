# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Layer : Agregations Metier et KPIs
# MAGIC
# MAGIC Ce notebook lit les donnees Silver (nettoyees et enrichies) et produit
# MAGIC des indicateurs cles de performance (KPIs) directement exploitables
# MAGIC par les equipes metier d'Enedis.
# MAGIC
# MAGIC Tables Gold produites :
# MAGIC - kpi_conso_par_region     : Consommation totale, moyenne et par site, par region et par an
# MAGIC - kpi_tendance_nationale   : Evolution nationale annuelle avec variation d'une annee sur l'autre
# MAGIC - kpi_categorie_distribution : Repartition des volumes par categorie de consommation
# MAGIC - kpi_top_regions          : Classement des regions les plus consommatrices

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration et imports

# COMMAND ----------

# --- Imports des fonctions d'agregation et de fenetrage PySpark ---
from pyspark.sql.functions import (
    col,                      # Reference a une colonne par son nom
    sum as spark_sum,         # Somme des valeurs d'une colonne (renomme pour eviter le conflit avec sum() Python)
    avg,                      # Moyenne des valeurs d'une colonne
    count,                    # Comptage du nombre de lignes
    round as spark_round,     # Arrondi a N decimales
    rank,                     # Fonction de fenetrage : attribue un rang a chaque ligne dans une partition
    desc,                     # Tri descendant (du plus grand au plus petit)
    lit,                      # Valeur constante (meme valeur pour toutes les lignes)
    when,                     # Structure conditionnelle (equivalent SQL du CASE WHEN)
    lag,                      # Fonction de fenetrage : recupere la valeur de la ligne precedente
    current_timestamp         # Horodatage exact au moment de l'execution
)

# Window est necessite pour les fonctions de fenetrage (rank, lag...)
# Une Window definit le perimetre sur lequel s'applique la fonction : partition et tri
from pyspark.sql.window import Window

from pyspark.sql.types import DoubleType  # Type virgule flottante double precision

# Nom du dataset traite dans ce notebook
# Utilise pour construire les noms des tables managees (silver_, gold_)
DATASET_NAME = "conso_inf36_region"

print("Configuration Gold chargée")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chargement des données Silver

# COMMAND ----------

# Lecture de la table Silver depuis le catalogue Databricks
# spark.read.table() lit la table managee creee par le notebook 02
df_silver = spark.read.table(f"silver_{DATASET_NAME}")

print(f"Lignes Silver chargees : {df_silver.count():,}")
print(f"Colonnes disponibles : {df_silver.columns}")

# Apercu pour verifier que les colonnes Silver sont bien présentes
df_silver.limit(3).display()

# COMMAND ----------

# Diagnostic : affichage des vraies colonnes de la table Silver
print("=== COLONNES REELLES DE LA TABLE SILVER ===")
for c in df_silver.columns:
    print(f"  - {c}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Détection dynamique des colonnes

# COMMAND ----------

from pyspark.sql.functions import year, to_timestamp

# Extraction de l'annee depuis horodate (format ISO 8601 : "2024-06-30T22:00:00+00:...")
# to_timestamp() convertit la chaine de caracteres en type timestamp Spark
# year() extrait ensuite l'annee sous forme d'entier (ex: 2024)
df_silver = df_silver.withColumn("annee", year(to_timestamp(col("horodate"))))

# Assignation manuelle des colonnes metier d'apres le vrai schema Enedis
region_col    = "region"                     # Nom de la region
annee_col     = "annee"                      # Annee extraite depuis horodate
conso_col     = "total_energie_soutiree_wh"  # Consommation totale en Wh
sites_col     = "nb_points_soutirage"        # Nombre de points de soutirage
categorie_col = "categorie_consommation"     # Colonne creee par l'UDF du notebook 02

print(f"region_col  = {region_col}")
print(f"annee_col   = {annee_col}")
print(f"conso_col   = {conso_col}")
print(f"sites_col   = {sites_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 1 - Consommation par région et par année

# COMMAND ----------

# Ce KPI repond a la question : "Combien chaque region consomme-t-elle par an ?"
# C'est l'agregation de base pour le pilotage reseau regional d'Enedis

if region_col and annee_col and conso_col:

    # Construction de la liste des expressions d'agregation
    # On prepare les calculs que l'on va appliquer dans le groupBy
    agg_exprs = [
        spark_sum(conso_col).alias("conso_totale_mwh"),   # Somme de toutes les consommations de la region sur l'annee
        avg(conso_col).alias("conso_moyenne_mwh"),         # Moyenne des consommations de la region
        count("*").alias("nb_enregistrements")             # Nombre de lignes agregees (points de mesure)
    ]

    # Si la colonne du nombre de sites est disponible, on l'ajoute a l'agregation
    if sites_col:
        agg_exprs.append(
            spark_sum(sites_col).alias("nb_sites_total")  # Total des sites de la region sur l'annee
        )

    # groupBy() regroupe les lignes par couple (region, annee)
    # agg() applique toutes les fonctions d'agregation de la liste sur chaque groupe
    df_kpi_region = (
        df_silver
        .groupBy(region_col, annee_col)  # Chaque combinaison region + annee forme un groupe
        .agg(*agg_exprs)                 # L'asterisque deplie la liste en arguments separes
        # Arrondi des valeurs numeriques a 2 decimales pour la lisibilite
        .withColumn("conso_totale_mwh",   spark_round(col("conso_totale_mwh"),  2))
        .withColumn("conso_moyenne_mwh",  spark_round(col("conso_moyenne_mwh"), 2))
    )

    # Calcul de la consommation par site (indicateur d'intensite energetique)
    if sites_col:
        df_kpi_region = df_kpi_region.withColumn(
            "conso_par_site_mwh",
            spark_round(
                when(
                    col("nb_sites_total") > 0,              # Evite la division par zero
                    col("conso_totale_mwh") / col("nb_sites_total")  # Consommation totale divisee par le nb de sites
                ).otherwise(None),  # Si nb_sites_total est 0 ou null, la valeur est nulle
                2  # Arrondi a 2 decimales
            )
        )

    # Tri par region puis par annee pour une lecture ordonnée
    df_kpi_region = df_kpi_region.orderBy(region_col, annee_col)

    print("KPI 1 - Consommation par région et par année :")
    df_kpi_region.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretation - KPI 1
# MAGIC
# MAGIC Avec 15 000 enregistrements, 12 régions métropolitaines sont représentées sur 3 années (2023, 2024, 2025) soit 36 combinaisons région × année.
# MAGIC
# MAGIC Auvergne–Rhône-Alpes reste la région la plus consommatrice, avec 23,5 milliards de Wh en 2024 soit environ trois fois la consommation de la Bretagne ou du Grand Est sur la même année. Cet écart, à mon avis, reflète son poids industriel et démographique.
# MAGIC
# MAGIC La hausse systématique de la consommation en 2024 par rapport à 2023 est visible dans toutes les régions. Le nombre d’enregistrements passe de 492 à 656 en 2024 ce qui suggère que l’API retourne davantage de points de mesure pour cette année — probablement en raison d’un découpage temporel plus fin ou d’une couverture réseau plus complète.
# MAGIC
# MAGIC L’indicateur conso_par_site_mwh met en évidence des disparités régionales intéressantes : Centre‑Val de Loire affiche la valeur la plus élevée en 2024 (350 Wh/site). 
# MAGIC
# MAGIC A mon avis, cela peut s’expliquer par une proportion plus importante de chauffage électrique dans l’habitat rural par opposition aux grandes agglomérations où la diversité des modes de chauffage est plus forte.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 2 - Évolution temporelle nationale avec variation annuelle (YoY)

# COMMAND ----------

# Ce KPI repond a : "La consommation nationale augmente-t-elle ou baisse-t-elle d'une annee sur l'autre ?"
# La variation YoY (Year over Year) est un indicateur cle de la transition energetique

if annee_col and conso_col:

    # Agregation nationale : on regroupe tout par annee (toutes regions confondues)
    df_kpi_tendance = (
        df_silver
        .groupBy(annee_col)
        .agg(
            spark_sum(conso_col).alias("conso_nationale_mwh"),  # Consommation totale nationale en MWh
            count("*").alias("nb_points_mesure")                # Nombre de points de mesure contribuant
        )
        # Conversion de MWh en TWh pour une meilleure lisibilite (1 TWh = 1 000 000 MWh)
        .withColumn(
            "conso_nationale_twh",
            spark_round(col("conso_nationale_mwh") / 1_000_000, 4)  # Division et arrondi a 4 decimales
        )
        .orderBy(annee_col)  # Tri chronologique pour que le lag() fonctionne correctement
    )

    # --- Calcul de la variation Year-over-Year avec une Window Function ---
    # Une Window Function permet d'acceder aux valeurs d'autres lignes depuis la ligne courante,
    # sans reduire le nombre de lignes (contrairement a groupBy + agg)

    # Definition de la fenetre : on ordonne les lignes par annee, sans partitionnement
    # (partitionBy() vide signifie que toutes les lignes forment un seul groupe)
    window_yoy = Window.orderBy(annee_col)

    df_kpi_tendance = (
        df_kpi_tendance

        # lag("conso_nationale_twh", 1) recupere la valeur de la ligne PRECEDENTE dans la fenetre
        # Pour l'annee 2023, lag retourne la valeur de 2022. Pour 2022, il retourne 2021, etc.
        # La premiere annee n'a pas de predecesseur : lag retourne null
        .withColumn(
            "conso_annee_precedente_twh",
            lag("conso_nationale_twh", 1).over(window_yoy)
        )

        # Calcul de la variation en pourcentage :
        # variation = (valeur_courante - valeur_precedente) / valeur_precedente * 100
        .withColumn(
            "variation_yoy_pct",
            spark_round(
                when(
                    # On ne calcule que si la valeur precedente existe et est non nulle
                    col("conso_annee_precedente_twh").isNotNull() &
                    (col("conso_annee_precedente_twh") != 0),

                    # Formule de la variation en pourcentage
                    (col("conso_nationale_twh") - col("conso_annee_precedente_twh"))
                    / col("conso_annee_precedente_twh") * 100
                )
                .otherwise(None),  # Pas de variation calculable pour la premiere annee
                2  # Arrondi a 2 decimales
            )
        )

        # Suppression de la colonne intermediaire (plus besoin apres le calcul)
        .drop("conso_annee_precedente_twh")
    )

    print("KPI 2 - Évolution nationale et variation Year-over-Year :")
    df_kpi_tendance.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpétation - KPI 2
# MAGIC
# MAGIC
# MAGIC Sur 15 000 enregistrements couvrant 12 régions, la consommation nationale agrégée atteint 89047 TWh en 2023, 134190 TWh en 2024 (+50,7%) puis 82175 TWh en 2025 (–38,76%).
# MAGIC
# MAGIC La hausse observée en 2024 est cohérente avec les constats du KPI 1 : le nombre de points de mesure passe de 4500 à 6000, ce qui indique que l’API fournit une granularité temporelle plus fine ou une couverture réseau plus complète pour cette année. Une partie de la variation YoY reflète donc la structure du dataset et non uniquement une évolution réelle de la consommation sur le réseau.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 3 - Repartition des volumes par categorie de consommation

# COMMAND ----------

# Ce KPI exploite la colonne "categorie_consommation" produite par l'UDF Python du notebook 02
# Si cette colonne est absente, on utilise "profil" comme categorie de substitution
# Il repond a : "Quelle part du volume total est consommee par chaque categorie ?"

# Determination de la colonne categorie a utiliser
if "categorie_consommation" in df_silver.columns:
    cat_col = "categorie_consommation"
elif "profil" in df_silver.columns:
    # profil contient les types de consommateurs (ENT1, ENT2, ENT3...) definis par Enedis
    cat_col = "profil"
    print("Info : categorie_consommation absente, utilisation de 'profil' comme substitut")
else:
    cat_col = None

if cat_col and conso_col:

    # Definition d'une fenetre globale (sans partition) pour calculer le total toutes categories
    # Cette fenetre couvre toutes les lignes du DataFrame : elle est utilisee pour le denominateur
    window_total = Window.partitionBy()  # partitionBy() sans argument = une seule partition globale

    df_kpi_categorie = (
        df_silver
        .groupBy(cat_col)
        .agg(
            count("*").alias("nb_mesures"),                       # Nombre de mesures pour cette categorie
            spark_sum(conso_col).alias("conso_totale_mwh")        # Consommation totale de la categorie
        )
        .withColumn("conso_totale_mwh", spark_round(col("conso_totale_mwh"), 2))

        # Calcul du pourcentage de volume que represente cette categorie sur le total national
        # spark_sum("conso_totale_mwh").over(window_total) calcule la somme de TOUTES les categories
        # On divise la conso de la categorie par ce total pour obtenir le pourcentage
        .withColumn(
            "pct_volume_total",
            spark_round(
                col("conso_totale_mwh") /
                spark_sum("conso_totale_mwh").over(window_total) * 100,
                2
            )
        )

        # Tri par volume decroissant : la categorie la plus consommatrice en premier
        .orderBy(desc("conso_totale_mwh"))
    )

    print("KPI 3 - Répartition par categorie de consommation :")
    df_kpi_categorie.display()

else:
    print("SKIP KPI 3 : aucune colonne categorie disponible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretation - KPI 3
# MAGIC
# MAGIC La colonne categorie_consommation est produite par l’UDF Python du notebook 02 qui classe chaque ligne selon son volume de consommation en trois niveaux : MODÉRÉE, ÉLEVÉE ou TRÈS ÉLEVÉE.
# MAGIC
# MAGIC La quasi‑totalité des enregistrements (99,99 %) se retrouve dans la catégorie TRÈS ÉLEVÉE. Cela s’explique par le fait que l’UDF a été conçue pour catégoriser des consommations individuelles (compteurs résidentiels exprimés en kWh) alors que les données Enedis utilisées ici sont des agrégats régionaux. Chaque ligne représente la somme de milliers de points de soutirage sur une région entière ce qui génère mécaniquement des volumes en milliards de Wh automatiquement classés dans la catégorie la plus élevée.
# MAGIC
# MAGIC La catégorie INCONNU (1 106 mesures avec consommation nulle) correspond aux lignes où la valeur total_energie_soutiree_wh est nulle ou absente dans la source empêchant l’UDF d’attribuer une catégorie.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 4 - Classement des regions les plus consommatrices

# COMMAND ----------

# Ce KPI repond a : "Quelles regions consomment le plus pour la derniere annee disponible ?"
# Utilite : priorisation des investissements reseau, planification capacitaire

if region_col and conso_col and annee_col:

    # Recuperation de la valeur maximale d'annee dans le DataFrame
    # .collect()[0][0] extrait la valeur scalaire du resultat de l'agregation Spark
    # (collect() ramene les donnees du cluster vers le driver, [0][0] accede a la premiere ligne / premiere colonne)
    derniere_annee = df_silver.agg({annee_col: "max"}).collect()[0][0]
    print(f"Classement pour l'annee : {derniere_annee}")

    # Definition de la fenetre pour le classement :
    # orderBy(desc("conso_totale_mwh")) trie les regions de la plus consommatrice a la moins consommatrice
    # Pas de partitionnement : on classe toutes les regions ensemble
    window_rank = Window.orderBy(desc("conso_totale_mwh"))

    df_kpi_top_regions = (
        df_silver

        # On filtre pour ne garder que les donnees de la derniere annee disponible
        .filter(col(annee_col) == derniere_annee)

        # Agregation par region pour obtenir la consommation totale et le nombre de sites
        .groupBy(region_col)
        .agg(
            spark_sum(conso_col).alias("conso_totale_mwh"),
            # Ajout du nombre de sites si disponible, sinon colonne nulle
            spark_sum(sites_col).alias("nb_sites") if sites_col else lit(None).alias("nb_sites")
        )

        .withColumn("conso_totale_mwh", spark_round(col("conso_totale_mwh"), 2))

        # rank() attribue un numero de classement selon la fenetre definie
        # La region avec la plus grande consommation recoit le rang 1
        # En cas d'egalite, les deux regions recoivent le meme rang (ex: 1, 1, 3...)
        .withColumn("classement", rank().over(window_rank))

        # Tri par classement croissant pour afficher la region n°1 en premier
        .orderBy("classement")
    )

    print(f"KPI 4 - Classement des régions ({derniere_annee}) :")
    df_kpi_top_regions.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretation - KPI 4
# MAGIC
# MAGIC Le classement 2025 couvre 12 des 13 régions métropolitaines françaises. La seule région absente est vraisemblablement la Corse dont la faible population, à mon avis, génère trop peu de lignes dans le dataset avec limit = 15000.
# MAGIC
# MAGIC Auvergne–Rhône-Alpes confirme sa position de leader avec 14,3 milliards de Wh soit environ quatre fois la consommation de la Normandie (rang 12). Cet écart reflète les différences de tissu industriel, de densité de population et de profil de consommation entre les régions.
# MAGIC
# MAGIC L’Île‑de‑France se positionne au rang 2 avec 9,7 milliards de Wh un résultat beaucoup plus cohérent avec son poids démographique (12 millions d’habitants).
# MAGIC
# MAGIC La fonction rank() pour gérer automatiquement les ex‑æquo en attribuant le même rang à deux régions ayant une consommation identique.
# MAGIC
# MAGIC J’ai voulu intégrer ce KPI car il constitue selon moi un indicateur directement exploitable pour prioriser les investissements réseau et orienter la planification capacitaire régionale en identifiant clairement les zones les plus consommatrices et donc les plus critiques pour le dimensionnement du réseau.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Écriture des tables Gold en Delta Lake

# COMMAND ----------

# Dictionnaire associant chaque nom de table Gold au DataFrame correspondant
# On utilise dir() pour verifier si le DataFrame a bien ete cree (evite une NameError)
gold_tables = {
    "kpi_conso_par_region":       df_kpi_region      if "df_kpi_region"      in dir() else None,
    "kpi_tendance_nationale":     df_kpi_tendance    if "df_kpi_tendance"    in dir() else None,
    "kpi_categorie_distribution": df_kpi_categorie   if "df_kpi_categorie"   in dir() else None,
    "kpi_top_regions":            df_kpi_top_regions if "df_kpi_top_regions" in dir() else None,
}

print("Ecriture des tables Gold :")

for table_name, df in gold_tables.items():

    if df is not None:
        (
            # Ajout de l'horodatage de la creation de la table Gold (traçabilite)
            df.withColumn("_gold_timestamp", current_timestamp())
              .write
              .format("delta")                    # Format Delta Lake
              .mode("overwrite")                  # Remplacement complet si la table existe
              .option("overwriteSchema", "true")  # Mise a jour du schema si necessaire
              .saveAsTable(table_name)             # Enregistrement comme table managee dans le catalogue
                                                  # (compatible Serverless, pas besoin d'acces DBFS)
        )

        print(f"  [OK] {table_name:<40} -> table managée enregistrée")

    else:
        # Si le DataFrame n'a pas pu etre cree (colonne manquante, erreur precedente...)
        # on logue un avertissement mais on ne bloque pas les autres tables
        print(f"  [SKIP] {table_name:<40} -> ignoree (données manquantes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vue d'ensemble du pipeline complet
# MAGIC
# MAGIC     API Open Data Enedis
# MAGIC          |
# MAGIC          v
# MAGIC     BRONZE  : donnees brutes Delta Lake + metadonnees d'ingestion
# MAGIC          |
# MAGIC          v
# MAGIC     SILVER  : donnees nettoyées + UDFs Python + Data Quality + RGPD
# MAGIC          |
# MAGIC          v
# MAGIC     GOLD    : KPIs metier prets pour exploitation BI / dashboards
# MAGIC
# MAGIC Compétences techniques illustrées dans ce projet :
# MAGIC
# MAGIC   - Pipeline d'ingestion batch avec Delta Lake (Bronze)
# MAGIC
# MAGIC   - Transformations PySpark avec detection dynamique du schema (Silver)
# MAGIC
# MAGIC   - UDFs Python : categorisation energetique et score carbone (Silver)
# MAGIC
# MAGIC   - Controles qualite programmatiques avec rapport consolide (Silver)
# MAGIC
# MAGIC   - Conformite RGPD par k-anonymisation (Silver)
# MAGIC
# MAGIC   - Window Functions Spark pour les calculs de rang et de variation YoY (Gold)
# MAGIC
# MAGIC   - Architecture Lakehouse Bronze / Silver / Gold avec Delta Lake