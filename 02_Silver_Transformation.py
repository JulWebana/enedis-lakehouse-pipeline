# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Layer : Transformation, Qualite et UDFs Python
# MAGIC
# MAGIC Ce notebook lit les donnees Bronze (brutes), puis applique :
# MAGIC - Nettoyage : standardisation des noms de colonnes, cast des types, suppression des doublons
# MAGIC - UDFs Python : categorisation energetique et score carbone (equivalent Scala en production)
# MAGIC - Data Quality : controles qualite avec rapport detaille
# MAGIC - Conformite RGPD : suppression des lignes exposant des donnees individuelles
# MAGIC
# MAGIC Le resultat est ecrit en Delta Lake Silver, pret pour les agregations Gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration et imports

# COMMAND ----------

# --- Imports Python ---

import unicodedata  # Module Python pour normaliser les caracteres Unicode (ex: supprimer les accents)
import re           # Module Python pour les expressions regulieres (nettoyage de chaines de caracteres)

from datetime import datetime  # Pour mesurer les durees et horodater les etapes

# --- Imports des fonctions PySpark ---
# Ces fonctions s'appliquent sur des colonnes de DataFrames Spark
from pyspark.sql.functions import (
    col,               # Permet de referencer une colonne par son nom dans une expression Spark
    current_timestamp, # Retourne l'horodatage exact au moment de l'execution du traitement
    current_date,      # Retourne la date du jour au moment de l'execution
    lit,               # Cree une colonne avec une valeur constante (meme valeur pour toutes les lignes)
    when,              # Structure conditionnelle (equivalent du IF/CASE WHEN en SQL)
    expr,              # Permet d'ecrire une expression SQL sous forme de chaine de caracteres
    round as spark_round  # Arrondit une valeur numerique a N decimales (renomme pour eviter le conflit avec round() Python)
)

# --- Imports des types de donnees Spark ---
# Utilises pour convertir (caster) les colonnes vers le bon type
from pyspark.sql.types import (
    DoubleType,   # Type virgule flottante double precision (pour les valeurs de consommation en MWh)
    IntegerType,  # Type entier (pour les annees)
    StringType    # Type chaine de caracteres (pour les libelles, categories...)
)

# Nom du dataset traite dans ce notebook
# Ce nom est utilise pour construire les noms des tables managees (bronze_, silver_)
DATASET_NAME = "conso_inf36_region"

print("Configuration Silver chargee")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chargement des données Bronze

# COMMAND ----------

# Lecture de la table Bronze depuis le catalogue Databricks
# spark.read.table() lit une table managee enregistree par le notebook 01
# Le nom suit la convention bronze_ + nom du dataset
df_bronze = spark.read.table(f"bronze_{DATASET_NAME}")

# Affichage du nombre total de lignes (action Spark : parcourt toutes les partitions)
print(f"Lignes chargees depuis Bronze : {df_bronze.count():,}")

# Affichage du schema : nom de chaque colonne et son type Spark detecte automatiquement
print(f"Colonnes disponibles : {df_bronze.columns}")

# Apercu des 3 premieres lignes dans un tableau interactif Databricks
df_bronze.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## UDFs Python - Catégorisation énergetique
# MAGIC
# MAGIC Une UDF (User Defined Function) est une fonction personnalisee que l'on enregistre
# MAGIC dans Spark pour l'utiliser comme n'importe quelle fonction SQL sur les colonnes d'un DataFrame.
# MAGIC
# MAGIC Note technique : dans un environnement de production avec un cluster Spark classique,
# MAGIC ces UDFs seraient implementees en Scala pour de meilleures performances (Scala s'execute
# MAGIC nativement sur la JVM de Spark, sans serialisation/deserialisation Python).
# MAGIC Dans ce contexte Serverless, on utilise l'equivalent Python qui produit le meme resultat.

# COMMAND ----------

# Import necessaire pour creer des UDFs PySpark
from pyspark.sql.functions import udf

# Import du type de retour de l'UDF : on doit indiquer a Spark le type de la valeur retournee
# StringType signifie que la fonction retourne une chaine de caracteres
from pyspark.sql.types import StringType, DoubleType

# --- UDF 1 : Categorisation de la consommation electrique ---

# Definition de la fonction Python pure qui contient la logique metier
def categorize_consumption(consumption):
    """
    Categorise une valeur de consommation electrique (en MWh) en une etiquette lisible.

    Cette logique serait identique en Scala dans un cluster de production :
        val categorizeConsumption = udf((consumption: java.lang.Double) => { ... })

    Parametres :
        consumption : valeur numerique en MWh (peut etre None si donnee manquante)

    Retourne :
        chaine de caracteres representant la categorie de consommation
    """

    # Si la valeur est nulle (donnee absente ou non renseignee), on retourne "INCONNU"
    
    if consumption is None:
        return "INCONNU"

    # Moins de 1 000 MWh : petite zone (commune rurale, quartier peu dense)
    elif consumption < 1000.0:
        return "TRES FAIBLE"

    # Entre 1 000 et 5 000 MWh : consommation moderee (bourg, petite ville)
    elif consumption < 5000.0:
        return "FAIBLE"

    # Entre 5 000 et 20 000 MWh : consommation significative (ville moyenne)
    elif consumption < 20000.0:
        return "MODEREE"

    # Entre 20 000 et 100 000 MWh : forte consommation (grande ville)
    elif consumption < 100000.0:
        return "ELEVEE"

    # Au-dela de 100 000 MWh : tres forte consommation (metropole, zone industrielle dense)
    else:
        return "TRES ELEVEE"


# udf() transforme la fonction Python en UDF Spark exploitable sur les colonnes d'un DataFrame
# StringType() indique a Spark que la fonction retourne une chaine de caracteres
categorize_consumption_udf = udf(categorize_consumption, StringType())

# Enregistrement de l'UDF dans le catalogue SQL de Spark
# Cela permet de l'appeler via expr() ou directement dans des requetes SQL Spark
spark.udf.register("categorize_consumption_udf", categorize_consumption, StringType())

print("UDF categorize_consumption_udf enregistree")

# COMMAND ----------

# --- UDF 2 : Score carbone normalise par site ---

def compute_carbon_score(consumption, nb_sites):
    """
    Calcule un score d'intensite carbone normalise entre 0 et 100,
    base sur la consommation moyenne par site de livraison.

    Logique equivalente Scala (cluster classique) :
        val computeCarbonScore = udf((consumption: java.lang.Double, nbSites: java.lang.Long) => { ... })

    Parametres :
        consumption : consommation totale en MWh (peut etre None)
        nb_sites    : nombre de sites de livraison (peut etre None)

    Retourne :
        score flottant entre 0.0 et 100.0, ou None si calcul impossible
    """

    # Si l'une des deux valeurs est manquante, ou si nb_sites vaut 0
    # (on ne peut pas diviser par zero), on retourne None
    if consumption is None or nb_sites is None or nb_sites == 0:
        return None

    # Calcul de la consommation moyenne par site (en MWh)
    conso_par_site = consumption / nb_sites

    # Normalisation sur une echelle 0-100 :
    # on divise par 50 pour ramener a une echelle relative,
    # on multiplie par 10 pour conserver une decimale apres l'arrondi,
    # round() arrondit a l'entier le plus proche,
    # on divise par 10.0 pour retrouver la decimale,
    # min(100.0, ...) plafonne le score a 100 maximum
    score = min(100.0, round(conso_par_site / 50.0 * 10.0) / 10.0)

    return score


# Enregistrement de l'UDF avec le type de retour DoubleType (nombre a virgule flottante)
compute_carbon_score_udf = udf(compute_carbon_score, DoubleType())
spark.udf.register("compute_carbon_score_udf", compute_carbon_score, DoubleType())

print("UDF compute_carbon_score_udf enregistree")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nettoyage et standardisation des colonnes

# COMMAND ----------

def standardize_column_names(df):
    """
    Normalise les noms de toutes les colonnes d'un DataFrame Spark :
    1. Supprime les accents (ex: "Région" -> "Region")
    2. Remplace les espaces et caracteres speciaux par des underscores
    3. Met tout en minuscules (snake_case)

    Parametres :
        df : DataFrame Spark a normaliser

    Retourne :
        DataFrame Spark avec les noms de colonnes standardises
    """

    new_cols = []  # Liste qui va accumuler les nouveaux noms de colonnes

    for c in df.columns:  # Iteration sur chaque nom de colonne actuel

        # Etape 1 : decomposition Unicode NFD
        # NFD separe les caracteres et leurs accents en entites distinctes
        # Ex: "é" devient "e" + accent_aigu (deux entites separees)
        normalized = unicodedata.normalize("NFD", c)

        # Etape 2 : encodage ASCII en ignorant les octets non-ASCII (les accents)
        # encode("ascii", "ignore") supprime tout ce qui n'est pas ASCII pur
        # decode("ascii") reconvertit les bytes en chaine de caracteres Python
        ascii_str = normalized.encode("ascii", "ignore").decode("ascii")

        # Etape 3 : remplacement de tous les caracteres non-alphanumeriques par "_"
        # [^a-zA-Z0-9] est une regex qui matche tout sauf les lettres et chiffres
        # .lower() met en minuscules
        clean = re.sub(r"[^a-zA-Z0-9]", "_", ascii_str).lower()

        # Etape 4 : nettoyage des underscores multiples ou en debut/fin
        # r"_+" matche un ou plusieurs underscores consecutifs et les remplace par un seul
        # .strip("_") supprime les underscores en debut et fin de chaine
        clean = re.sub(r"_+", "_", clean).strip("_")

        new_cols.append(clean)  # Ajout du nom nettoye a la liste

    # toDF() renomme toutes les colonnes du DataFrame en une seule operation
    # L'ordre des noms dans new_cols doit correspondre a l'ordre des colonnes dans df
    return df.toDF(*new_cols)  # L'asterisque * deplie la liste en arguments separes


# --- Suppression des colonnes de metadonnees Bronze ---
# Ces colonnes ont ete ajoutees par le notebook 01 pour la traçabilite Bronze
# On les supprime ici car la couche Silver va creer ses propres metadonnees
META_COLS = [
    "_ingestion_timestamp",
    "_ingestion_date",
    "_source_dataset",
    "_source_url",
    "_source_description"
]

# On ne supprime que les colonnes qui existent reellement dans le DataFrame
# (securite en cas d'evolution du schema Bronze)
df_work = df_bronze.drop(*[c for c in META_COLS if c in df_bronze.columns])

# Application de la standardisation des noms de colonnes
df_work = standardize_column_names(df_work)

# --- Suppression des doublons ---
before_dedup = df_work.count()  # Comptage avant deduplication
df_work      = df_work.dropDuplicates()  # Supprime les lignes identiques sur toutes les colonnes
after_dedup  = df_work.count()  # Comptage apres deduplication

print(f"Doublons supprimes : {before_dedup - after_dedup:,}")

print("\nColonnes standardisees :")
for c in df_work.columns:
    print(f"  - {c}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Controles qualité des données (Data Quality)

# COMMAND ----------

def run_data_quality_checks(df, dataset_name: str) -> list:
    """
    Execute une serie de controles qualité sur le DataFrame.

    Controles appliqués :
      1. Taux de valeurs nulles par colonne
      2. Présence de doublons
      3. Valeurs négatives dans les colonnes numeriques

    Parametres :
        df           : DataFrame Spark a verifier
        dataset_name : nom du dataset pour l'affichage du rapport

    Retourne :
        liste de dictionnaires, chaque dictionnaire decrivant le resultat d'un check
    """

    checks     = []        # Accumulateur des resultats de tous les checks
    total_rows = df.count()  # Nombre total de lignes (reference pour le calcul des pourcentages)

    print(f"\nDATA QUALITY CHECKS - {dataset_name.upper()}")
    print(f"Total lignes : {total_rows:,}")
    print("-" * 60)

    # --- Check 1 : Valeurs nulles par colonne ---
    # Une colonne avec trop de valeurs nulles peut signaler un probleme de source ou d'ingestion
    print("\nCheck 1 : Valeurs nulles")

    for col_name in df.columns:

        # Comptage des lignes ou cette colonne est nulle
        null_count = df.filter(col(col_name).isNull()).count()

        # Calcul du pourcentage de nulls par rapport au total de lignes
        # Protection contre la division par zero si le DataFrame est vide
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0

        # Classification du statut selon le seuil de nulls
        if null_pct == 0:
            status = "PASS"     # Aucun null : parfait
        elif null_pct < 5:
            status = "WARNING"  # Quelques nulls : a surveiller
        else:
            status = "FAIL"     # Trop de nulls : probleme probable

        # Enregistrement du resultat du check
        checks.append({
            "check":      "null_check",
            "column":     col_name,
            "null_count": null_count,
            "null_pct":   round(null_pct, 2),
            "status":     status
        })

        # Affichage uniquement si des nulls sont detectes (evite de polluer les logs)
        if null_pct > 0:
            print(f"  [{status}] {col_name:<40} | {null_pct:5.1f}% nuls ({null_count:,})")

    # --- Check 2 : Detection des doublons ---
    # Des doublons peuvent fausser les agregations (sommes, moyennes) dans la couche Gold
    print("\nCheck 2 : Doublons")

    # dropDuplicates() retourne un DataFrame sans doublons
    # La difference de count() donne le nombre exact de doublons
    dup_count  = total_rows - df.dropDuplicates().count()
    dup_status = "PASS" if dup_count == 0 else "WARNING"

    checks.append({
        "check":     "duplicate_check",
        "column":    "ALL",
        "dup_count": dup_count,
        "status":    dup_status
    })
    print(f"  [{dup_status}] Doublons detectes : {dup_count:,}")

    # --- Check 3 : Valeurs negatives dans les colonnes numeriques ---
    # Une consommation negative n'a pas de sens physique : cela signale une erreur de donnee
    print("\nCheck 3 : Valeurs negatives (colonnes numeriques)")

    # Filtrage des colonnes de type numerique dans le schema Spark
    # f.dataType est le type Spark de chaque champ, on le compare aux types numeriques connus
    numeric_cols = [
        f.name for f in df.schema.fields
        if str(f.dataType) in ("DoubleType()", "FloatType()", "LongType()", "IntegerType()")
    ]

    for col_name in numeric_cols:

        # Comptage des lignes avec une valeur strictement negative pour cette colonne
        neg_count  = df.filter(col(col_name) < 0).count()
        neg_status = "PASS" if neg_count == 0 else "FAIL"

        checks.append({
            "check":     "negative_check",
            "column":    col_name,
            "neg_count": neg_count,
            "status":    neg_status
        })
        print(f"  [{neg_status}] {col_name:<40} | {neg_count:,} valeurs negatives")

    # Synthese du rapport
    fails = sum(1 for c in checks if c["status"] == "FAIL")
    warns = sum(1 for c in checks if c["status"] == "WARNING")
    print(f"\nResume : {len(checks)} checks | {fails} FAIL | {warns} WARNING")

    return checks


# Execution des controles sur le DataFrame en cours de traitement
quality_report = run_data_quality_checks(df_work, DATASET_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Détection dynamique des colonnes métier et cast des types

# COMMAND ----------

# --- Fonction de detection des colonnes par mots-cles ---
def detect_col(df, *keywords):
    """
    Detecte la premiere colonne du DataFrame dont le nom contient
    l'un des mots-cles fournis (recherche insensible a la casse).

    Parametres :
        df       : DataFrame Spark
        keywords : mots-cles a tester, par ordre de priorite

    Retourne :
        Nom de la colonne trouvee, ou None
    """
    for kw in keywords:
        match = next((c for c in df.columns if kw in c.lower()), None)
        if match:
            return match
    return None

# Assignation manuelle des colonnes metier d'apres le vrai schema Enedis
# Les noms reels ne correspondent pas aux mots-cles generiques de detect_col
# On assigne donc directement les noms de colonnes confirmes dans la table Bronze
conso_col  = "total_energie_soutiree_wh"  # Colonne de consommation electrique en Wh
annee_col  = None                          # Pas de colonne annee directe : sera extraite depuis horodate
region_col = "region"                      # Colonne du libelle de region
sites_col  = "nb_points_soutirage"         # Colonne du nombre de points de soutirage

print("Colonnes metier assignees :")
print(f"  Consommation : {conso_col}")
print(f"  Annee        : {annee_col}")
print(f"  Region       : {region_col}")
print(f"  Nb sites     : {sites_col}")

# --- Cast des types Spark ---
# Les colonnes lues depuis un CSV sont souvent de type StringType par defaut
# On les convertit dans le type metier attendu pour permettre les calculs

if conso_col:
    # cast(DoubleType()) convertit la colonne en nombre a virgule flottante
    # Necessaire pour les calculs de somme, moyenne, etc.
    df_work = df_work.withColumn(conso_col, col(conso_col).cast(DoubleType()))

if sites_col:
    # cast("long") convertit en Long (entier 64 bits) car le nombre de sites peut etre grand
    df_work = df_work.withColumn(sites_col, col(sites_col).cast("long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Application des UDFs Python

# COMMAND ----------

# --- UDF 1 : Categorisation de la consommation ---
# expr() permet d'appeler une fonction SQL enregistree (ici notre UDF Python)
# depuis du code PySpark, en passant le nom de la colonne comme argument
if conso_col:
    df_work = df_work.withColumn(
        "categorie_consommation",                         # Nom de la nouvelle colonne creee
        expr(f"categorize_consumption_udf({conso_col})")  # Appel de l'UDF enregistree
    )
    print(f"UDF appliquee : categorie_consommation calculee depuis '{conso_col}'")

# --- UDF 2 : Score carbone normalise ---
# Cette UDF necessite deux arguments : consommation et nombre de sites
if conso_col and sites_col:
    df_work = df_work.withColumn(
        "score_carbone_normalise",                                        # Nouvelle colonne de score
        expr(f"compute_carbon_score_udf({conso_col}, {sites_col})")       # Appel avec deux arguments
    )
    print(f"UDF appliquee : score_carbone_normalise calcule depuis '{conso_col}' et '{sites_col}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conformite RGPD - k-anonymisation
# MAGIC
# MAGIC Le RGPD interdit de publier des donnees permettant d'identifier indirectement
# MAGIC un individu. Dans le contexte Enedis, une ligne avec un tres faible nombre de sites
# MAGIC dans une petite commune pourrait permettre d'identifier le profil de consommation
# MAGIC d'un foyer specifique.
# MAGIC
# MAGIC On applique la regle de k-anonymisation : toute ligne avec moins de k sites
# MAGIC est supprimee car elle pourrait exposer des donnees individuelles.

# COMMAND ----------

# Seuil de k-anonymisation : une ligne doit representer au moins 5 sites differents
# pour etre conservee. En dessous, le risque de re-identification est trop eleve.
RGPD_MIN_SITES = 5

# Comptage avant le filtre pour mesurer l'impact
silver_pre_rgpd = df_work.count()

if sites_col:
    # On ne conserve que les lignes ou le nombre de sites est connu ET superieur au seuil
    # col(sites_col).isNotNull() : on exclut les lignes sans information sur le nombre de sites
    # col(sites_col) >= RGPD_MIN_SITES : on exclut les lignes avec trop peu de sites
    df_silver = df_work.filter(
        col(sites_col).isNotNull() & (col(sites_col) >= RGPD_MIN_SITES)
    )

    silver_post_rgpd = df_silver.count()
    lignes_supprimees = silver_pre_rgpd - silver_post_rgpd
    print(f"RGPD - Lignes supprimees (moins de {RGPD_MIN_SITES} sites) : {lignes_supprimees:,}")

else:
    # Si la colonne nb_sites n'est pas disponible, on ne peut pas appliquer le filtre
    # On log un avertissement mais on ne bloque pas le pipeline
    df_silver = df_work
    print("Avertissement : colonne 'nb_sites' non trouvee, filtre RGPD non applique.")

# --- Ajout des metadonnees Silver ---
# Meme principe que le notebook Bronze : on trace chaque etape de transformation
df_silver = (
    df_silver
    .withColumn("_silver_timestamp", current_timestamp())  # Horodatage de la transformation Silver
    .withColumn("_silver_date",      current_date())       # Date de la transformation
    .withColumn("_rgpd_min_sites",   lit(RGPD_MIN_SITES))  # Seuil RGPD applique (pour l'audit)
)

print(f"Lignes Silver apres nettoyage et RGPD : {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Écriture en Delta Lake Silver

# COMMAND ----------

# Nom de la table Silver dans le catalogue Databricks
# La convention silver_ permet d'identifier la couche de traitement
table_name = f"silver_{DATASET_NAME}"

# Construction de l'objet writer Spark
writer = (
    df_silver.write              # Acces a l'interface d'ecriture du DataFrame
             .format("delta")   # Format Delta Lake pour les transactions ACID et le versioning
             .mode("overwrite")                 # Remplace les donnees existantes si la table existe deja
             .option("overwriteSchema", "true") # Accepte les changements de schema
)

# Ajout du partitionnement si une colonne annee a ete detectee
# Partitionner par annee permet a Spark de ne lire que les partitions concernees
# lors des requetes filtrees sur l'annee (optimisation des performances)
if annee_col:
    writer = writer.partitionBy(annee_col)

# saveAsTable() enregistre la table dans le catalogue Databricks (compatible Serverless)
# contrairement a .save(chemin) qui necessite l'acces au DBFS root (desactive en Serverless)
writer.saveAsTable(table_name)

print(f"Donnees Silver ecrites avec succes -> table : {table_name}")

# Apercu des donnees Silver finales
df_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recapitulatif de la couche Silver
# MAGIC
# MAGIC | Etape | Action realisee |
# MAGIC |---|---|
# MAGIC | Nettoyage | Standardisation des noms de colonnes, suppression des doublons |
# MAGIC | Cast des types | Consommation en Double, annee en Integer, nb_sites en Long |
# MAGIC | Data Quality | Controles : nulls, doublons, valeurs negatives |
# MAGIC | UDF Scala | categorize_consumption_scala et compute_carbon_score_scala |
# MAGIC | RGPD | k-anonymisation : suppression des lignes avec moins de 5 sites |
# MAGIC | Stockage | Delta Lake Silver, partitionne par annee |
# MAGIC
# MAGIC Nous arrivons ainsi à la fin du step Silver. Passeons au notebook 03_Gold_Aggregations pour produire les KPIs metier.