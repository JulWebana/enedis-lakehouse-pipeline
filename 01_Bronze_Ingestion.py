# Databricks notebook source

# MAGIC %md
# MAGIC # 01 - Bronze Layer : Ingestion des donnees Open Data Enedis
# MAGIC
# MAGIC Architecture Lakehouse : Bronze (Raw) > Silver (Cleaned) > Gold (Business)
# MAGIC
# MAGIC Ce notebook telecharge les donnees brutes depuis l'API Open Data Enedis
# MAGIC et les stocke en tables Delta Lake managees sans aucune transformation.
# MAGIC
# MAGIC Principe de la couche Bronze : on stocke tout, tel quel, avec les metadonnees d'ingestion.
# MAGIC On ne modifie jamais les donnees brutes a ce stade.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration generale

# COMMAND ----------

# --- Imports des bibliotheques necessaires ---

import requests          # Bibliotheque HTTP pour appeler des APIs et telecharger des fichiers
import pandas as pd      # Bibliotheque de manipulation de donnees en memoire (DataFrames Python)
from io import StringIO  # Permet de traiter une chaine de caracteres comme un fichier (utile pour lire du CSV depuis une reponse HTTP)

# Imports PySpark : fonctions SQL disponibles dans Spark
from pyspark.sql.functions import (
    current_timestamp,  # Fonction Spark qui retourne l'horodatage exact au moment de l'execution
    current_date,       # Fonction Spark qui retourne la date du jour au moment de l'execution
    lit                 # Fonction Spark qui cree une colonne avec une valeur constante (literale)
)

from datetime import datetime  # Module Python standard pour manipuler les dates et heures

# COMMAND ----------

# --- Definition des datasets a ingerer depuis l'API Open Data Enedis ---
# Chaque entree est un dictionnaire avec l'URL de telechargement et une description lisible

DATASETS = {
    "conso_inf36_region": {
        # URL de l'API Enedis pour le dataset de consommation residentielle inferieure a 36 kVA par region
        # Le format CSV est demande via le chemin /exports/csv
        "url": "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/conso-inf36-region/exports/csv",
        "description": "Consommation residentielle inferieure a 36 kVA par region (annuelle)"
    },
    "conso_sup36_region": {
        # Meme logique pour la consommation professionnelle superieure a 36 kVA
        "url": "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/conso-sup36-region/exports/csv",
        "description": "Consommation professionnelle superieure a 36 kVA par region (annuelle)"
    }
}

# Affichage de confirmation dans les logs du notebook
print("Configuration chargee")
print(f"Datasets a ingerer : {list(DATASETS.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fonction d'ingestion generique

# COMMAND ----------

def ingest_dataset(dataset_name: str, dataset_config: dict) -> dict:
    """
    Telecharge un dataset depuis l'API Open Data Enedis,
    ajoute des metadonnees techniques d'ingestion,
    et persiste le tout en table Delta Lake managee dans la couche Bronze.

    Note sur le stockage : on utilise saveAsTable() au lieu d'un chemin DBFS.
    saveAsTable() enregistre la table dans le catalogue Databricks (Unity Catalog),
    ce qui est la methode recommandee en mode Serverless. La table est ensuite
    accessible par son nom (ex: bronze_conso_inf36_region) depuis n'importe quel notebook.

    Parametres :
        dataset_name   : identifiant textuel du dataset (ex: "conso_inf36_region")
        dataset_config : dictionnaire contenant 'url' et 'description' du dataset

    Retourne :
        dict contenant les metadonnees de l'ingestion (statut, nb lignes, duree...)
    """

    # On enregistre l'heure de debut pour mesurer la duree totale de l'ingestion
    ingestion_start = datetime.now()

    print(f"Ingestion en cours : {dataset_name}")
    print(f"  Source URL : {dataset_config['url']}")

    # --- Etape 1 : Telechargement des donnees depuis l'API ---

    # Parametres de la requete HTTP GET envoyee a l'API Enedis
    params = {
        "limit": 15000,   # 15000 lignes : couvre les 12 regions metropolitaines sur 3 ans
        "delimiter": ";"  # Le CSV utilise le point-virgule comme separateur (standard francais)
    }

    # Envoi de la requete HTTP GET vers l'URL de l'API
    # timeout=120 signifie qu'on attend au maximum 120 secondes avant d'abandonner
    response = requests.get(
        dataset_config["url"],
        params=params,
        timeout=120
    )

    # raise_for_status() leve une exception automatiquement si le code HTTP est une erreur (4xx, 5xx)
    # Cela interrompt la fonction proprement si l'API ne repond pas correctement
    response.raise_for_status()

    # --- Etape 2 : Conversion du contenu CSV en DataFrame Spark ---

    # response.text contient le contenu brut de la reponse (le CSV sous forme de chaine de caracteres)
    # StringIO transforme cette chaine en "faux fichier" que pandas peut lire directement
    # pd.read_csv lit ce faux fichier CSV avec le separateur point-virgule
    pdf = pd.read_csv(StringIO(response.text), sep=";")

    print(f"  Lignes recues : {len(pdf):,}")
    print(f"  Colonnes      : {list(pdf.columns)}")

    # spark.createDataFrame() convertit le DataFrame Pandas (execution locale)
    # en DataFrame Spark (distribue sur le cluster), ce qui permet de traiter
    # des volumes bien plus importants et d'ecrire en Delta Lake
    sdf = spark.createDataFrame(pdf)

    # --- Etape 3 : Ajout des metadonnees d'ingestion ---
    # C'est le pattern standard de la couche Bronze : on enrichit chaque ligne
    # avec des informations techniques sur l'origine et le moment de l'ingestion.
    # Ces metadonnees permettront de tracer la lineage de la donnee (d'ou elle vient, quand elle est arrivee).

    sdf = (
        sdf
        # Colonne horodatage exact (date + heure) au moment de l'ingestion
        .withColumn("_ingestion_timestamp", current_timestamp())

        # Colonne date (sans heure) utile pour partitionner les donnees par jour
        .withColumn("_ingestion_date", current_date())

        # Colonne avec le nom du dataset source (valeur constante pour toutes les lignes)
        # lit() cree une colonne avec la meme valeur pour chaque ligne
        .withColumn("_source_dataset", lit(dataset_name))

        # Colonne avec l'URL exacte utilisee pour le telechargement (traçabilite complete)
        .withColumn("_source_url", lit(dataset_config["url"]))

        # Colonne avec la description lisible du dataset
        .withColumn("_source_description", lit(dataset_config["description"]))
    )

    # --- Etape 4 : Ecriture en table Delta Lake managee (couche Bronze) ---

    # Nom de la table dans le catalogue Databricks
    # La convention de nommage bronze_ permet d'identifier facilement la couche
    table_name = f"bronze_{dataset_name}"

    (
        sdf.write                              # Acces a l'interface d'ecriture de Spark
           .format("delta")                   # Format Delta Lake : ajoute les transactions ACID, le versioning
           .mode("overwrite")                 # Si la table existe deja, on la remplace completement
           .option("overwriteSchema", "true") # En cas de changement de schema (nouvelles colonnes), on l'accepte
           .saveAsTable(table_name)           # Enregistrement comme table managee dans le catalogue Databricks
                                              # (compatible Serverless, contrairement aux chemins DBFS)
    )

    # Calcul de la duree en secondes depuis le debut de la fonction
    duration_s = (datetime.now() - ingestion_start).seconds

    print(f"  Succes en {duration_s}s  ->  table : {table_name}")

    # Retour d'un dictionnaire de metadonnees pour alimenter le rapport d'ingestion
    return {
        "dataset":          dataset_name,   # Nom du dataset
        "table":            table_name,     # Nom de la table Delta cree dans le catalogue
        "rows":             len(pdf),       # Nombre de lignes ingereees
        "columns":          len(pdf.columns),  # Nombre de colonnes
        "duration_seconds": duration_s,     # Duree de l'ingestion en secondes
        "status":           "SUCCESS",      # Statut de l'ingestion
        "timestamp":        ingestion_start.isoformat()  # Horodatage ISO 8601 du debut
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lancement de l'ingestion

# COMMAND ----------

# Liste qui va accumuler les metadonnees de chaque ingestion (succes ou echec)
# Cela permet de generer un rapport consolide a la fin
ingestion_log = []

# Boucle sur chaque dataset defini dans le dictionnaire DATASETS
for ds_name, ds_config in DATASETS.items():
    try:
        # Appel de la fonction d'ingestion pour ce dataset
        meta = ingest_dataset(ds_name, ds_config)

        # Ajout des metadonnees de succes dans la liste de logs
        ingestion_log.append(meta)

    except Exception as exc:
        # En cas d'erreur (reseau, API indisponible, donnees malformees...),
        # on ne fait pas planter tout le pipeline : on logue l'echec et on passe au suivant
        print(f"Erreur pour {ds_name} : {exc}")

        # Ajout des metadonnees d'echec dans la liste de logs
        ingestion_log.append({
            "dataset":   ds_name,
            "status":    "FAILED",
            "error":     str(exc),           # Message d'erreur converti en chaine de caracteres
            "timestamp": datetime.now().isoformat()
        })

# --- Affichage du rapport d'ingestion consolide ---
print("\n" + "="*60)
print("RAPPORT D'INGESTION - COUCHE BRONZE")
print("="*60)

for log in ingestion_log:
    # Recuperation du nombre de lignes avec valeur par defaut "N/A" si l'ingestion a echoue
    rows = f"{log.get('rows', 'N/A'):,}" if isinstance(log.get('rows'), int) else "N/A"
    secs = log.get('duration_seconds', '-')
    print(f"  [{log['status']}]  {log['dataset']:<35} | {rows:>10} lignes | {secs}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification - Apercu des donnees Bronze

# COMMAND ----------

# On relit chaque table depuis le catalogue pour confirmer que les donnees ont bien ete persistees
for ds_name in DATASETS.keys():

    # Nom de la table a relire : meme convention bronze_ que lors de l'ecriture
    table_name = f"bronze_{ds_name}"

    try:
        # spark.read.table() lit une table enregistree dans le catalogue Databricks
        # C'est l'equivalent de spark.read.format("delta").load(chemin) mais pour les tables managees
        df = spark.read.table(table_name)

        print(f"\nTable : {table_name}")
        print(f"  Lignes   : {df.count():,}")  # count() declenche une action Spark (parcourt toutes les partitions)
        print(f"  Colonnes : {df.columns}")

        # display() est une fonction Databricks qui affiche le DataFrame sous forme de tableau HTML interactif
        # limit(5) recupere uniquement les 5 premieres lignes pour l'apercu (evite de tout charger)
        df.limit(5).display()

    except Exception as exc:
        # Si la lecture echoue (table inexistante, ingestion ratee precedemment...)
        print(f"Impossible de lire {table_name} : {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recapitulatif de la couche Bronze
# MAGIC
# MAGIC | Zone   | Format      | Transformation | Stockage          | Metadonnees ajoutees |
# MAGIC |--------|-------------|----------------|-------------------|----------------------|
# MAGIC | Bronze | Delta Lake  | Aucune (raw)   | Table managee     | _ingestion_timestamp, _ingestion_date, _source_dataset, _source_url |
# MAGIC
# MAGIC Tables creees : bronze_conso_inf36_region, bronze_conso_sup36_region
# MAGIC
# MAGIC Passer au notebook 02_Silver_Transformation pour nettoyer et enrichir ces donnees.
