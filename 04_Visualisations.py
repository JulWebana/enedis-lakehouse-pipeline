# Databricks notebook source

# MAGIC %md
# MAGIC # 04 - Visualisations des KPIs Enedis
# MAGIC
# MAGIC Ce notebook lit les tables Gold produites par le notebook 03
# MAGIC et genere des visualisations matplotlib pour chaque KPI.
# MAGIC
# MAGIC Visualisations produites :
# MAGIC - KPI 1 : Graphique en barres groupees - Consommation par region et par annee
# MAGIC - KPI 2 : Courbe avec marqueurs - Evolution nationale et variation YoY
# MAGIC - KPI 3 : Graphique en barres horizontales - Repartition par categorie
# MAGIC - KPI 4 : Graphique en barres horizontales - Classement des regions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration et imports

# COMMAND ----------

# --- Imports des bibliotheques de visualisation ---
import matplotlib.pyplot as plt        # Bibliotheque principale de visualisation Python
import matplotlib.ticker as mticker    # Formateurs d'axes (ex: afficher les grands nombres lisiblement)
import numpy as np                     # Calculs numeriques (positions des barres, tableaux)

# Configuration globale du style des graphiques
plt.rcParams["figure.figsize"]  = (14, 6)   # Taille par defaut de chaque figure (largeur x hauteur en pouces)
plt.rcParams["axes.spines.top"]    = False  # Suppression du bord superieur du graphique (style epure)
plt.rcParams["axes.spines.right"]  = False  # Suppression du bord droit
plt.rcParams["axes.grid"]          = True   # Grille activee par defaut pour faciliter la lecture
plt.rcParams["grid.alpha"]         = 0.3    # Grille semi-transparente pour ne pas surcharger
plt.rcParams["font.size"]          = 11     # Taille de police par defaut

# Palette de couleurs Enedis (bleu institutionnel + nuances)
COULEUR_2023 = "#003189"   # Bleu fonce Enedis
COULEUR_2024 = "#0070C0"   # Bleu moyen
COULEUR_2025 = "#00AEEF"   # Bleu clair

print("Configuration visualisations chargee")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chargement des tables Gold

# COMMAND ----------

# Lecture des 4 tables Gold depuis le catalogue Databricks
# On les convertit en Pandas pour les utiliser avec matplotlib
# (matplotlib ne travaille pas directement avec les DataFrames Spark)

df_region    = spark.read.table("kpi_conso_par_region").toPandas()
df_tendance  = spark.read.table("kpi_tendance_nationale").toPandas()
df_categorie = spark.read.table("kpi_categorie_distribution").toPandas()
df_top       = spark.read.table("kpi_top_regions").toPandas()

print(f"kpi_conso_par_region      : {len(df_region)} lignes")
print(f"kpi_tendance_nationale    : {len(df_tendance)} lignes")
print(f"kpi_categorie_distribution: {len(df_categorie)} lignes")
print(f"kpi_top_regions           : {len(df_top)} lignes")
print("Tables Gold chargees en Pandas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualisation KPI 1 - Consommation par region et par annee

# COMMAND ----------

# Preparation des donnees : on pivote le DataFrame pour avoir une colonne par annee
# pivot_table() cree un tableau croise : index = region, colonnes = annees, valeurs = conso_totale
df_pivot = df_region.pivot_table(
    index="region",           # Une ligne par region
    columns="annee",          # Une colonne par annee
    values="conso_totale_mwh" # Valeur a afficher
)

# Conversion des valeurs en milliards de Wh pour une meilleure lisibilite sur l'axe Y
df_pivot = df_pivot / 1_000_000_000

# Tri des regions par consommation totale decroissante pour un classement lisible
df_pivot["total"] = df_pivot.sum(axis=1)   # Somme des 3 annees pour chaque region
df_pivot = df_pivot.sort_values("total", ascending=False)  # Tri decroissant
df_pivot = df_pivot.drop(columns="total")  # Suppression de la colonne de tri

# --- Construction du graphique en barres groupees ---
annees     = df_pivot.columns.tolist()   # Liste des annees disponibles (ex: [2023, 2024, 2025])
n_regions  = len(df_pivot)               # Nombre de regions
n_annees   = len(annees)                 # Nombre d'annees

# Calcul des positions des barres sur l'axe X
# np.arange cree un tableau de positions entières (une par region)
x         = np.arange(n_regions)
largeur   = 0.25    # Largeur de chaque barre (3 barres cote a cote = 0.75 total)
couleurs  = [COULEUR_2023, COULEUR_2024, COULEUR_2025]

fig, ax = plt.subplots(figsize=(16, 7))  # Creation de la figure et de l'axe

for i, (annee, couleur) in enumerate(zip(annees, couleurs)):
    # Calcul du decalage horizontal pour chaque groupe d'annees
    # (i - n_annees/2 + 0.5) centre les barres autour de la position de la region
    decalage = (i - n_annees / 2 + 0.5) * largeur

    barres = ax.bar(
        x + decalage,               # Position horizontale de chaque barre
        df_pivot[annee],            # Hauteur des barres (consommation)
        width=largeur,              # Largeur de chaque barre
        color=couleur,              # Couleur selon l'annee
        label=str(annee),           # Legende
        alpha=0.85                  # Legere transparence pour un rendu plus doux
    )

# --- Mise en forme des axes ---
ax.set_xticks(x)                                          # Position des etiquettes sur l'axe X
ax.set_xticklabels(df_pivot.index, rotation=30, ha="right", fontsize=10)  # Noms des regions inclines
ax.set_ylabel("Consommation totale (milliards de Wh)", fontsize=11)
ax.set_xlabel("")
ax.set_title(
    "KPI 1 - Consommation electrique par region et par annee\n(perimetre : consommateurs <= 36 kVA)",
    fontsize=13, fontweight="bold", pad=15
)
ax.legend(title="Annee", fontsize=10)

# Formateur de l'axe Y : affiche les nombres avec 1 decimale (ex: 14.4)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f"))

plt.tight_layout()   # Ajustement automatique des marges pour eviter les coupures
plt.show()           # Affichage du graphique dans Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualisation KPI 2 - Evolution nationale et variation YoY

# COMMAND ----------

# Tri chronologique (necessaire pour que la courbe soit dans le bon sens)
df_tendance = df_tendance.sort_values("annee")

# Conversion de la consommation en milliards de Wh
df_tendance["conso_mds"] = df_tendance["conso_nationale_mwh"] / 1_000_000_000

fig, ax1 = plt.subplots(figsize=(12, 6))

# --- Courbe principale : consommation nationale ---
# ax1 est l'axe gauche (consommation en milliards de Wh)
ax1.plot(
    df_tendance["annee"],    # Axe X : annees
    df_tendance["conso_mds"],# Axe Y : consommation
    color=COULEUR_2023,      # Couleur de la courbe
    linewidth=2.5,           # Epaisseur de la ligne
    marker="o",              # Cercle a chaque point de donnee
    markersize=10,           # Taille des cercles
    label="Consommation nationale (Mds Wh)"
)

# Affichage des valeurs au-dessus de chaque point
for _, row in df_tendance.iterrows():
    ax1.annotate(
        f"{row['conso_mds']:.1f} Mds Wh",   # Texte a afficher
        xy=(row["annee"], row["conso_mds"]),  # Position du point
        xytext=(0, 12),                       # Decalage vertical du texte
        textcoords="offset points",           # Unite du decalage
        ha="center", fontsize=10, color=COULEUR_2023, fontweight="bold"
    )

ax1.set_ylabel("Consommation nationale (milliards de Wh)", fontsize=11, color=COULEUR_2023)
ax1.tick_params(axis="y", labelcolor=COULEUR_2023)
ax1.set_ylim(0, df_tendance["conso_mds"].max() * 1.3)  # Marge au-dessus pour les annotations

# --- Axe secondaire : variation YoY en pourcentage ---
# ax2 est l'axe droit (variation en %)
ax2 = ax1.twinx()   # twinx() cree un second axe Y partageant le meme axe X

# On filtre les lignes avec une variation disponible (pas null)
df_yoy = df_tendance.dropna(subset=["variation_yoy_pct"])

# Couleur conditionnelle : vert si hausse, rouge si baisse
couleurs_yoy = ["#2ECC71" if v > 0 else "#E74C3C" for v in df_yoy["variation_yoy_pct"]]

ax2.bar(
    df_yoy["annee"],             # Position sur l'axe X
    df_yoy["variation_yoy_pct"], # Hauteur des barres (%)
    width=0.3,                   # Barres etroites pour ne pas masquer la courbe
    color=couleurs_yoy,          # Vert ou rouge selon le signe
    alpha=0.4,                   # Transparente pour ne pas masquer la courbe principale
    label="Variation YoY (%)"
)

# Affichage des valeurs de variation sur chaque barre
for _, row in df_yoy.iterrows():
    signe = "+" if row["variation_yoy_pct"] > 0 else ""
    ax2.annotate(
        f"{signe}{row['variation_yoy_pct']:.1f}%",
        xy=(row["annee"], row["variation_yoy_pct"]),
        xytext=(0, 5 if row["variation_yoy_pct"] > 0 else -15),
        textcoords="offset points",
        ha="center", fontsize=10, fontweight="bold",
        color="#2ECC71" if row["variation_yoy_pct"] > 0 else "#E74C3C"
    )

ax2.set_ylabel("Variation annuelle (%)", fontsize=11, color="#555555")
ax2.tick_params(axis="y", labelcolor="#555555")
ax2.axhline(y=0, color="#555555", linewidth=0.8, linestyle="--")  # Ligne de reference a 0%

# Legendes combinees des deux axes
lignes1, labels1 = ax1.get_legend_handles_labels()
lignes2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lignes1 + lignes2, labels1 + labels2, loc="upper left", fontsize=10)

ax1.set_xticks(df_tendance["annee"])
ax1.set_title(
    "KPI 2 - Evolution de la consommation nationale et variation Year-over-Year",
    fontsize=13, fontweight="bold", pad=15
)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualisation KPI 3 - Repartition par categorie de consommation

# COMMAND ----------

# On exclut la categorie INCONNU (valeurs nulles) pour ne pas fausser le graphique
df_cat_filtre = df_categorie[df_categorie["categorie_consommation"] != "INCONNU"].copy()

# Tri par consommation decroissante
df_cat_filtre = df_cat_filtre.sort_values("conso_totale_mwh", ascending=True)

# Conversion en milliards de Wh
df_cat_filtre["conso_mds"] = df_cat_filtre["conso_totale_mwh"] / 1_000_000_000

# Palette de couleurs par categorie
palette = {
    "TRES ELEVEE": COULEUR_2023,
    "ELEVEE":      COULEUR_2024,
    "MODEREE":     COULEUR_2025
}
couleurs_cat = [palette.get(c, "#AAAAAA") for c in df_cat_filtre["categorie_consommation"]]

fig, (ax_bar, ax_pie) = plt.subplots(1, 2, figsize=(16, 6))

# --- Graphique gauche : barres horizontales ---
barres = ax_bar.barh(
    df_cat_filtre["categorie_consommation"],  # Axe Y : categories
    df_cat_filtre["conso_mds"],               # Axe X : consommation
    color=couleurs_cat,
    alpha=0.85,
    height=0.5
)

# Affichage des valeurs et pourcentages en bout de barre
for barre, (_, row) in zip(barres, df_cat_filtre.iterrows()):
    ax_bar.text(
        barre.get_width() + 0.5,     # Position X : juste apres la barre
        barre.get_y() + barre.get_height() / 2,  # Position Y : centre de la barre
        f"{row['conso_mds']:.1f} Mds Wh  ({row['pct_volume_total']:.2f}%)",
        va="center", fontsize=10
    )

ax_bar.set_xlabel("Consommation totale (milliards de Wh)", fontsize=11)
ax_bar.set_title("Consommation par categorie", fontsize=12, fontweight="bold")
ax_bar.set_xlim(0, df_cat_filtre["conso_mds"].max() * 1.5)

# --- Graphique droit : camembert ---
ax_pie.pie(
    df_cat_filtre["pct_volume_total"],              # Valeurs (pourcentages)
    labels=df_cat_filtre["categorie_consommation"], # Etiquettes
    colors=couleurs_cat,                             # Couleurs
    autopct="%1.2f%%",                               # Affichage du % sur chaque part
    startangle=90,                                   # Rotation de depart
    pctdistance=0.6                                  # Distance du texte % par rapport au centre
)
ax_pie.set_title("Part du volume total", fontsize=12, fontweight="bold")

fig.suptitle(
    "KPI 3 - Repartition des volumes par categorie de consommation",
    fontsize=13, fontweight="bold", y=1.02
)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualisation KPI 4 - Classement des regions

# COMMAND ----------

# Tri par consommation croissante pour que la region n°1 soit en haut du graphique
df_top_sorted = df_top.sort_values("conso_totale_mwh", ascending=True)

# Conversion en milliards de Wh
df_top_sorted["conso_mds"] = df_top_sorted["conso_totale_mwh"] / 1_000_000_000

# Couleur variable selon le classement : les 3 premieres en bleu fonce, les autres en bleu clair
couleurs_top = [
    COULEUR_2023 if rang <= 3 else COULEUR_2025
    for rang in df_top_sorted["classement"]
]

fig, ax = plt.subplots(figsize=(14, 8))

barres = ax.barh(
    df_top_sorted["region"],      # Axe Y : noms des regions
    df_top_sorted["conso_mds"],   # Axe X : consommation en Mds Wh
    color=couleurs_top,
    alpha=0.85,
    height=0.6
)

# Affichage des valeurs et du rang en bout de barre
for barre, (_, row) in zip(barres, df_top_sorted.iterrows()):
    ax.text(
        barre.get_width() + 0.1,
        barre.get_y() + barre.get_height() / 2,
        f"{row['conso_mds']:.2f} Mds Wh  (rang {int(row['classement'])})",
        va="center", fontsize=10
    )

ax.set_xlabel("Consommation totale 2025 (milliards de Wh)", fontsize=11)
ax.set_title(
    "KPI 4 - Classement des regions les plus consommatrices (2025)\n(perimetre : consommateurs <= 36 kVA)",
    fontsize=13, fontweight="bold", pad=15
)
ax.set_xlim(0, df_top_sorted["conso_mds"].max() * 1.4)

# Ligne verticale de reference : consommation moyenne
moyenne = df_top_sorted["conso_mds"].mean()
ax.axvline(
    x=moyenne,
    color="#E74C3C",
    linewidth=1.5,
    linestyle="--",
    label=f"Moyenne : {moyenne:.2f} Mds Wh"
)
ax.legend(fontsize=10)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recapitulatif des visualisations
# MAGIC
# MAGIC Les 4 graphiques ci-dessus illustrent les KPIs calcules dans le notebook 03 Gold :
# MAGIC
# MAGIC - **KPI 1** : barres groupees par region et par annee, triees par consommation totale decroissante
# MAGIC - **KPI 2** : courbe de tendance nationale avec barres de variation YoY (vert = hausse, rouge = baisse)
# MAGIC - **KPI 3** : barres horizontales + camembert de repartition par categorie de consommation
# MAGIC - **KPI 4** : barres horizontales classees, avec ligne de reference sur la moyenne nationale
# MAGIC
# MAGIC Ces visualisations sont directement exploitables pour un reporting metier ou un dashboard Enedis.
