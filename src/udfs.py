def categorize_consumption(consumption):
    if consumption is None:
        return "INCONNU"
    elif consumption < 1000.0:
        return "TRES FAIBLE"
    elif consumption < 5000.0:
        return "FAIBLE"
    elif consumption < 20000.0:
        return "MODEREE"
    elif consumption < 100000.0:
        return "ELEVEE"
    else:
        return "TRES ELEVEE"

def compute_carbon_score(consumption_wh, nb_sites):
    if consumption_wh is None or nb_sites is None or nb_sites == 0:
        return None
    FACTEUR_CARBONE = 0.0571
    score = (consumption_wh / 1000) * FACTEUR_CARBONE / nb_sites
    return round(score, 2)
