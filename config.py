import os

# ==================================================
# 1. CHEMINS ET DOSSIERS
# ==================================================

# Dossier principal où sont stockés les fichiers Excel bruts
DOSSIER_PATH = r"C:\Users\OCELOU\Desktop\UF_DSE_DRIVE\UF_datasets\POWERQUERY"

# Dossier de sortie pour les fichiers nettoyés et intermédiaires
DOSSIER_SORTIE = os.path.join(DOSSIER_PATH, "sortie_nettoyage")

# ==================================================
# 2. CONSTANTES DE FILTRAGE ET NOMMAGE DES FICHIERS
# ==================================================

# Filtres dans le nom des fichiers d'entrée (bruts)
NOM_FILTRE_2023 = "_UF2023_"
NOM_FILTRE_2024 = "_UF2024_"
NOM_FILTRE_2025 = "_UF2025_"

# Noms des fichiers de sortie
#  - FICHIER_SORTIE_NETTOYEE : export KEYED (colonnes du pipeline)
#  - FICHIER_SORTIE_RENOMMEE : export final (colonnes renommées pour la base)
FICHIER_SORTIE_NETTOYEE = "_UFALLTIME__KEYED.xlsx"
FICHIER_SORTIE_RENOMMEE = "_UFALLTIME_DATAS.xlsx"

# Fichiers pour les listes de doublons (si utilisés par ton pipeline)
FICHIER_DOUBLONS_ETUDIANTS = "liste_doublons_etudiants.xlsx"
FICHIER_DOUBLONS_INSCRIPTIONS = "liste_doublons_inscriptions.xlsx"

# ==================================================
# 3. COLONNES ATTENDUES (ORDRE DE RÉFÉRENCE)
# ==================================================

COLONNES_ATTENDUES = [
    # 1. IDENTIFIANTS UNIQUES ET BASE
    "code_etudiant",          # Identifiant unique de l'étudiant (Hash tronqué)
    "code_inscription",       # Identifiant unique de l'inscription (Hash semestriel)
    "numero_inscription",     # Numéro d'inscription officiel
    "annee_universitaire",    # Année de l'inscription

    # 2. INFORMATIONS INSTITUTIONNELLES
    "institution_id",
    "institution_nom",
    "institution_type",

    # 3. INFORMATIONS D'INSCRIPTION ET DE FORMATION
    "composante",
    "domaine",
    "mention",
    "parcours",
    "id_Parcours",
    "formation",
    "type_formation",
    "formation_master",
    "niveau",                 # Niveau (ex : L1, M1), si encore utilisé en amont
    "semestre",               # Semestre (ex : S01)
    "semestre_id",            # Numéro/ID du semestre (ex : 1, 2, 3...)

    # 4. INFORMATIONS PERSONNELLES ET CIVILES
    "nom",
    "prenoms",
    "sexe",
    "naissance_date",
    "naissance_annee",
    "naissance_mois",
    "naissance_jour",
    "naissance_lieu",
    "cin",
    "cin_date",
    "cin_lieu",
    "nationalite",

    # 5. INFORMATIONS BACCALAURÉAT
    "bacc_annee",
    "bacc_numero",
    "bacc_serie",
    "bacc_serie_technique",
    "bacc_centre",
    "bacc_mention",

    # 6. CONTACTS
    "telephone",
    "mail",

    # Optionnel : à décommenter si ces colonnes existent dans tes données
    # "redoublement",
    # "boursier",
    # "taux_bourse",
    # "adresse",
    # "pere_nom",
    # "pere_profession",
    # "mere_nom",
    # "mere_profession",
]

# ==================================================
# 4. RÈGLES DE RENOMMAGE DES COLONNES
#    (ANCIEN_NOM -> NOUVEAU_NOM POUR LA BASE)
# ==================================================

COLONNES_RENOMMAGE = {
    # 1. IDENTIFIANTS UNIQUES ET BASE
    "code_etudiant": "Etudiant_id",
    "code_inscription": "Inscription_code",
    "numero_inscription": "Etudiant_numero_inscription",
    "annee_universitaire": "AnneeUniversitaire_annee",

    # 2. INFORMATIONS INSTITUTIONNELLES
    "institution_id": "Institution_code",
    "institution_nom": "Institution_nom",
    "institution_type": "Institution_type",

    # 3. INFORMATIONS D'INSCRIPTION ET DE FORMATION
    "composante": "Composante_code",
    "domaine": "Domaine_code",
    "mention": "Mention_abbreviation",
    "parcours": "Parcours_abbreviation",
    "id_Parcours": "Parcours_code",
    "formation": "ModeInscription_label",
    # "type_formation": (conservé tel quel si besoin, sinon ajouter ici)
    # "formation_master": (idem)
    "niveau": "Niveau_code",
    # "semestre" : conservé tel quel (unité de suivi S01, S02...)
    "semestre_id": "Semestre_numero",

    # 4. INFORMATIONS PERSONNELLES ET CIVILES
    "nom": "Etudiant_nom",
    "prenoms": "Etudiant_prenoms",
    "sexe": "Etudiant_sexe",
    "naissance_date": "Etudiant_naissance_date",
    "naissance_lieu": "Etudiant_naissance_lieu",
    # "naissance_annee", "naissance_mois", "naissance_jour" : conservés tels quels
    "cin": "Etudiant_cin",
    "cin_date": "Etudiant_cin_date",
    "cin_lieu": "Etudiant_cin_lieu",
    "nationalite": "Etudiant_nationalite",

    # 5. INFORMATIONS BACCALAURÉAT
    "bacc_annee": "Etudiant_bacc_annee",
    "bacc_numero": "Etudiant_bacc_numero",
    "bacc_serie": "Etudiant_bacc_serie",
    "bacc_centre": "Etudiant_bacc_centre",
    "bacc_mention": "Etudiant_bacc_mention",
    # "bacc_serie_technique" : conservé tel quel pour l’instant

    # 6. CONTACTS
    "telephone": "Etudiant_telephone",
    "mail": "Etudiant_mail",
}
