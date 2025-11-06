import os

# --- 1. CHEMINS ET DOSSIERS ---

# Chemin du dossier principal où sont stockés les fichiers Excel bruts
# ATTENTION : Ce chemin doit être adapté à votre environnement
DOSSIER_PATH = r"C:\Users\OCELOU\Desktop\UF_DSE_DRIVE\UF_datasets\POWERQUERY"

# Chemin du dossier de sortie pour les fichiers nettoyés et les listes de doublons
DOSSIER_SORTIE = os.path.join(DOSSIER_PATH, 'sortie_nettoyage')

# --- 2. CONSTANTES DE FILTRAGE ET NOMMAGE ---

# Constantes pour le filtrage des noms de fichiers
NOM_FILTRE_2023 = "_UF2023_"
NOM_FILTRE_2024 = "_UF2024_"
NOM_FILTRE_2025 = "_UF2025_" 

# Noms des fichiers de sortie
FICHIER_SORTIE_NETTOYEE = '_UFALLTIME__KEYED.xlsx'
FICHIER_DOUBLONS_ETUDIANTS = 'liste_doublons_etudiants.xlsx'
FICHIER_DOUBLONS_INSCRIPTIONS = 'liste_doublons_inscriptions.xlsx'

# --- 3. COLONNES ATTENDUES ET STRUCTURE ---

# Colonnes attendues (utilisées pour l'ordre et le filtrage à l'exportation)
COLONNES_ATTENDUES = [
    # 1. IDENTIFIANTS UNIQUES ET BASE
    'code_etudiant',                        # Identifiant unique de l'étudiant (Hash tronqué)
    'code_inscription',                     # Identifiant unique de l'inscription (Hash)
    'numero_inscription',                   # Numéro d'inscription officiel
    'annee_universitaire',                  # Année de l'inscription
    
    # 2. INFORMATIONS INSTITUTIONNELLES (NOUVELLE POSITION)
    'institution_id', 'institution_nom', 'institution_type', # <- Mis à jour
    
    # 3. INFORMATIONS D'INSCRIPTION ET DE FORMATION
    'composante', 'domaine', 'mention', 'parcours', 'id_Parcours',
    'formation', 'formation_master', 'niveau', 'semestre',
    
    # --- Nouvelles colonnes binaires de semestre (S01 à S16) ---
    'S01', 'S02', 'S03', 'S04', 'S05', 'S06', 'S07', 'S08', 
    'S09', 'S10', 'S11', 'S12', 'S13', 'S14', 'S15', 'S16',
    
    # 4. INFORMATIONS PERSONNELLES ET CIVILES
    'nom', 'prenoms', 'sexe', 
    'naissance_date', 'naissance_annee', 'naissance_mois', 'naissance_jour', 'naissance_lieu', 
    'cin', 'cin_date', 'cin_lieu', 'nationalite',
    
    # 5. INFORMATIONS BACCALAURÉAT
    'bacc_annee', 'bacc_numero', 'bacc_serie', 'bacc_serie_technique', 
    'bacc_centre', 'bacc_mention',
    
    # 6. CONTACTS
    'telephone', 'mail'
    
    # COLONNES OPTIONNELLES/COMMENTÉES
    #'redoublement', 'boursier','taux_bourse', 'adresse', 'pere_nom', 'pere_profession', 'mere_nom', 'mere_profession'
]