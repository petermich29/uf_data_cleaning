# config.py

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

# Noms des fichiers de sortie
FICHIER_SORTIE_NETTOYEE = '_UFALLTIME__KEYED.xlsx'
FICHIER_DOUBLONS_ETUDIANTS = 'liste_doublons_etudiants.xlsx'
FICHIER_DOUBLONS_INSCRIPTIONS = 'liste_doublons_inscriptions.xlsx'

# --- 3. PARAMÈTRES D'ALGORITHMES ---
# Mode de hachage des identifiants (pour utilisation future) : 
# - 'SHA-256' pour la robustesse et l'anonymisation (par défaut)
# - 'MD5' pour un identifiant plus court (32 caractères)
HASH_ALGORITHM = 'SHA-256' 

# --- 4. COLONNES ATTENDUES ET STRUCTURE ---

# Colonnes attendues (utilisées pour l'ordre et le filtrage à l'exportation)
COLONNES_ATTENDUES = [
    'code_etudiant', 'code_inscription',   # Identifiant unique de l'étudiant (Hash tronqué à 32)
    'numero_inscription', 'composante', 'domaine', 'mention', 'parcours', 'id_Parcours',
    'formation', 'formation_master', 'niveau', 'annee_universitaire', 'nom', 'prenoms',
    # Informations personnelles
    'sexe', 'naissance_date', 'naissance_annee', 'naissance_mois', 'naissance_jour', 'naissance_lieu', 
    'cin', 'cin_date', 'cin_lieu', 'nationalite', 
    'bacc_annee', 'bacc_serie_technique', 'bacc_serie',
    'bacc_numero', 'bacc_centre', 'bacc_mention', 'semestre', 'telephone', 'mail'
    #'redoublement', 'boursier','taux_bourse', 'adresse', 'pere_nom', 'pere_profession', 'mere_nom', 'mere_profession'
]