# inscription_code_manager.py

import pandas as pd
import hashlib
import numpy as np

# --- (Fonction generer_hash dupliquée pour l'autonomie du fichier) ---

def generer_hash(chaine_a_hasher: str, algorithme: str, longueur: int = 32) -> str:
    """Génère un hachage unique pour une chaîne de caractères et le tronque (par défaut 32 caractères)."""
    if pd.isna(chaine_a_hasher) or chaine_a_hasher == '':
        return pd.NA
    
    chaine_normalisee = str(chaine_a_hasher).strip().upper()
    
    try:
        if algorithme == 'SHA-256':
            hashed_value = hashlib.sha256(chaine_normalisee.encode('utf-8')).hexdigest()
        elif algorithme == 'MD5':
            hashed_value = hashlib.md5(chaine_normalisee.encode('utf-8')).hexdigest()
        else:
            hashed_value = hashlib.sha256(chaine_normalisee.encode('utf-8')).hexdigest()
            
        return hashed_value[:longueur]
        
    except Exception:
        return pd.NA

# --- FONCTION PRINCIPALE DE GESTION DES CODES D'INSCRIPTION ---

def gerer_code_inscription_et_supprimer_doublons(df: pd.DataFrame, hash_algorithm: str) -> pd.DataFrame:
    """
    Crée un identifiant unique (code_inscription) basé sur l'identité et les variables d'inscription
    et supprime les doublons basés sur cet identifiant composite.
    """
    print("\n==================================================================")
    print("🚀 DÉMARRAGE : GESTION DES CODES D'INSCRIPTION ET SUPPRESSION DES DOUBLONS")
    print(f"Total des lignes d'inscription initial : {len(df)}")
    print("==================================================================")
    
    # 1. Préparation de la Clé d'Inscription
    
    # S'assurer que les colonnes nécessaires sont présentes
    colonnes_requises = ['code_etudiant', 'annee_universitaire', 'niveau', 'id_Parcours']
    for col in colonnes_requises:
        if col not in df.columns:
            print(f"❌ Erreur : Colonne '{col}' manquante. Le processus s'arrête.")
            return df # Retourne le DataFrame non modifié

    # Création de la Clé d'Inscription basée sur la contrainte demandée
    df['cle_inscription_unique'] = (
        df['code_etudiant'].astype(str).fillna('NA_ID') + 
        df['annee_universitaire'].astype(str).fillna('NA_ANNEE') + 
        df['niveau'].astype(str).fillna('NA_NIV') + 
        df['id_Parcours'].astype(str).fillna('NA_PARC')
    )

    print("\n--- ÉTAPE 1 : CRÉATION DU CODE D'INSCRIPTION ---")
    print(f"🔑 Clé utilisée : code_etudiant + annee_universitaire + niveau + id_Parcours.")
    
    # 2. Hachage et Attribution du code_inscription
    
    # Hachage de la clé pour obtenir le code_inscription
    df['code_inscription'] = df['cle_inscription_unique'].apply(
        lambda x: generer_hash(x, hash_algorithm, 32)
    )

    print(f"✅ {df['code_inscription'].nunique()} codes d'inscription uniques générés initialement.")
    
    # 3. Suppression des Doublons
    
    print("\n--- ÉTAPE 2 : SUPPRESSION DES DOUBLONS D'INSCRIPTION ---")
    
    lignes_avant = len(df)
    
    # Conserver la première occurrence du code d'inscription en doublon
    # C'est l'étape qui supprime les lignes.
    df_final = df.drop_duplicates(subset=['code_inscription'], keep='first')
    
    lignes_supprimees = lignes_avant - len(df_final)

    print(f"🔥 Lignes en doublon supprimées : **{lignes_supprimees}**.")
    
    # 4. Nettoyage final
    
    colonnes_a_supprimer = ['cle_inscription_unique']
    df_final = df_final.drop(columns=colonnes_a_supprimer, errors='ignore')
    
    print("\n==================================================================")
    print("✨ RÉSULTAT FINAL DU GESTIONNAIRE D'INSCRIPTION")
    print(f"Total des lignes conservées : **{len(df_final)}**.")
    print(f"Total des codes d'inscription uniques : **{df_final['code_inscription'].nunique()}**.")
    print("==================================================================")

    return df_final