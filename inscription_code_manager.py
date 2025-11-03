# inscription_code_manager.py (Mis à jour avec id_Parcours)

import pandas as pd
import numpy as np

def gerer_code_inscription_et_supprimer_doublons(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée un identifiant unique (code_inscription) basé sur la nomenclature simplifiée :
    <CODE_ETUDIANT>_<SEQUENCE_UNIQUE_INSCRIPTION>
    et supprime les doublons basés sur la contrainte d'unicité.
    
    La clé d'unicité est: code_etudiant + annee_universitaire + niveau + id_Parcours.
    """
    print("\n==================================================================")
    print("🚀 DÉMARRAGE : GESTION DES CODES D'INSCRIPTION ET SUPPRESSION DES DOUBLONS")
    print(f"Total des lignes d'inscription initial : {len(df)}")
    print("==================================================================")
    
    # --- 1. Vérification des Colonnes et Création de la Clé d'Unicité ---
    
    # CHANGEMENT ICI : Utilisation de 'id_Parcours' (P majuscule)
    colonnes_requises = ['code_etudiant', 'annee_universitaire', 'niveau', 'id_Parcours']
    for col in colonnes_requises:
        if col not in df.columns:
            print(f"❌ Erreur : Colonne '{col}' manquante. Le processus s'arrête.")
            return df

    # Clé de CONTRAINTE (utilisée pour détecter les doublons)
    df['cle_contrainte'] = (
        df['code_etudiant'].astype(str).fillna('NA_ID') + 
        df['annee_universitaire'].astype(str).fillna('NA_ANNEE') + 
        df['niveau'].astype(str).fillna('NA_NIV') + 
        df['id_Parcours'].astype(str).fillna('NA_PARC') # CHANGEMENT ICI
    )

    print("\n--- ÉTAPE 1 : SUPPRESSION DES DOUBLONS SUR LA CONTRAINTE ---")
    
    lignes_avant = len(df)
    
    # Suppression des doublons basés sur la contrainte d'unicité
    df_final = df.drop_duplicates(subset=['cle_contrainte'], keep='first').copy()
    
    lignes_supprimees = lignes_avant - len(df_final)

    print(f"🔥 Lignes en doublon de contrainte supprimées : **{lignes_supprimees}**.")
    
    # --- 2. Génération de la Nomenclature code_inscription Simplifiée ---
    
    print("\n--- ÉTAPE 2 : GÉNÉRATION DU CODE D'INSCRIPTION SIMPLIFIÉ ---")
    
    # 2.1 Déterminer l'ordre des inscriptions pour chaque étudiant
    df_final['annee_debut'] = df_final['annee_universitaire'].astype(str).str.split('-').str[0].astype(int, errors='ignore').fillna(9999)
    
    # Trier d'abord par code_etudiant, puis par l'année de l'inscription (pour l'ordre séquentiel)
    df_final.sort_values(by=['code_etudiant', 'annee_debut'], ascending=[True, True], inplace=True)
    
    # 2.2 Générer la séquence (001, 002, 003, ...) pour chaque groupe (code_etudiant)
    df_final['sequence_compteur'] = df_final.groupby('code_etudiant').cumcount() + 1
    
    # Formatage de la séquence sur 3 chiffres (XXX)
    sequence_formattee = df_final['sequence_compteur'].astype(str).str.zfill(3)
    
    # 2.3 Assemblage du code final : <CODE_ETUDIANT>_<SEQUENCE_3_CHIFFRES>
    df_final['code_inscription'] = (
        df_final['code_etudiant'].astype(str) + '_' + sequence_formattee
    )
    
    print(f"✅ {df_final['code_inscription'].nunique()} codes d'inscription uniques générés avec la nomenclature.")

    # --- 3. Nettoyage final ---
    
    colonnes_a_supprimer = ['cle_contrainte', 'annee_debut', 'sequence_compteur']
    df_final = df_final.drop(columns=colonnes_a_supprimer, errors='ignore')
    
    print("\n==================================================================")
    print("✨ RÉSULTAT FINAL DU GESTIONNAIRE D'INSCRIPTION")
    print(f"Total des lignes conservées : **{len(df_final)}**.")
    print(f"Total des codes d'inscription uniques : **{df_final['code_inscription'].nunique()}**.")
    print("==================================================================")

    return df_final