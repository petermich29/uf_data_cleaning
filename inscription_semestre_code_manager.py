# inscription_semestre_code_manager.py

import pandas as pd
import numpy as np

def gerer_code_inscription_par_semestre(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme les inscriptions de niveau annuel à niveau semestriel (explosion).
    Crée un identifiant unique (code_inscription) basé sur la contrainte d'unicité 
    semestrielle et supprime les doublons.
    
    La clé d'unicité est: code_etudiant + annee_universitaire + id_Parcours + Semestre (SXX).
    Le format du code_inscription est: 'code_etudiant_2X-2X_id_Parcours_SXX'.
    """
    print("\n==================================================================")
    print("🚀 DÉMARRAGE : GESTION DES CODES D'INSCRIPTION PAR SEMESTRE")
    print(f"Total des lignes d'inscription initial : {len(df)}")
    print("==================================================================")
    
    # --- 1. Préparation des Colonnes de Semestre ---
    
    semestre_cols = [f'S{i:02d}' for i in range(1, 17)]
    base_cols = ['code_etudiant', 'annee_universitaire', 'id_Parcours']
    
    # Vérification des colonnes essentielles
    if not all(col in df.columns for col in base_cols) or not any(col in df.columns for col in semestre_cols):
        print("❌ Erreur : Colonnes de base ('code_etudiant', 'annee_universitaire', 'id_Parcours') ou colonnes de semestre ('S01' à 'S16') manquantes.")
        return df

    # --- 2. Transformation Large vers Long (Explosion par Semestre) ---

    print("\n--- ÉTAPE 1 : EXPLOSION DES LIGNES PAR SEMESTRE INSCRIT ---")
    
    # 2.1 Utiliser melt pour transformer les colonnes SXX en lignes
    # On récupère toutes les colonnes qui ne sont PAS des SXX pour les garder comme identifiants
    id_vars_list = [col for col in df.columns if col not in semestre_cols]
    
    df_melted = df.melt(
        id_vars=id_vars_list,
        value_vars=semestre_cols,
        var_name='semestre_id',
        value_name='inscrit'
    )
    
    # 2.2 Filtrer pour ne garder que les inscriptions (où SXX vaut 1)
    df_semestres = df_melted[df_melted['inscrit'] == 1].copy()
    
    lignes_explosees = len(df_semestres)
    print(f"🔥 Lignes après explosion (Semestres inscrits) : {lignes_explosees}.")
    
    # --- 3. Suppression des Doublons sur la Nouvelle Contrainte ---
    
    print("\n--- ÉTAPE 2 : SUPPRESSION DES DOUBLONS SUR LA CONTRAINTE SEMESTRIELLE ---")
    
    # Clé de CONTRAINTE (code_etudiant + annee_universitaire + id_Parcours + semestre_id)
    df_semestres['cle_contrainte'] = (
        df_semestres['code_etudiant'].astype(str).fillna('NA_ID') + 
        df_semestres['annee_universitaire'].astype(str).fillna('NA_ANNEE') + 
        df_semestres['id_Parcours'].astype(str).fillna('NA_PARC') +
        df_semestres['semestre_id'].astype(str).fillna('NA_SEME')
    )
    
    lignes_avant_dedup = len(df_semestres)
    
    # Suppression des doublons (si deux fichiers sources donnent la même inscription semestrielle)
    df_final = df_semestres.drop_duplicates(subset=['cle_contrainte'], keep='first').copy()
    
    lignes_supprimees = lignes_avant_dedup - len(df_final)

    print(f"🔥 Doublons de contrainte semestrielle supprimés : **{lignes_supprimees}**.")
    
    # --- 4. Génération du Code d'Inscription au Format Spécifié ---
    
    print("\n--- ÉTAPE 3 : GÉNÉRATION DU CODE D'INSCRIPTION FORMATÉ ---")

    # Fonction pour convertir '2023-2024' en '23-24'
    def format_annee_courte(annee_full):
        if pd.isna(annee_full):
            return 'NA_A'
        try:
            parts = str(annee_full).split('-')
            # Prend les deux derniers chiffres de chaque année
            return f"{parts[0][-2:]}-{parts[1][-2:]}"
        except:
            return 'ERR_A'

    df_final['annee_courte'] = df_final['annee_universitaire'].apply(format_annee_courte)
    
    # Assemblage du code final : 'code_etudiant_2X-2X_id_Parcours_SXX'
    df_final['code_inscription'] = (
        df_final['code_etudiant'].astype(str) + '_' +
        df_final['annee_courte'].astype(str) + '_' +
        df_final['id_Parcours'].astype(str) + '_' +
        df_final['semestre_id'].astype(str)
    )
    
    # --- 5. Nettoyage final ---
    
    # Suppression des colonnes temporaires et des colonnes SXX originales
    colonnes_a_supprimer = ['cle_contrainte', 'inscrit', 'annee_courte'] + semestre_cols
    df_final = df_final.drop(columns=colonnes_a_supprimer, errors='ignore')
    
    # Mise à jour du nom de la colonne de semestre pour correspondre aux COLONNES_ATTENDUES si nécessaire
    # Note : Le nom 'semestre_id' peut être renommé 'semestre' pour la cohérence finale.
    #if 'semestre_id' in df_final.columns:
         #df_final.rename(columns={'semestre_id': 'semestre'}, inplace=True)
    
    print("\n==================================================================")
    print("✨ RÉSULTAT FINAL DU GESTIONNAIRE D'INSCRIPTION PAR SEMESTRE")
    print(f"Total des inscriptions semestrielles conservées : **{len(df_final)}**.")
    print(f"Total des codes d'inscription uniques générés : **{df_final['code_inscription'].nunique()}**.")
    print("==================================================================")

    return df_final