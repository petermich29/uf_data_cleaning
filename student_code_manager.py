import pandas as pd
import numpy as np
from tqdm import tqdm
import re # Nécessaire pour les expressions régulières dans le nettoyage

# --- Paramètres Globaux (Conservés pour la clarté) ---
KEY_COLUMNS = [
    'np_naissance', 
    'np_cin', 
    'np_cin_lieu', 
    'np_telephone',
    'np_mail',
    'np_composante_mention' # Clé faible, utilisant composante et mention
]
COLONNES_FORTES_CHECK = ['cin', 'naissance_date'] 


# --- Fonctions de Nettoyage et de Préparation ---

def standardiser_champs_pour_hachage(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise les champs Nom, Prénoms, etc., pour une meilleure robustesse des clés."""
    # Colonnes à nettoyer spécifiquement pour la création de clés
    cols_a_nettoyer = ['nom', 'prenoms', 'cin_lieu', 'mail', 'composante', 'mention'] 
    
    for col in cols_a_nettoyer:
        if col in df.columns:
            df[f'{col}_standard'] = df[col].astype(str).str.upper().str.strip()
            if col not in ['cin', 'telephone']:
                df[f'{col}_standard'] = df[f'{col}_standard'].str.replace(r'[^A-Z0-9]', '', regex=True)
            df[f'{col}_standard'] = df[f'{col}_standard'].replace('', pd.NA)
        else:
            df[f'{col}_standard'] = pd.NA
    
    # Création du champ Nom_Prenoms standard (Autorise Prénoms NULL)
    df['nom_prenoms_standard'] = pd.NA
    nom_std = df.get('nom_standard', pd.Series(pd.NA, index=df.index))
    prenoms_std = df.get('prenoms_standard', pd.Series(pd.NA, index=df.index))
    condition_nom_ok = nom_std.notna() 
    
    df.loc[condition_nom_ok, 'nom_prenoms_standard'] = \
        nom_std.fillna('') + prenoms_std.fillna('')
        
    df['nom_prenoms_standard'] = df['nom_prenoms_standard'].replace('', pd.NA)
    
    return df

def creer_cles_de_concatenation(df: pd.DataFrame) -> pd.DataFrame:
    """Crée les colonnes de concaténation robustes demandées."""
    cles_config = {
        'np_naissance': ['nom_prenoms_standard', 'naissance_date'],
        'np_cin': ['nom_prenoms_standard', 'cin'],
        'np_cin_lieu': ['nom_prenoms_standard', 'cin_lieu_standard'], # Utiliser la version standardisée
        'np_telephone': ['nom_prenoms_standard', 'telephone'],
        'np_mail': ['nom_prenoms_standard', 'mail_standard'],        # Utiliser la version standardisée
        'np_composante_mention': ['nom_prenoms_standard', 'composante_standard', 'mention_standard'],
    }

    reference_cols = ['nom_prenoms_standard', 'naissance_date', 'cin', 'telephone', 'composante_standard', 'mention_standard', 'cin_lieu_standard', 'mail_standard']
    for col in reference_cols:
        if col not in df.columns:
            df[col] = pd.Series(pd.NA, index=df.index)
    
    for key_name, components in cles_config.items():
        condition_creation = df['nom_prenoms_standard'].notna()
        for comp in components[1:]: 
            condition_creation &= df[comp].notna()

        df[key_name] = pd.NA 
        
        if condition_creation.any():
            cols_to_concat = []
            for col in components:
                data_series = df.loc[condition_creation, col].astype(str).str.upper()
                
                if col in ['naissance_date', 'cin', 'telephone']:
                    cleaned_data = data_series.str.replace(r'[^A-Z0-9-]', '', regex=True)
                else:
                    cleaned_data = data_series
                    
                cols_to_concat.append(cleaned_data)
            
            new_keys = cols_to_concat[0]
            for i in range(1, len(cols_to_concat)):
                new_keys = new_keys.str.cat(cols_to_concat[i], sep='_')

            df.loc[condition_creation, key_name] = new_keys
            
    return df

def verifier_contradiction_forte(df: pd.DataFrame, indices_a_tester: pd.Index, colonnes_fortes: list) -> bool:
    """Vérifie si un groupe d'enregistrements combiné présente une forte contradiction sur CIN ou Date de Naissance."""
    df_test = df.loc[indices_a_tester]
    
    for col in colonnes_fortes:
        unique_values = df_test[col].dropna().unique()
        # Si plus d'une valeur unique non-NA est trouvée, il y a contradiction
        if len(unique_values) > 1:
            return True
            
    return False

# --- Fonction Principale de Dédoublonnage ---

def gerer_code_etudiant_et_consolider(df: pd.DataFrame, hash_algorithm: str = 'SHA256') -> pd.DataFrame:
    """
    Détecte les doublons par chaînage, assigne le code étudiant ETU<ANNEE_MIN>_<SEQUENCE>
    et consolide les champs.
    """
    if df.empty:
        return df

    print("--- 🔢 Attribution des Codes Étudiants et Détection de Doublons ---")

    # Étape 1 à 3 : Initialisation et Préparation
    df['id_temporaire'] = df.index + 1
    df = standardiser_champs_pour_hachage(df)
    df = creer_cles_de_concatenation(df)

    # Étape 4 : Propagation du plus petit ID pour regrouper les doublons (Algorithme de chaînage)
    df_temp = df[['id_temporaire'] + KEY_COLUMNS].copy()
    for col in KEY_COLUMNS:
        df_temp[col] = df_temp[col].astype(pd.StringDtype())

    iteration = 0
    while True:
        iteration += 1
        nouvelles_fusions = 0
        tqdm.write(f"--- Itération {iteration} : Détection et Chaînage ---")
        
        id_temp_current = df_temp['id_temporaire'].copy()
        
        for key_col in tqdm(KEY_COLUMNS, desc=f"Regroupement par clé"):
            mask_not_na = df_temp[key_col].notna()
            df_subset = df_temp.loc[mask_not_na].copy()
            
            if df_subset.empty:
                continue

            # 1. Calcul de l'ID Canonique (le plus petit ID du groupe)
            grouped = df_subset.groupby(key_col)
            canonical_ids = grouped['id_temporaire'].transform('min')
            
            # 2. Condition de fusion: L'ID actuel doit être plus grand que l'ID canonique
            condition_fusion_simple = (df_subset['id_temporaire'] > canonical_ids)
            indices_a_maj = condition_fusion_simple[condition_fusion_simple].index
            
            if len(indices_a_maj) > 0:
                indices_valides = indices_a_maj 
                
                # --- RÈGLE CONDITIONNELLE D'EXCLUSION pour la clé faible ---
                if key_col == 'np_composante_mention':
                    propositions = pd.DataFrame({
                        'id_courant': df_temp.loc[indices_a_maj, 'id_temporaire'],
                        'id_cible': canonical_ids.loc[indices_a_maj]
                    }).reset_index()

                    fusions_inter_groupes = propositions[propositions['id_courant'] != propositions['id_cible']].copy()
                    groupes_a_tester = fusions_inter_groupes.drop_duplicates(subset=['id_courant', 'id_cible'])
                    groupes_valides_cible_id = set()
                    
                    for _, row in groupes_a_tester.iterrows():
                        id_courant = row['id_courant']
                        id_cible = row['id_cible']
                        
                        indices_courant = df_temp[df_temp['id_temporaire'] == id_courant].index
                        indices_cible = df_temp[df_temp['id_temporaire'] == id_cible].index
                        indices_combines = indices_courant.union(indices_cible)

                        if not verifier_contradiction_forte(df, indices_combines, COLONNES_FORTES_CHECK):
                            groupes_valides_cible_id.add(id_cible)
                            
                    indices_valides = propositions[
                        (propositions['id_cible'].isin(groupes_valides_cible_id)) | 
                        (propositions['id_courant'] == propositions['id_cible']) 
                    ]['index']
                # --- Fin de la Règle Conditionnelle ---
                
                canonical_values_validated = canonical_ids.loc[indices_valides]
                df_temp.loc[indices_valides, 'id_temporaire'] = canonical_values_validated
                nouvelles_fusions += len(indices_valides)
                
        if nouvelles_fusions == 0:
            tqdm.write("Pas de nouvelles fusions détectées. Le processus a convergé.")
            break
        elif iteration == 1:
            tqdm.write(f"Fusion de {nouvelles_fusions} liens détectée. Continuer l'itération pour chaînage.")
            
    # Étape 5 : Mise à jour du DataFrame original après chaînage
    df['id_groupe'] = df_temp['id_temporaire']
    
    
    # --- ÉTAPE 6 : NOMENCLATURE DU CODE ÉTUDIANT (ETU<ANNEE_MIN>_<SEQUENCE>) ---
    
    # 6.1 : Déterminer l'année universitaire la plus ancienne pour chaque groupe
    if 'annee_universitaire' not in df.columns:
         df['annee_universitaire'] = 'NON_SPECIFIEE'

    # Extraire l'année de début (ex: 2023 de 2023-2024)
    df['annee_debut'] = df['annee_universitaire'].astype(str).str.split('-').str[0].astype(int, errors='ignore').fillna(9999)

    # Propagation de l'année la plus petite au sein de chaque groupe
    annee_min_par_groupe = df.groupby('id_groupe', dropna=False)['annee_debut'].transform('min')
    df['annee_universitaire_min'] = annee_min_par_groupe
    
    # 6.2 : Génération du Numéro Séquentiel (xxxxxx)
    
    # Tri par année min pour attribuer les codes séquentiels dans l'ordre chronologique d'ancienneté.
    codes_uniques = df.drop_duplicates(subset=['id_groupe']).sort_values(by='annee_universitaire_min').index
    
    # Créer un mapping de l'id_groupe vers un nouveau numéro séquentiel (1, 2, 3...)
    group_to_sequence = {
        group_id: seq + 1 
        for seq, group_id in enumerate(df.loc[codes_uniques, 'id_groupe'].unique())
    }
    
    df['code_final_sequence'] = df['id_groupe'].map(group_to_sequence)

    # 6.3 : Concaténation finale
    sequence_formattee = df['code_final_sequence'].astype(str).str.zfill(6)
    df['code_etudiant'] = (
        'ETU' 
        + df['annee_universitaire_min'].astype(str) 
        + '_' 
        + sequence_formattee
    )
    
    # Propagation formelle du code généré à toutes les lignes du groupe (même s'il l'est déjà)
    df['code_etudiant'] = df.groupby('id_groupe', dropna=False)['code_etudiant'].transform('first')


    # --- ÉTAPE 7 : CONSOLIDATION DES CHAMPS (IMPUTATION) ---
    colonnes_consolidation = [
        'nom', 'prenoms', 'cin', 'cin_date', 'cin_lieu', 'nationalite', 'naissance_lieu', 
        'mail', 'telephone', 'adresse', 'sexe', 'bacc_annee', 'bacc_serie', 
        'bacc_numero', 'bacc_centre', 'bacc_mention',
        'naissance_date', 'numero_inscription'
    ]

    print("\n--- ÉTAPE 7 : CONSOLIDATION DES CHAMPS (IMPUTATION) ---")
    
    for col in tqdm(colonnes_consolidation, desc="Consolidation des valeurs non-NA"):
        if col in df.columns:
            is_object = df[col].dtype == 'object' or df[col].dtype.name == 'string'
            if is_object:
                df[col] = df[col].replace('', np.nan) 
            
            df[col] = df.groupby('id_groupe', dropna=False)[col].transform('first')
            
            if is_object:
                df[col] = df[col].convert_dtypes()

    print("✅ Consolidation des champs effectuée.")


    # --- Étape 8 : Nettoyage final ---
    cols_a_dropper = [col for col in df.columns if col.endswith('_standard') or col in KEY_COLUMNS or col.startswith('id_groupe') or col.startswith('code_final_sequence') or col.startswith('id_temporaire') or col in ['annee_debut', 'annee_universitaire_min']]
    df = df.drop(columns=cols_a_dropper, errors='ignore')

    nombre_codes_uniques = df['code_etudiant'].nunique()
    lignes_total = len(df)
    
    print(f"\n✅ Traitement terminé : {lignes_total} lignes, {nombre_codes_uniques} codes étudiants uniques.")
    
    return df