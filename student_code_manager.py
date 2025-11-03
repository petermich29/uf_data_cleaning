import pandas as pd
import numpy as np
import re
from tqdm import tqdm

# Liste des colonnes nécessaires pour le matching
COLONNES_REQUISES = [
    'nom', 'prenoms', 'naissance_date', 
    'cin', 'cin_lieu', 'telephone', 'mail', 
    'id_mention', # Conservé par précaution
    'composante', # Ajouté pour la nouvelle clé
    'mention' # Ajouté pour la nouvelle clé
]

# Clés de concaténation qui serviront de base à la détection de doublons
# La clé 'np_id_mention' est renommée en 'np_composante_mention' pour refléter la nouvelle logique.
KEY_COLUMNS = [
    'np_naissance', 
    'np_cin', 
    'np_cin_lieu', 
    'np_telephone',
    'np_mail',
    'np_composante_mention' # Clé faible, utilisant composante et mention
]

# Clés d'identité fortes pour le contrôle conditionnel (non utilisées dans le chaînage actuel)
STRONG_IDENTITY_COLUMNS = ['cin', 'naissance_date']


# --- Fonctions de Nettoyage et de Préparation ---

def standardiser_champs_pour_hachage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardise les champs Nom et Prénoms pour une meilleure robustesse des clés.
    Supprime les caractères non alphanumériques et met en majuscule.
    
    Crée 'nom_prenoms_standard' qui tolère les prénoms nuls, tant que le nom est présent.
    """
    print("--- ⚙️ Standardisation des champs de base pour le Hachage ---")

    # Colonnes à nettoyer spécifiquement pour la création de clés
    # Ajout de composante et mention au nettoyage standard
    cols_a_nettoyer = ['nom', 'prenoms', 'cin_lieu', 'mail', 'composante', 'mention'] 
    
    # Prétraitement de toutes les colonnes requises
    for col in tqdm(cols_a_nettoyer, desc="Nettoyage des colonnes pour clés"):
        if col in df.columns:
            # Remplacer NaN par chaîne vide, mettre en majuscule, supprimer les espaces multiples et caractères spéciaux
            df[f'{col}_standard'] = df[col].astype(str).str.upper().str.strip()
            # Suppression des caractères non alphanumériques pour un matching strict (sauf CIN déjà formaté)
            if col not in ['cin', 'telephone']: # Ces colonnes devraient déjà être propres
                df[f'{col}_standard'] = df[f'{col}_standard'].str.replace(r'[^A-Z0-9]', '', regex=True)
            
            df[f'{col}_standard'] = df[f'{col}_standard'].replace('', pd.NA)
        else:
            # Créer la colonne standard si elle n'existe pas, la remplir avec NA
            df[f'{col}_standard'] = pd.NA
            tqdm.write(f"⚠️ Colonne '{col}' manquante, remplie avec NA.")
    
    # Création du champ Nom_Prenoms standard (Autorise Prénoms NULL)
    df['nom_prenoms_standard'] = pd.NA
    
    # Récupération des versions standardisées, avec gestion si la colonne n'a pas pu être créée
    nom_std = df.get('nom_standard', pd.Series(pd.NA, index=df.index))
    prenoms_std = df.get('prenoms_standard', pd.Series(pd.NA, index=df.index))
    
    # La concaténation est possible si 'nom' est non nul.
    condition_nom_ok = nom_std.notna() 
    
    df.loc[condition_nom_ok, 'nom_prenoms_standard'] = \
        nom_std.fillna('') + prenoms_std.fillna('')
        
    df['nom_prenoms_standard'] = df['nom_prenoms_standard'].replace('', pd.NA) # Ne doit pas être une chaîne vide
    
    # Assurer que 'id_mention' est présent (pour la nouvelle clé)
    if 'id_mention' not in df.columns:
        df['id_mention'] = pd.NA
        tqdm.write(f"⚠️ Colonne 'id_mention' manquante, remplie avec NA.")
        
    return df

def creer_cles_de_concatenation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée les colonnes de concaténation robustes demandées, y compris la clé np_composante_mention.
    """
    print("\n--- 🗝️ Création des Clés de Concatenation ---")
    
    # Configuration des clés
    cles_config = {
        'np_naissance': ['nom_prenoms_standard', 'naissance_date'],
        'np_cin': ['nom_prenoms_standard', 'cin'],
        'np_cin_lieu': ['nom_prenoms_standard', 'cin_lieu'],
        'np_telephone': ['nom_prenoms_standard', 'telephone'],
        'np_mail': ['nom_prenoms_standard', 'mail'],
        # Nouvelle clé basée sur Nom/Prénoms, Composante et Mention
        'np_composante_mention': ['nom_prenoms_standard', 'composante_standard', 'mention_standard'],
    }

    # S'assurer que les colonnes de référence existent (y compris les versions standardisées)
    reference_cols = [
        'nom_prenoms_standard', 'naissance_date', 'cin', 'cin_lieu', 
        'telephone', 'mail', 'id_mention', 'composante_standard', 'mention_standard'
    ]
    
    for col in reference_cols:
        if col not in df.columns:
            df[col] = pd.Series(pd.NA, index=df.index)
    
    for key_name, components in tqdm(cles_config.items(), desc="Génération des clés"):
        
        # Les composants sont les colonnes à utiliser pour le hachage
        
        # 1. Condition de non-nullité : TOUS les composants DOIVENT être non nuls
        condition_creation = df['nom_prenoms_standard'].notna()
        # Assurer que tous les composants additionnels sont non nuls
        for comp in components[1:]: 
            condition_creation &= df[comp].notna()

        df[key_name] = pd.NA # Initialisation de la colonne à NA
        
        if condition_creation.any():
            # Prétraitement des composants pour la concaténation (uniquement pour les lignes éligibles)
            cols_to_concat = []
            for col in components:
                
                # Utilisation des données pour les lignes concernées
                data_series = df.loc[condition_creation, col].astype(str).str.upper()
                
                # NETTOYAGE RÉTROACTIF POUR LES CLÉS CRITIQUES 
                if col in ['naissance_date', 'cin', 'telephone']:
                    # Nettoyage spécifique pour les ID bruts
                    cleaned_data = data_series.str.replace(r'[^A-Z0-9-]', '', regex=True)
                else:
                    # nom_prenoms_standard et les autres sont déjà nettoyés et en majuscules
                    cleaned_data = data_series
                    
                cols_to_concat.append(cleaned_data)
            
            # Concaténation avec '_'
            new_keys = cols_to_concat[0]
            for i in range(1, len(cols_to_concat)):
                 # Concaténation de tous les composants avec '_'
                 new_keys = new_keys.str.cat(cols_to_concat[i], sep='_')

            # Application des nouvelles clés sur les lignes éligibles
            df.loc[condition_creation, key_name] = new_keys
            
        else:
            tqdm.write(f"ℹ️ Clé {key_name} non générée : aucun enregistrement valide.")
            
    print("✅ Clés de concaténation créées avec la règle de non-nullité stricte.")
    return df

# --- NOUVELLE FONCTION : Vérification de Contradiction Forte ---

def verifier_contradiction_forte(df: pd.DataFrame, indices_a_tester: pd.Index, colonnes_fortes: list) -> bool:
    """
    Vérifie si un ensemble d'enregistrements (indices) présente une forte contradiction 
    sur les identifiants clés (CIN, Date de Naissance).
    
    Une forte contradiction est détectée si, pour l'une des colonnes_fortes, il existe 
    plus d'une valeur non-NA unique.
    
    :param df: Le DataFrame principal contenant les données nettoyées.
    :param indices_a_tester: Les indices des lignes à vérifier.
    :param colonnes_fortes: Liste des colonnes fortes à vérifier (ex: ['cin', 'naissance_date']).
    :return: True si une contradiction est trouvée, False sinon.
    """
    # Le test doit porter sur les valeurs consolidées/nettoyées de df
    df_test = df.loc[indices_a_tester]
    
    for col in colonnes_fortes:
        # Récupérer les valeurs uniques non-NA
        # df[col] est censé être déjà nettoyé (cin formaté, naissance_date en datetime)
        unique_values = df_test[col].dropna().unique()
        
        # Si plus d'une valeur unique non-NA est trouvée, il y a contradiction
        if len(unique_values) > 1:
            return True
            
    return False

# --- Fonction Principale d'Assignation de Code ---

def gerer_code_etudiant_et_consolider(df: pd.DataFrame, hash_algorithm: str) -> pd.DataFrame:
    """
    Attribue un code étudiant unique, regroupant les doublons identifiés
    par les clés de concaténation, puis consolide les champs.
    
    L'étape 4 implémente l'algorithme de chaînage avec convergence garantie
    pour toutes les clés (y compris np_composante_mention), en appliquant
    une condition d'exclusion pour la clé faible.
    """
    if df.empty:
        print("DataFrame vide. Aucun code étudiant assigné.")
        return df

    print("\n--- 🔢 Attribution des Codes Étudiants et Détection de Doublons ---")

    # Étape 1 : Initialisation de l'ID temporaire unique pour chaque ligne
    df['id_temporaire'] = df.index + 1
    
    # Étape 2 : Standardisation des champs pour la création des clés
    df = standardiser_champs_pour_hachage(df)
    
    # Étape 3 : Création des clés de matching
    df = creer_cles_de_concatenation(df)

    # Étape 4 : Propagation du plus petit ID pour regrouper les doublons (Algorithme de chaînage)
    
    # Utiliser une copie des colonnes pertinentes pour les manipulations d'ID
    df_temp = df[['id_temporaire'] + KEY_COLUMNS].copy()
    
    # Colonnes fortes à vérifier pour la règle conditionnelle (elles doivent être présentes et nettoyées dans df)
    COLONNES_FORTES_CHECK = ['cin', 'naissance_date'] 

    # S'assurer que les colonnes clés sont de type StringDtype pour la robustesse
    for col in KEY_COLUMNS:
        df_temp[col] = df_temp[col].astype(pd.StringDtype())

    total_doublons = 0
    iteration = 0
    
    # Algorithme de chaînage : itérer jusqu'à convergence (nouvelles_fusions == 0)
    while True:
        iteration += 1
        nouvelles_fusions = 0
        tqdm.write(f"\n--- Itération {iteration} : Détection et Chaînage ---")
        
        # Copie des ID actuels pour que les calculs de 'min' soient cohérents
        id_temp_current = df_temp['id_temporaire'].copy()
        
        for key_col in tqdm(KEY_COLUMNS, desc=f"Regroupement par clé"):
            
            mask_not_na = df_temp[key_col].notna()
            df_subset = df_temp.loc[mask_not_na].copy() # Copie pour le groupement
            
            if df_subset.empty:
                continue

            # 1. Calcul de l'ID Canonique (le plus petit ID du groupe)
            grouped = df_subset.groupby(key_col)
            canonical_ids = grouped['id_temporaire'].transform('min')
            
            # 2. Condition de fusion: L'ID actuel doit être plus grand que l'ID canonique
            condition_fusion_finale = (df_subset['id_temporaire'] > canonical_ids)
            
            # Indices des lignes qui proposent une mise à jour d'ID dans cette clé
            indices_a_maj = condition_fusion_finale[condition_fusion_finale].index
            
            if len(indices_a_maj) > 0:
                
                indices_valides = indices_a_maj # Par défaut, toutes les fusions sont valides
                
                # --- RÈGLE CONDITIONNELLE D'EXCLUSION pour la clé faible ---
                if key_col == 'np_composante_mention':
                    
                    # Mapping des fusions: ID actuel -> ID Canonique proposé
                    propositions = pd.DataFrame({
                        'id_courant': df_temp.loc[indices_a_maj, 'id_temporaire'],
                        'id_cible': canonical_ids.loc[indices_a_maj]
                    }).reset_index() # Conserve l'index original

                    # Identifier les fusions qui joignent deux groupes différents (id_courant != id_cible)
                    fusions_inter_groupes = propositions[propositions['id_courant'] != propositions['id_cible']].copy()

                    # On ne teste qu'une seule fois la fusion d'un groupe A vers B
                    groupes_a_tester = fusions_inter_groupes.drop_duplicates(subset=['id_courant', 'id_cible'])
                    
                    # ID des cibles (id_cible) qui ont été testées et validées
                    groupes_valides_cible_id = set()
                    
                    for _, row in groupes_a_tester.iterrows():
                        id_courant = row['id_courant']
                        id_cible = row['id_cible']
                        
                        # Récupérer tous les indices des lignes faisant partie des deux groupes (avant fusion)
                        indices_courant = df_temp[df_temp['id_temporaire'] == id_courant].index
                        indices_cible = df_temp[df_temp['id_temporaire'] == id_cible].index
                        indices_combines = indices_courant.union(indices_cible)

                        # Vérification de Contradiction Forte
                        if not verifier_contradiction_forte(df, indices_combines, COLONNES_FORTES_CHECK):
                            # Si AUCUNE contradiction, autoriser la fusion
                            groupes_valides_cible_id.add(id_cible)
                            
                    
                    # Le filtre: seules les lignes dont l'id_cible a été validé sont conservées
                    # On conserve aussi les fusions intra-groupe (id_courant == id_cible)
                    indices_valides = propositions[
                        (propositions['id_cible'].isin(groupes_valides_cible_id)) | 
                        (propositions['id_courant'] == propositions['id_cible'])    
                    ]['index']

                # --- Fin de la Règle Conditionnelle ---
                
                # Récupérer les nouvelles valeurs canoniques pour les indices validés/non-filtrés
                canonical_values_validated = canonical_ids.loc[indices_valides]
                
                # Appliquer la mise à jour dans df_temp (utilise les indices originaux)
                df_temp.loc[indices_valides, 'id_temporaire'] = canonical_values_validated
                nouvelles_fusions += len(indices_valides)
                
            # Mise à jour du total des doublons (sera utilisé dans le message de conclusion)
            if key_col == 'np_composante_mention' and iteration == 1:
                 # Seules les fusions validées sont comptées
                 tqdm.write(f"   (Clé faible) Fusions validées par np_composante_mention : {len(indices_valides)}")
            
        total_doublons += nouvelles_fusions
        
        # Test de convergence
        if nouvelles_fusions == 0:
            tqdm.write("Pas de nouvelles fusions détectées. Le processus a convergé.")
            break
        elif iteration == 1:
            tqdm.write(f"Fusion de {nouvelles_fusions} liens détectée. Continuer l'itération pour chaînage.")
            
    # Étape 5 : Mise à jour du DataFrame original
    df['id_groupe'] = df_temp['id_temporaire']
    
    # Étape 6 : Finalisation du code étudiant
    # Attribution d'un numéro séquentiel unique à chaque groupe d'étudiants (code_final_sequence)
    codes_uniques = df.groupby('id_groupe').ngroup() + 1
    df['code_final_sequence'] = codes_uniques

    df['code_etudiant'] = 'ETU_' + df['code_final_sequence'].astype(str).str.zfill(8)

    # --- Étape 7 : Consolidation des champs (Imputation des valeurs non nulles) ---
    colonnes_consolidation = [
        'nom', 'prenoms', 'cin', 'cin_date', 'cin_lieu', 'nationalite', 'naissance_lieu', 
        'mail', 'telephone', 'adresse', 'sexe', 'bacc_annee', 'bacc_serie_technique', 'bacc_serie', 
        'bacc_numero', 'bacc_centre', 'bacc_mention',
        'naissance_date', 'naissance_annee', 'naissance_mois', 'naissance_jour', 
        'id_mention', 'composante', 'mention' # Ajout pour consolidation
    ]

    print("\n--- ÉTAPE 7 : CONSOLIDATION DES CHAMPS (IMPUTATION) ---")
    
    for col in tqdm(colonnes_consolidation, desc="Consolidation des valeurs non-NA"):
        if col in df.columns:
             # Convertir en string si c'est de type objet pour le remplacement
            is_object = df[col].dtype == 'object' or df[col].dtype.name == 'string'
            if is_object:
                # Remplacer les chaînes vides par NaN pour garantir que transform('first') fonctionne correctement
                df[col] = df[col].replace('', np.nan) 
            
            # Application de la consolidation: propager la première valeur non-NA
            # Groupement par l'ID de groupe final
            df[col] = df.groupby('id_groupe', dropna=False)[col].transform('first')
            
            # Tenter de remettre le type de données initial si possible
            if is_object:
                df[col] = df[col].convert_dtypes() # Utiliser StringDtype si possible

    print("✅ Consolidation des champs (imputation des valeurs non nulles des doublons) effectuée.")


    # --- Étape 8 : Nettoyage final ---
    # Suppression des colonnes de travail
    cols_a_dropper = [col for col in df.columns if col.endswith('_standard') or col in KEY_COLUMNS or col.startswith('id_groupe') or col.startswith('code_final_sequence') or col.startswith('id_temporaire')]
    df = df.drop(columns=cols_a_dropper, errors='ignore')

    nombre_codes_uniques = df['code_etudiant'].nunique()
    lignes_total = len(df)
    
    print(f"\n✅ Traitement terminé : {lignes_total} lignes, {nombre_codes_uniques} codes étudiants uniques.")
    print(f"   {total_doublons} lignes ont été regroupées grâce aux clés de doublons.")
    
    return df
