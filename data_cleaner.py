import pandas as pd
import glob
import re
import os
from tqdm import tqdm
import numpy as np

# --- Fonctions de chargement et combinaison ---

def charger_et_combiner_fichiers(dossier_path: str, filtre_2023: str, filtre_2024: str, filtre_2025: str) -> pd.DataFrame:
    """
    Recherche les fichiers Excel contenant les chaînes de filtre spécifiées (2023, 2024, 2025),
    les charge, leur assigne l'année universitaire correspondante, et les combine.
    Affiche le nom des fichiers chargés.
    """
    # Recherche récursive de fichiers
    file_pattern_2023 = os.path.join(dossier_path, f"**\*{filtre_2023}*.xlsx")
    file_pattern_2024 = os.path.join(dossier_path, f"**\*{filtre_2024}*.xlsx")
    file_pattern_2025 = os.path.join(dossier_path, f"**\*{filtre_2025}*.xlsx") # Ajout du filtre 2025

    fichiers_excel_2023 = glob.glob(file_pattern_2023, recursive=True)
    fichiers_excel_2024 = glob.glob(file_pattern_2024, recursive=True)
    fichiers_excel_2025 = glob.glob(file_pattern_2025, recursive=True) # Recherche des fichiers 2025
    
    # Combinaison des listes de fichiers (en utilisant set pour éviter les doublons)
    fichiers_excel = list(set(fichiers_excel_2023 + fichiers_excel_2024 + fichiers_excel_2025))

    if not fichiers_excel:
        print(f"❌ Aucun fichier Excel trouvé dans {dossier_path} avec les motifs spécifiés.")
        return pd.DataFrame()

    print(f"--- 📂 {len(fichiers_excel)} Fichiers à traiter (incluant les sous-dossiers) ---")
    liste_dfs = []
    fichiers_charges_par_annee = {} # Pour le récapitulatif

    # Utilisation de tqdm pour la barre de progression
    for fichier in tqdm(fichiers_excel, desc="Chargement et combinaison des données"):
        annee_universitaire = None
        nom_fichier = os.path.basename(fichier)
        
        # Attribution de l'année universitaire basée sur le nom du fichier (du plus récent au plus ancien)
        if re.search(filtre_2025, fichier, re.IGNORECASE):
            annee_universitaire = '2024-2025'
        elif re.search(filtre_2024, fichier, re.IGNORECASE):
            annee_universitaire = '2023-2024'
        elif re.search(filtre_2023, fichier, re.IGNORECASE):
            annee_universitaire = '2022-2023'
        
        if annee_universitaire:
            try:
                # Lecture de la première feuille
                df = pd.read_excel(fichier, sheet_name=0)
                df['annee_universitaire'] = annee_universitaire
                liste_dfs.append(df)
                
                # Enregistrement pour le récapitulatif
                if annee_universitaire not in fichiers_charges_par_annee:
                    fichiers_charges_par_annee[annee_universitaire] = []
                fichiers_charges_par_annee[annee_universitaire].append(nom_fichier)
                
                # AFFICHAGE DU NOM DU FICHIER CHARGÉ (Utilisation de tqdm.write pour ne pas perturber la barre de progression)
                tqdm.write(f"  > Fichier chargé : {nom_fichier} ({annee_universitaire}, {len(df)} lignes)")

            except Exception as e:
                tqdm.write(f"⚠️ Erreur lors du chargement de {nom_fichier}: {e}")
        else:
            tqdm.write(f"⚠️ Fichier ignoré : {nom_fichier} ne correspond à aucun filtre d'année universitaire.")


    df_final = pd.concat(liste_dfs, ignore_index=True)
    
    # Affichage du récapitulatif final
    print("\n--- ✅ Récapitulatif de chargement par Année Universitaire ---")
    if fichiers_charges_par_annee:
        for annee, fichiers in sorted(fichiers_charges_par_annee.items()):
            print(f"  * {annee} ({len(fichiers)} fichiers chargés)")
    else:
        print("  * Aucun fichier chargé.")
        
    print(f"\n✅ Total des lignes chargées après combinaison : {len(df_final)}")
    return df_final

# --------------------------------------------------------------------------
# --- Fonctions de Nettoyage Spécifiques ---

def nettoyer_colonnes_texte(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie les colonnes de type texte (suppression des espaces et gestion de 'nan')."""
    colonnes_texte = df.select_dtypes(include=['object']).columns

    print("\n--- 🧹 Nettoyage Général des Colonnes Texte ---")
    for col in tqdm(colonnes_texte, desc="Suppression des espaces (strip)"):
        df[col] = df[col].astype(str).str.replace('nan', '', regex=False).str.strip() 
        # Remplace les chaînes vides résultantes par la valeur manquante standard de Pandas
        # NOTE: Nous utilisons pd.NA ici pour le nettoyage général.
        df.loc[df[col] == '', col] = pd.NA 
    
    print("✅ Espaces en début/fin et chaînes 'nan' traités.")
    return df

def traiter_annee_universitaire(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assure le type et gère les NaN pour la colonne 'annee_universitaire'.
    Suppression de tous les espaces internes pour le hachage.
    """
    print("\n--- 🎓 Traitement de l'Année Universitaire ---")

    if 'annee_universitaire' in df.columns:
        # Convertir en chaîne, supprimer les espaces de bord et mettre en majuscule
        df['annee_universitaire'] = df['annee_universitaire'].astype(str).str.strip().str.upper()
        
        # Supprimer TOUS les espaces internes pour l'uniformité du hachage
        df['annee_universitaire'] = df['annee_universitaire'].apply(
            lambda x: re.sub(r'\s+', '', str(x)) if pd.notna(x) else x
        ).replace('NAN', pd.NA).replace('', pd.NA) # Nettoyer les 'NAN' et les chaînes vides restantes
        
        # Gestion des NaN
        condition_nan_ou_vide = df['annee_universitaire'].isna()
        df.loc[condition_nan_ou_vide, 'annee_universitaire'] = pd.NA
        
        df['annee_universitaire'] = df['annee_universitaire'].convert_dtypes()
        print("✅ Colonne 'annee_universitaire' nettoyée, formatée et uniformisée (tous espaces supprimés).")
    else:
        print("⚠️ Colonne 'annee_universitaire' non trouvée.")
        
    return df

def traiter_annee_bac(df: pd.DataFrame) -> pd.DataFrame:
    """Convertit la colonne 'bacc_annee' en entier (nullable), gérant les erreurs."""
    print("\n--- 🎓 Traitement de l'Année du BAC (bacc_annee) ---")
    
    col = 'bacc_annee'
    if col in df.columns:
        # 1. Nettoyer les chaînes (s'assurer qu'il n'y a pas d'espaces de bord)
        df[col] = df[col].astype(str).str.strip()

        # 2. Convertir en numérique (les valeurs non-numériques deviennent NaN)
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 3. Convertir en type entier nullable de Pandas (Int64)
        df[col] = df[col].convert_dtypes()
        
        print(f"✅ Colonne '{col}' convertie en entier (Int64).")
    else:
        print(f"⚠️ Colonne '{col}' non trouvée. Traitement ignoré.")
        
    return df

def nettoyer_bacc_numero(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie la colonne 'bacc_numero'. Ne conserve que les valeurs composées de 7 chiffres exacts,
    sinon remplace par NA.
    """
    col = 'bacc_numero'
    print(f"\n--- 🔢 Nettoyage de la Colonne '{col}' (7 chiffres requis) ---")
    
    if col in df.columns:
        # 1. Copie de la colonne pour le nettoyage
        bacc_num_clean = df[col].astype(str).str.replace(r'[^\d]', '', regex=True)
        
        # 2. Condition : doit avoir exactement 7 chiffres
        condition_valide = bacc_num_clean.str.len() == 7
        
        # 3. Application du nettoyage
        df[col] = np.where(condition_valide, bacc_num_clean, pd.NA)
        
        # 4. Finalisation du type
        df[col] = df[col].convert_dtypes()
        
        val_nulles_apres = df[col].isna().sum()
        print(f"✅ Colonne '{col}' nettoyée. Les valeurs n'ayant pas 7 chiffres exacts ont été mises à NA (Total NA: {val_nulles_apres}).")
    else:
        print(f"⚠️ Colonne '{col}' non trouvée. Traitement ignoré.")
        
    return df

def traiter_naissance_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Traite 'naissance_date': extrait l'année des mentions textuelles ("vers 1990")
    et décompose la date nettoyée en Année, Mois, Jour.
    """
    print("\n--- 📅 Traitement de la Date de Naissance ('naissance_date') ---")
    if 'naissance_date' in df.columns:
        # Gère les cas "vers 1990" en extrayant l'année
        df['annee_vers'] = df['naissance_date'].astype(str).str.extract(r'vers\s*(\d{4})', flags=re.IGNORECASE).astype('float')

        # Convertit la colonne de date principale, en gérant les erreurs
        df['naissance_date_clean'] = pd.to_datetime(
            df['naissance_date'],
            errors='coerce',
            dayfirst=True 
        )

        df['naissance_annee'] = df['naissance_date_clean'].dt.year.astype('float')
        df['naissance_mois'] = df['naissance_date_clean'].dt.month.astype('float')
        df['naissance_jour'] = df['naissance_date_clean'].dt.day.astype('float')
        
        # Impute l'année à partir de 'vers XXXX' si la conversion principale a échoué
        condition_vers = df['naissance_annee'].isna() & df['annee_vers'].notna()
        df.loc[condition_vers, 'naissance_annee'] = df.loc[condition_vers, 'annee_vers']

        df['naissance_date'] = df['naissance_date_clean'] # Mettre la date standardisée
        
        # Conversion des types pour une meilleure gestion des NaN (Int/String nullable)
        df['naissance_annee'] = df['naissance_annee'].convert_dtypes()
        df['naissance_mois'] = df['naissance_mois'].convert_dtypes()
        df['naissance_jour'] = df['naissance_jour'].convert_dtypes()

        df = df.drop(columns=['annee_vers', 'naissance_date_clean'], errors='ignore')
        print("✅ Colonnes de date de naissance (Date, Année, Mois, Jour) nettoyées et mises à jour.")
    else:
        print("⚠️ Colonne 'naissance_date' non trouvée.")
    return df

def traiter_cin_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Traite 'cin_date': conversion stricte en date (dayfirst=True). 
    Toute valeur non identifiable est mise à NA.
    """
    print("\n--- 📅 Traitement de la Date de Délivrance CIN ('cin_date') ---")
    
    if 'cin_date' in df.columns:
        # Convertit la colonne en date. 'errors='coerce'' met NaT (qui devient pd.NA) si non identifiable.
        df['cin_date'] = pd.to_datetime(
            df['cin_date'],
            errors='coerce', # <-- Met NaT si la date est non identifiable
            dayfirst=True 
        )
        # S'assurer que le type est bien datetime nullable
        df['cin_date'] = df['cin_date'].convert_dtypes() 
        
        val_nulles_apres = df['cin_date'].isna().sum()
        print(f"✅ Colonne 'cin_date' nettoyée et convertie en date. Les valeurs non identifiables sont maintenant NA (Total NA: {val_nulles_apres}).")
    else:
        print("⚠️ Colonne 'cin_date' non trouvée.")
        
    return df

def standardiser_sexe(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise la colonne 'sexe' en 'Féminin' ou 'Masculin', en conservant les valeurs manquantes."""
    print("\n--- ♀️♂️ Standardisation de la Colonne 'sexe' ---")
    
    if 'sexe' in df.columns:
        col_sexe = df['sexe'].astype(str).str.upper().str.strip()
        col_sexe = col_sexe.replace('NAN', pd.NA)

        # Assumer que toutes les valeurs non vides par défaut sont Masculin, sauf correction
        df['sexe_standard'] = np.where(col_sexe.notna(), 'Masculin', col_sexe)
        
        # Correction pour les cas féminins (F, FÉMININ, etc.)
        condition_feminin = col_sexe.str.contains(r'F|FÉMININ', na=False) 
        df.loc[condition_feminin, 'sexe_standard'] = 'Féminin'
        
        df['sexe'] = df['sexe_standard'].convert_dtypes()
        df = df.drop(columns=['sexe_standard'], errors='ignore')
        
        print("✅ Colonne 'sexe' standardisée.")
    else:
        print("⚠️ Colonne 'sexe' non trouvée.")
        
    return df

def traiter_formation_hybride(df: pd.DataFrame) -> pd.DataFrame:
    """Utilise la colonne 'hybride' (C ou H) pour renseigner la colonne 'formation'."""
    print("\n--- 🔄 Renseignement de 'formation' par 'hybride' ---")
    
    if 'hybride' in df.columns and 'formation' in df.columns:
        col_hybride = df['hybride'].astype(str).str.upper().str.strip()
        
        condition_c = col_hybride == 'C'
        df.loc[condition_c, 'formation'] = 'CLASSIQUE'
        
        condition_h = col_hybride == 'H'
        df.loc[condition_h, 'formation'] = 'HYBRIDE'
        
        df = df.drop(columns=['hybride'], errors='ignore')
        
        df['formation'] = df['formation'].convert_dtypes()

        print("✅ Colonne 'formation' mise à jour en fonction de la colonne 'hybride'.")
    else:
        print("⚠️ Colonne 'hybride' ou 'formation' manquante. Traitement ignoré.")
        
    return df

def ajouter_colonnes_institutionnelles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajoute les colonnes institutionnelles constantes (ID, Nom, Type)
    pour toutes les lignes du DataFrame, correspondant à l'Université de Fianarantsoa.
    """
    print("\n--- 🏢 Ajout des Colonnes Institutionnelles (Université de Fianarantsoa) ---")

    # Définition des valeurs constantes
    INSTITUTION_ID = 'UNIV_FIANARA'
    INSTITUTION_NOM = 'Université de Fianarantsoa'
    INSTITUTION_TYPE = 'PUBLIQUE'

    # Création et remplissage des colonnes pour toutes les lignes
    df['institution_id'] = INSTITUTION_ID
    df['institution_nom'] = INSTITUTION_NOM
    df['institution_type'] = INSTITUTION_TYPE
    
    # Conversion des types pour utiliser le StringDtype (nullable string)
    df['institution_id'] = df['institution_id'].convert_dtypes()
    df['institution_nom'] = df['institution_nom'].convert_dtypes()
    df['institution_type'] = df['institution_type'].convert_dtypes()

    print(f"✅ Colonnes institutionnelles créées : ID={INSTITUTION_ID}, Nom={INSTITUTION_NOM}, Type={INSTITUTION_TYPE}.")
    return df

def imputer_id_parcours(df: pd.DataFrame) -> pd.DataFrame:
    """Impute les valeurs manquantes de 'id_Parcours' par concaténation: composante_mention_parcours."""
    print("\n--- 🧩 Imputation de 'id_Parcours' ---")

    colonnes_requises = ['id_Parcours', 'composante', 'mention', 'parcours']
    if not all(col in df.columns for col in colonnes_requises):
        print("⚠️ Une ou plusieurs colonnes requises sont manquantes. Traitement ignoré.")
        return df

    condition_manquant = df['id_Parcours'].isna() 

    sources = df.loc[condition_manquant, ['composante', 'mention', 'parcours']].copy()
    
    # Prétraitement des sources pour la concaténation
    sources = sources.fillna('').astype(str).apply(lambda x: x.str.upper().str.strip())
    
    # Concaténation
    nouveaux_ids = sources['composante'] + '_' + sources['mention'] + '_' + sources['parcours']
    
    # Supprime les IDs inutiles comme "__" ou "_"
    nouveaux_ids = nouveaux_ids.str.replace(r'(_+)', '_', regex=True).str.strip('_').replace('', pd.NA)

    df.loc[condition_manquant, 'id_Parcours'] = nouveaux_ids
    
    df['id_Parcours'] = df['id_Parcours'].convert_dtypes()

    lignes_imputees = condition_manquant.sum()
    print(f"✅ {lignes_imputees} valeurs 'id_Parcours' imputées par concaténation.")
    
    return df

def nettoyer_et_formater_cin(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie le CIN, extrait les 12 premiers chiffres trouvés (même au milieu d'un texte), 
    et le formate en 'XXX-XXX-XXX-XXX'. Sinon met NA.
    """
    print("\n--- 🆔 Nettoyage et Formatage Robuste du CIN ---")
    
    if 'cin' in df.columns:
        
        # 1. Nettoyage : retirer les caractères non numériques
        df['cin_clean'] = df['cin'].astype(str).str.replace(r'[^\d]', '', regex=True)
        
        # 2. Extraction des 12 premiers chiffres
        df['cin_extrait'] = df['cin_clean'].str[:12]
        
        # 3. Validation et Formatage
        def formater_cin_tiret(chaine):
            # La validation : si la chaîne n'a pas 12 caractères, elle est invalide
            if pd.isna(chaine) or len(chaine) != 12:
                return pd.NA
            
            # Formater en groupes de trois séparés par un tiret
            return f"{chaine[0:3]}-{chaine[3:6]}-{chaine[6:9]}-{chaine[9:12]}"

        # 4. Application de la fonction
        df['cin'] = df['cin_extrait'].apply(formater_cin_tiret)
        
        # 5. Finalisation
        df['cin'] = df['cin'].convert_dtypes()
        df = df.drop(columns=['cin_clean', 'cin_extrait'], errors='ignore')
        
        val_nulles = df['cin'].isna().sum()
        print(f"✅ Colonne 'cin' nettoyée et formatée. {val_nulles} valeurs ont été mises à NA car moins de 12 chiffres trouvés.")
    else:
        print("⚠️ Colonne 'cin' non trouvée. Traitement ignoré.")
        
    return df

def nettoyer_et_formater_telephone(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie la colonne 'telephone' : supprime les préfixes internationaux,
    normalise à 10 chiffres (en ajoutant '0' si 9 chiffres) et formate en '0XX XX XXX XX', sinon met NA.
    """
    col_name = None
    if 'telephone' in df.columns:
        col_name = 'telephone'
    elif 'tel' in df.columns:
        col_name = 'tel'
    
    print("\n--- 📞 Nettoyage et Formatage du Numéro de Téléphone ---")

    if col_name:
        
        # 1. Nettoyage : retirer tous les caractères non numériques
        df['tel_clean'] = df[col_name].astype(str).str.replace(r'[^\d]', '', regex=True)
        
        # 2. Gestion du préfixe international (+261 ou 261) et formatage
        def normaliser_numero(chaine):
            if pd.isna(chaine) or not chaine:
                return pd.NA
            
            chaine_locale = chaine
            # Suppression du préfixe international 261
            if chaine.startswith('261'):
                chaine_locale = chaine[3:] 
            
            # Validation et Normalisation à 10 chiffres
            if len(chaine_locale) == 9:
                numero_normalise = '0' + chaine_locale
            elif len(chaine_locale) == 10:
                numero_normalise = chaine_locale
            else:
                return pd.NA
                
            # Formater : 0XX XX XXX XX
            return f"{numero_normalise[0:3]} {numero_normalise[3:5]} {numero_normalise[5:8]} {numero_normalise[8:10]}"
            
        # 3. Application de la fonction
        df[col_name] = df['tel_clean'].apply(normaliser_numero)
        
        # 4. Finalisation
        df[col_name] = df[col_name].convert_dtypes()
        df = df.drop(columns=['tel_clean'], errors='ignore')
        
        val_nulles = df[col_name].isna().sum()
        print(f"✅ Colonne '{col_name}' nettoyée, normalisée et formatée. {val_nulles} valeurs mises à NA.")
    else:
        print("⚠️ Colonne 'telephone' ou 'tel' non trouvée. Traitement ignoré.")
        
    return df

def nettoyer_et_formater_num_inscription(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardise la colonne 'numero_inscription' (priorité), 'num_inscription' ou 'inscription' :
    - Supprime les espaces et les caractères de séparation courants.
    - Met en majuscule.
    - **Applique le préfixe 'mention' UNIQUEMENT si le numéro d'inscription est NON VIDE après nettoyage.**
    - Conserve les valeurs nulles initiales (NaN, NA) comme telles.
    """
    col_ni = None
    
    # 1. Détermination de la colonne d'inscription
    if 'numero_inscription' in df.columns:
        col_ni = 'numero_inscription'
    elif 'num_inscription' in df.columns:
        col_ni = 'num_inscription'
    elif 'inscription' in df.columns:
        col_ni = 'inscription'
        
    print("\n--- 📝 Standardisation du Numéro d'Inscription (Règle Stricte) ---")
    
    if col_ni:
        # --- A. Préparation du Préfixe Mention ---
        prefixe_col = 'mention'
        if prefixe_col not in df.columns:
            print(f"⚠️ Colonne '{prefixe_col}' manquante. Le préfixage par mention est ignoré.")
            # Créer une série vide pour éviter l'erreur de référence
            mention_prefixe = pd.Series([''] * len(df), index=df.index)
        else:
            # Nettoyage et préparation du préfixe
            mention_prefixe = df[prefixe_col].astype(str).str.upper().str.strip()
            # Remplacer les NaN/NULL par des chaînes vides pour la concaténation
            mention_prefixe = mention_prefixe.replace('NAN', '').fillna('') 
            # Ajouter le '_' uniquement si la mention existe et n'est pas vide
            mention_prefixe = mention_prefixe.apply(lambda x: x + '_' if x else '')
            

        # --- B. Nettoyage du Numéro d'Inscription ---
        # On travaille sur une copie temporaire pour la manipulation des valeurs non nulles
        temp_ni = df[col_ni].copy()
        
        # 1. Identification des valeurs qui NE SONT PAS NaN/NA
        condition_non_vide = temp_ni.notna() 
        
        # 2. Nettoyage des valeurs non-vides: Majuscule, strip et suppression des caractères spéciaux
        temp_ni.loc[condition_non_vide] = temp_ni.loc[condition_non_vide].astype(str).str.upper().str.strip()
        
        # Suppression des séparateurs (espaces, tirets, etc.)
        temp_ni.loc[condition_non_vide] = temp_ni.loc[condition_non_vide].str.replace(r'[\s\-\/\.]', '', regex=True)

        # Remplacer les chaînes 'NAN' restantes par des chaînes vides
        temp_ni.loc[condition_non_vide] = temp_ni.loc[condition_non_vide].replace('NAN', '')
        
        # 3. Retirer les valeurs qui sont devenues vides suite au nettoyage (afin qu'elles ne soient pas préfixées)
        condition_non_vide_apres_nettoyage = temp_ni != ''
        
        # 4. Application du préfixe mention UNIQUEMENT aux numéros NON vides après nettoyage
        df[col_ni] = np.where(
            condition_non_vide & condition_non_vide_apres_nettoyage, 
            mention_prefixe + temp_ni.astype(str), 
            temp_ni # Conserve la valeur originale (y compris NaN/NA) si elle est vide ou nulle
        )
        
        # 5. Finalisation du type
        # Convertir en StringDtype pour maintenir les vrais NA de Pandas si présents.
        df[col_ni] = df[col_ni].convert_dtypes() 
        
        print(f"✅ Colonne '{col_ni}' standardisée. Préfixage par 'mention' appliqué uniquement aux numéros d'inscription non vides.")
    else:
        print("⚠️ Colonne 'numero_inscription', 'num_inscription' ou 'inscription' non trouvée. Traitement ignoré.")
        
    return df

# --- Nouvelle Fonction pour le Semestre ---

def traiter_colonne_semestre(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée les 16 colonnes binaires S01 à S16 basées sur la colonne 'semestre'.
    Impute les valeurs manquantes en fonction de la colonne 'niveau'.
    """
    print("\n--- 📚 Création des Colonnes Binaires de Semestre (S01-S16) ---")

    if 'semestre' not in df.columns or 'niveau' not in df.columns:
        print("⚠️ Colonnes 'semestre' ou 'niveau' manquantes. Traitement ignoré.")
        return df

    # 1. Initialisation des 16 colonnes binaires à 0 (type entier nullable)
    semestre_cols = [f'S{i:02d}' for i in range(1, 17)]
    for col in semestre_cols:
        df[col] = pd.Series(0, dtype='Int64')

    # 2. Traitement des valeurs existantes dans 'semestre'
    
    # 2.1 Nettoyage et standardisation du contenu (S1, S2, S3 et S5, s1-s2, etc.)
    semestre_propre = df['semestre'].astype(str).str.upper().str.strip().replace('NAN', '')
    
    # Remplacer les séparateurs non-alphanumériques par un seul séparateur (par exemple, un espace)
    # Ceci transforme 'S1 et S2', 'S3, S5', 'S1-S2' en 'S1 S2', 'S3 S5', 'S1 S2' (après le nettoyage de 'S')
    semestre_propre = semestre_propre.str.replace(r'[^A-Z0-9]', ' ', regex=True)
    
    # Remplacer S suivi de 1 ou 2 chiffres
    # On isole chaque semestre (ex: 'S1 S2' devient ['S1', 'S2'])
    def extraire_semestres(valeur):
        if pd.isna(valeur) or not valeur:
            return set()
        
        # Trouver toutes les occurrences de S suivi d'un ou deux chiffres
        matches = re.findall(r'S(\d{1,2})', valeur, re.IGNORECASE)
        
        # Convertir en numéros et garantir qu'ils sont entre 1 et 16
        semestres = {int(s) for s in matches if 1 <= int(s) <= 16}
        return semestres

    df['semestres_identifies'] = semestre_propre.apply(extraire_semestres)

    # 2.2 Remplissage des colonnes binaires
    for i in range(1, 17):
        col_name = f'S{i:02d}'
        # Si 'i' est dans l'ensemble de semestres identifiés, mettre 1
        df.loc[df['semestres_identifies'].apply(lambda s: i in s), col_name] = 1

    # 3. Imputation par 'niveau' pour les lignes où AUCUN semestre n'a été identifié (colonne 'semestre' était vide ou illisible)
    condition_pas_de_semestre = df['semestres_identifies'].apply(len) == 0

    # Mapping Niveau -> Semestres à remplir (basé sur votre règle : L1 -> S01, S02, etc.)
    mapping_niveau_semestre = {
        'L1': [1, 2], 'L2': [3, 4], 'L3': [5, 6], 
        'M1': [7, 8], 'M2': [9, 10], 
        'D1': [11, 12], 'D2': [13, 14], 'D3': [15, 16]
    }
    
    # Nettoyage et standardisation de la colonne 'niveau'
    niveau_propre = df['niveau'].astype(str).str.upper().str.strip().replace('NAN', '')
    
    lignes_imputees = 0
    for niveau, semestres in mapping_niveau_semestre.items():
        # Condition : Pas de semestre trouvé ET Niveau correspond
        condition_imputation = condition_pas_de_semestre & (niveau_propre == niveau)
        
        if condition_imputation.any():
            lignes_imputees += condition_imputation.sum()
            for i in semestres:
                col_name = f'S{i:02d}'
                df.loc[condition_imputation, col_name] = 1

    print(f"✅ Colonnes binaires S01 à S16 créées et remplies. {lignes_imputees} lignes imputées par le niveau.")
    
    # Suppression de la colonne temporaire
    df = df.drop(columns=['semestres_identifies'], errors='ignore')
    
    return df

# --------------------------------------------------------------------------
# --- Fonction Orchestratrice Principale ---

def nettoyer_donnees(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fonction principale de nettoyage, orchestrant les sous-étapes.
    """
    if df.empty:
        return df

    # Exécution séquentielle des étapes de nettoyage
    
    # Étape 0 : Ajout des colonnes institutionnelles (avant tout nettoyage/imputation)
    df = ajouter_colonnes_institutionnelles(df)
    
    # Étape 1 : Nettoyage général des textes
    df = nettoyer_colonnes_texte(df)
    
    # Nettoyage et uniformisation des années
    df = traiter_annee_universitaire(df) 
    df = traiter_annee_bac(df)
    
    # Nettoyage du numéro de bac
    df = nettoyer_bacc_numero(df) 

    # --- Nettoyage des dates séparé (selon la demande) ---
    df = traiter_naissance_date(df)
    df = traiter_cin_date(df)
    # -----------------------------------------------------

    df = standardiser_sexe(df)
    df = traiter_formation_hybride(df)
    df = imputer_id_parcours(df)
    df = nettoyer_et_formater_cin(df)
    df = nettoyer_et_formater_telephone(df)
    df = nettoyer_et_formater_num_inscription(df)
    
    # Étape Semestre
    df = traiter_colonne_semestre(df)

    return df