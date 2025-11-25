# rename_database_columns.py

import pandas as pd
import config
from typing import Dict, List

import pandas as pd
from typing import Dict, List
import numpy as np


def preparer_nom_prenom(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique la règle de standardisation des noms et prénoms et supprime les lignes 
    où les deux champs (nom et prenoms) sont vides.

    Règle de standardisation: Si 'nom' est vide/manquant et que 'prenoms' est présent, 
    copie le contenu de 'prenoms' dans 'nom' en MAJUSCULES.

    Args:
        df (pd.DataFrame): Le DataFrame contenant potentiellement les colonnes 'nom' et 'prenoms'.

    Returns:
        pd.DataFrame: Le DataFrame avec la colonne 'nom' mise à jour et les lignes sans nom/prénom supprimées.
    """
    # 1. Vérification de la présence des colonnes
    if 'nom' not in df.columns or 'prenoms' not in df.columns:
        print("Avertissement: Les colonnes 'nom' et/ou 'prenoms' ne sont pas présentes pour la préparation.")
        return df

    # 2. Nettoyage et standardisation préliminaire des types
    # Remplacer NaN par une chaîne vide et convertir en chaîne (pour gérer les None/np.nan)
    df['nom'] = df['nom'].fillna('').astype(str).str.strip()
    df['prenoms'] = df['prenoms'].fillna('').astype(str).str.strip()

    # --- ÉTAPE 1: Standardisation des noms ---
    
    # 3. Définir la condition de modification (Nom vide ET Prénom non vide)
    condition_a_modifier = (df['nom'].str.len() == 0) & (df['prenoms'].str.len() > 0)
    
    nombre_lignes_modifiees = condition_a_modifier.sum()

    if nombre_lignes_modifiees > 0:
        # 4. Appliquer la transformation
        df.loc[condition_a_modifier, 'nom'] = df.loc[condition_a_modifier, 'prenoms'].str.upper()
        print(f"🛠️ Standardisation: **{nombre_lignes_modifiees}** lignes ont eu 'nom' complété par 'prenoms' en majuscule.")
    else:
        print("ℹ️ Standardisation: Aucune ligne nécessitant la transformation 'nom' <- 'prenoms' n'a été trouvée.")
        
    # --- ÉTAPE 2: Suppression des lignes totalement vides ---
    
    # 5. Définir la condition de suppression (Nom vide APRÈS standardisation ET Prénom vide)
    condition_a_supprimer = (df['nom'].str.len() == 0) & (df['prenoms'].str.len() == 0)
    
    nombre_lignes_supprimees = condition_a_supprimer.sum()
    
    if nombre_lignes_supprimees > 0:
        df_nettoye = df[~condition_a_supprimer].copy() # Conserver les lignes qui NE SONT PAS à supprimer
        print(f"🗑️ Suppression: **{nombre_lignes_supprimees}** lignes ont été supprimées car 'nom' et 'prenoms' étaient tous deux vides.")
        return df_nettoye
    else:
        print("ℹ️ Suppression: Aucune ligne n'a été supprimée (toutes les lignes ont au moins un nom ou un prénom).")
        return df

def renommer_colonnes_df(df: pd.DataFrame, regles_renommage: Dict[str, str]) -> pd.DataFrame:
    """
    Renomme les colonnes d'un DataFrame en utilisant un dictionnaire de règles.

    N'effectue le renommage que pour les colonnes qui existent dans le DataFrame.

    Args:
        df (pd.DataFrame): Le DataFrame à renommer.
        regles_renommage (Dict[str, str]): Dictionnaire {ancien_nom: nouveau_nom}.

    Returns:
        pd.DataFrame: Le DataFrame avec les colonnes renommées.
    """
    if df.empty:
        print("Avertissement: Le DataFrame est vide, aucun renommage effectué.")
        return df

    # Filtrer le dictionnaire de renommage pour ne garder que les colonnes présentes
    colonnes_existantes = df.columns.tolist()
    regles_filtrees = {
        ancien_nom: nouveau_nom
        for ancien_nom, nouveau_nom in regles_renommage.items()
        if ancien_nom in colonnes_existantes
    }
    
    if not regles_filtrees:
        print("Avertissement: Aucune colonne à renommer trouvée dans le DataFrame.")
        return df

    # Renommage
    df_renomme = df.rename(columns=regles_filtrees)
    
    # Optionnel: Afficher un résumé des renommages
    # print(f"Colonnes renommées : {list(regles_filtrees.keys())} -> {list(regles_filtrees.values())}")
    print(f"✅ {len(regles_filtrees)} colonnes renommées avec succès.")
    
    return df_renomme

# Exemple d'utilisation (non exécuté si le module est importé)
if __name__ == "__main__":

    df_resultat = renommer_colonnes_df(df_test.copy(), config.COLONNES_RENOMMAGE)
    
    print("\nDataFrame après renommage:\n", df_resultat)