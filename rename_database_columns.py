# rename_database_columns.py

import pandas as pd
import config
from typing import Dict, List

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
    # Ceci est un exemple pour tester la fonction
    data = {
        'nom': ['Pierre', 'Marie'], 
        'sexe': ['M', 'F'], 
        'matricule': [1, 2] # Colonne non dans les règles
    }
    df_test = pd.DataFrame(data)
    
    print("DataFrame original:\n", df_test)
    
    df_resultat = renommer_colonnes_df(df_test.copy(), config.COLONNES_RENOMMAGE)
    
    print("\nDataFrame après renommage:\n", df_resultat)