import os
import pandas as pd
import numpy as np

# --- Importation des Modules ---
try:
    import config
    # Module pour le nettoyage des champs (standardisation, formatage)
    from data_cleaner import (
        charger_et_combiner_fichiers,
        nettoyer_donnees 
    )
    from student_code_manager import gerer_code_etudiant_et_consolider 
    from inscription_semestre_code_manager import gerer_code_inscription_par_semestre
    
    # NOUVEL IMPORT : Module pour le renommage des colonnes
    from rename_database_columns import renommer_colonnes_df
    
    print("✅ Configuration et tous les gestionnaires de données importés.")
except ImportError as e:
    print(f"❌ Erreur d'importation : {e}")
    print("Veuillez vérifier que 'config.py', 'data_cleaner.py', 'student_code_manager.py', 'inscription_semestre_code_manager.py' et 'rename_database_columns.py' sont présents et accessibles.")
    exit()

# --- Fonctions d'Exportation ---

def exporter_et_renommer(df_final: pd.DataFrame):
    """
    Étape 6 : Exporte le DataFrame final KEYED puis le renomme et l'exporte à nouveau.
    """
    # Assurer l'existence du dossier de sortie
    if not os.path.exists(config.DOSSIER_SORTIE):
        os.makedirs(config.DOSSIER_SORTIE)
        print(f"\n📂 Création du dossier de sortie : {config.DOSSIER_SORTIE}")
    
    # Sécurité : ne garder que les colonnes attendues dans le bon ordre
    colonnes_a_exporter = [col for col in config.COLONNES_ATTENDUES if col in df_final.columns]
    df_export = df_final[colonnes_a_exporter].copy()
    
    # --- Étape 6.1 : Exportation KEYED (Noms du pipeline) ---
    chemin_keyed = os.path.join(config.DOSSIER_SORTIE, config.FICHIER_SORTIE_NETTOYEE)
    try:
        df_export.to_excel(chemin_keyed, index=False)
        print("\n--- 6.1 EXPORTATION KEYED RÉUSSIE ---")
        print(f"🎉 Fichier KEYED exporté à : **{chemin_keyed}**")
        print(f"Taille : **{len(df_export)} lignes**.")
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exportation du fichier KEYED : {e}")
        return # Arrêter si l'exportation échoue

    # --- Étape 6.2 : Renommage et Exportation FINALE (Nouveaux noms) ---
    print("\n--- 6.2 RENOMMAGE ET EXPORTATION FINALE ---")
    
    # Application du renommage
    # Utilisation de config.COLONNES_RENOMMAGE défini dans config.py
    df_final_renomme = renommer_colonnes_df(df_export.copy(), config.COLONNES_RENOMMAGE)
    
    # Assurer que toutes les colonnes qui devaient être renommées sont là, sinon l'exportation échouerait
    # On utilise toutes les colonnes du DataFrame renommé
    
    chemin_final = os.path.join(config.DOSSIER_SORTIE, config.FICHIER_SORTIE_RENOMMEE)
    
    try:
        df_final_renomme.to_excel(chemin_final, index=False)
        print("\n==================================================")
        print("🎉 Succès ! Données FINALES exportées à :")
        print(f"➡️ **{chemin_final}**")
        print(f"Taille du jeu de données final : **{len(df_final_renomme)} lignes**, {len(df_final_renomme.columns)} colonnes.")
        print("==================================================")
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exportation du fichier FINAL : {e}")


# --- Fonction Principale d'Exécution ---

def main():
    """
    Fonction principale pour exécuter le pipeline de chargement, nettoyage et codification.
    """
    print("==================================================")
    print("🚀 Démarrage du Pipeline de Traitement de Données 🎓")
    print("==================================================")

    # 1. Chargement et combinaison des données brutes
    print("\n\n--- ÉTAPE 1/4 : CHARGEMENT ET COMBINAISON ---")
    df_brut = charger_et_combiner_fichiers(
        dossier_path=config.DOSSIER_PATH,
        filtre_2023=config.NOM_FILTRE_2023,
        filtre_2024=config.NOM_FILTRE_2024,
        filtre_2025=config.NOM_FILTRE_2025 
    )

    if df_brut.empty:
        print("❌ Le traitement est arrêté car aucune donnée n'a été chargée.")
        return
    
    print(f"✅ Total des lignes brutes chargées : {len(df_brut)}")

    # 2. Nettoyage des données (champs)
    print("\n\n--- ÉTAPE 2/4 : EXÉCUTION DU NETTOYAGE DES CHAMPS (data_cleaner) ---")
    df_nettoye = nettoyer_donnees(df_brut.copy()) 
    
    print(f"\n✅ Total des lignes après nettoyage des champs : {len(df_nettoye)}")
    
    # 3. Gestion des Codes Étudiants et Consolidation
    print("\n\n--- ÉTAPE 3/4 : CRÉATION DU CODE ÉTUDIANT ET CONSOLIDATION (student_code_manager) ---")
    df_intermediaire = gerer_code_etudiant_et_consolider(df_nettoye.copy()) 
    
    print(f"\n✅ Total des lignes après gestion des codes étudiants : {len(df_intermediaire)}") 
    
    # 4. Gestion des Codes d'Inscription par Semestre et Suppression des Doublons
    print("\n\n--- ÉTAPE 4/4 : CRÉATION DU CODE INSCRIPTION PAR SEMESTRE ET SUPPRESSION DES DOUBLONS ---")
    df_final = gerer_code_inscription_par_semestre(df_intermediaire.copy()) 

    # 5. Finalisation et Double Exportation (KEYED puis DATAS)
    print("\n\n--- ÉTAPE 5 : FINALISATION ET DOUBLE EXPORTATION ---")
    exporter_et_renommer(df_final)
    
    print("\n==================================================")
    print("Pipeline terminé. Le fichier final est prêt.")
    print("==================================================")

if __name__ == "__main__":
    main()