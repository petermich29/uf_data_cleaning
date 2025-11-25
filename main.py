import os
import pandas as pd
import numpy as np
import shutil  # Pour la copie de fichiers

# --- Importation des Modules ---
try:
    import config
    # Module pour le chargement et le nettoyage des champs (standardisation, formatage)
    from data_cleaner import (
        charger_et_combiner_fichiers,
        nettoyer_donnees 
    )
    # Gestion des codes étudiants et consolidation
    from student_code_manager import gerer_code_etudiant_et_consolider
    # Gestion des codes d’inscription par semestre et suppression des doublons
    from inscription_semestre_code_manager import gerer_code_inscription_par_semestre
    # Renommage des colonnes pour la base finale
    from rename_database_columns import renommer_colonnes_df, preparer_nom_prenom

    print("✅ Configuration et tous les gestionnaires de données importés.")
except ImportError as e:
    print(f"❌ Erreur d'importation : {e}")
    print("Veuillez vérifier que 'config.py', 'data_cleaner.py', "
          "'student_code_manager.py', 'inscription_semestre_code_manager.py' "
          "et 'rename_database_columns.py' sont présents et accessibles.")
    exit()


# ==================================================
# FONCTION D’EXPORTATION (KEYED + RENOMMÉ + COPIE)
# ==================================================
def exporter_et_renommer(df_final: pd.DataFrame):
    """
    Étape finale :
      1) Exporte le DataFrame final en version KEYED (noms de colonnes pipeline)
      2) Applique le traitement des noms/prénoms, renomme les colonnes pour la base et exporte la version DATAS
      3) Copie le fichier final (_UFALLTIME_DATAS.xlsx) dans le dossier principal POWERQUERY
    """
    
    # Assurez-vous que les dépendances (os, shutil, config, etc.) et les fonctions 
    # (preparer_nom_prenom, renommer_colonnes_df) sont importées.
    
    # 0. Assurer l'existence du dossier de sortie
    if not os.path.exists(config.DOSSIER_SORTIE):
        os.makedirs(config.DOSSIER_SORTIE)
        print(f"\n📂 Création du dossier de sortie : {config.DOSSIER_SORTIE}")
    
    # 1. Sécurité : ne garder que les colonnes attendues dans le bon ordre
    colonnes_a_exporter = [col for col in config.COLONNES_ATTENDUES if col in df_final.columns]
    df_export = df_final[colonnes_a_exporter].copy()

    # 2. Exportation KEYED (colonnes pipeline)
    chemin_keyed = os.path.join(config.DOSSIER_SORTIE, config.FICHIER_SORTIE_NETTOYEE)
    try:
        df_export.to_excel(chemin_keyed, index=False)
        print("\n--- 6.1 EXPORTATION KEYED RÉUSSIE ---")
        print(f"🎉 Fichier KEYED exporté à : {chemin_keyed}")
        print(f"Taille : {len(df_export)} lignes, {len(df_export.columns)} colonnes.")
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exportation du fichier KEYED : {e}")
        return  # On arrête si l’export KEYED échoue

    # 3. Traitement, Renommage et Exportation finale (version DATAS)
    print("\n--- 6.2 TRAITEMENT, RENOMMAGE ET EXPORTATION FINALE (DATAS) ---")
    
    # 3.1. Préparez le DataFrame pour le renommage en copiant df_export
    df_a_traiter = df_export.copy()
    
    # 3.2. ✅ APPEL DE LA FONCTION DE TRAITEMENT DES NOMS ET PRÉNOMS
    # Cette étape standardise les noms avant le renommage des colonnes.
    df_apres_traitement = preparer_nom_prenom(df_a_traiter)

    # 3.3. Renommage des colonnes pour la base (version DATAS)
    df_final_renomme = renommer_colonnes_df(df_apres_traitement, config.COLONNES_RENOMMAGE)

    chemin_final = os.path.join(config.DOSSIER_SORTIE, config.FICHIER_SORTIE_RENOMMEE)
    try:
        df_final_renomme.to_excel(chemin_final, index=False)
        print("\n==================================================")
        print("🎉 Succès ! Données FINALES (DATAS) exportées à :")
        print(f"➡️ {chemin_final}")
        print(f"Taille du jeu de données final : {len(df_final_renomme)} lignes, "
              f"{len(df_final_renomme.columns)} colonnes.")
        print("==================================================")
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exportation du fichier FINAL (DATAS) : {e}")
        return  # On arrête si l’export DATAS échoue

    # 4. Copie du fichier DATAS vers le dossier principal POWERQUERY
    print("\n--- 6.3 COPIE DU FICHIER FINAL VERS LE DOSSIER PRINCIPAL POWERQUERY ---")

    source_path = chemin_final  # fichier dans sortie_nettoyage
    destination_path = os.path.join(config.DOSSIER_PATH, config.FICHIER_SORTIE_RENOMMEE)

    try:
        if os.path.exists(source_path):
            shutil.copy2(source_path, destination_path)
            print("📂 Copie du fichier final réussie :")
            print(f"   Source      : {source_path}")
            print(f"   Destination : {destination_path}")
        else:
            print(f"⚠️ Fichier source introuvable pour la copie : {source_path}")
    except Exception as e:
        print(f"⚠️ Erreur lors de la copie du fichier final : {e}")


# ==================================================
# FONCTION PRINCIPALE D’EXÉCUTION DU PIPELINE
# ==================================================
def main():
    """
    Fonction principale pour exécuter le pipeline de chargement,
    nettoyage, codification et exportation des données.
    """
    print("==================================================")
    print("🚀 Démarrage du Pipeline de Traitement de Données 🎓")
    print("==================================================")

    # 1. Chargement et combinaison des données brutes
    print("\n\n--- ÉTAPE 1/5 : CHARGEMENT ET COMBINAISON DES FICHIERS BRUTS ---")
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
    print("\n\n--- ÉTAPE 2/5 : NETTOYAGE DES CHAMPS (data_cleaner) ---")
    df_nettoye = nettoyer_donnees(df_brut.copy())
    print(f"✅ Total des lignes après nettoyage des champs : {len(df_nettoye)}")

    # 3. Gestion des Codes Étudiants et Consolidation
    print("\n\n--- ÉTAPE 3/5 : CRÉATION DU CODE ÉTUDIANT & CONSOLIDATION (student_code_manager) ---")
    df_intermediaire = gerer_code_etudiant_et_consolider(df_nettoye.copy())
    print(f"✅ Total des lignes après gestion des codes étudiants : {len(df_intermediaire)}")

    # 4. Gestion des Codes d'Inscription par Semestre & Doublons
    print("\n\n--- ÉTAPE 4/5 : CODE INSCRIPTION PAR SEMESTRE & SUPPRESSION DES DOUBLONS ---")
    df_final = gerer_code_inscription_par_semestre(df_intermediaire.copy())
    print(f"✅ Total des lignes finales après gestion des inscriptions : {len(df_final)}")

    # 5. Exportation KEYED, renommage, DATAS + copie vers POWERQUERY
    print("\n\n--- ÉTAPE 5/5 : FINALISATION & DOUBLE EXPORTATION (KEYED + DATAS) ---")
    exporter_et_renommer(df_final)

    print("\n==================================================")
    print("✅ Pipeline terminé. Le fichier final DATAS est prêt et copié dans POWERQUERY.")
    print("==================================================")


if __name__ == "__main__":
    main()
