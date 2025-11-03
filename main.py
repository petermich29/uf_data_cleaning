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
    # CORRECTION APPLIQUÉE : On revient au nom de fonction attendu 'gerer_code_etudiant_et_consolider'
    from student_code_manager import gerer_code_etudiant_et_consolider 
    # Module pour la création du code_inscription et la suppression des doublons
    from inscription_code_manager import gerer_code_inscription_et_supprimer_doublons
    
    print("✅ Configuration et tous les gestionnaires de données importés.")
except ImportError as e:
    print(f"❌ Erreur d'importation : {e}")
    print("Veuillez vérifier que 'config.py', 'data_cleaner.py', 'student_code_manager.py' et 'inscription_code_manager.py' sont présents et accessibles.")
    exit()

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
        filtre_2024=config.NOM_FILTRE_2024
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
    # Cette fonction continue d'utiliser l'algorithme de hachage pour créer des clés internes
    df_intermediaire = gerer_code_etudiant_et_consolider(df_nettoye.copy(), config.HASH_ALGORITHM) 
    
    # Le nombre de lignes n'a pas changé à cette étape, seules les colonnes code_etudiant et les champs ont été consolidés.
    print(f"\n✅ Total des lignes après gestion des codes étudiants : {len(df_intermediaire)}") 
    
    # 4. Gestion des Codes d'Inscription et Suppression des Doublons
    print("\n\n--- ÉTAPE 4/4 : CRÉATION DU CODE INSCRIPTION ET SUPPRESSION DES DOUBLONS (inscription_code_manager) ---")
    # CORRECTION APPLIQUÉE : Suppression de l'argument 'config.HASH_ALGORITHM'
    df_final = gerer_code_inscription_et_supprimer_doublons(df_intermediaire.copy()) 

    # 5. Finalisation et Exportation
    
    print("\n\n--- FINALISATION ET EXPORTATION ---")

    # Sécurité : ne garder que les colonnes attendues dans le bon ordre
    colonnes_a_exporter = [col for col in config.COLONNES_ATTENDUES if col in df_final.columns]
    
    df_export = df_final[colonnes_a_exporter].copy()
    
    # Assurer l'existence du dossier de sortie
    if not os.path.exists(config.DOSSIER_SORTIE):
        os.makedirs(config.DOSSIER_SORTIE)
        print(f"\n📂 Création du dossier de sortie : {config.DOSSIER_SORTIE}")

    # Chemin du fichier de sortie
    chemin_sortie = os.path.join(config.DOSSIER_SORTIE, config.FICHIER_SORTIE_NETTOYEE)

    # Exportation
    try:
        df_export.to_excel(chemin_sortie, index=False)
        print("\n==================================================")
        print(f"🎉 Succès ! Données nettoyées et codées exportées à :")
        print(f"➡️ **{chemin_sortie}**")
        print(f"Taille du jeu de données final : **{len(df_export)} lignes**, {len(df_export.columns)} colonnes.")
        print("==================================================")
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exportation du fichier : {e}")


if __name__ == "__main__":
    main()