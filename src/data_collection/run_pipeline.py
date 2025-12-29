# src/data_collection/run_pipeline.py
import sys
from pathlib import Path

def run_pipeline():
    """Exécute tout le pipeline de collecte de données"""
    
    print("=" * 60)
    print("🚀 PIPELINE COMPLET DE COLLECTE DE DONNÉES K8S")
    print("=" * 60)
    
    # 1. Téléchargement des datasets Kaggle
    print("\n1. 📥 TÉLÉCHARGEMENT DES DATASETS KAGGLE")
    print("-" * 40)
    
    try:
        from download_dataset import main as download_main
        if not download_main():
            print("❌ Échec du téléchargement")
            return False
    except ImportError as e:
        print(f"❌ Impossible d'importer download_dataset: {e}")
        return False
    
    # 2. Génération de logs simulés
    print("\n\n2. 🎭 GÉNÉRATION DE LOGS SIMULÉS")
    print("-" * 40)
    
    try:
        from generate_simulated_logs import main as generate_main
        generate_main()
    except ImportError as e:
        print(f"⚠️  Impossible de générer les logs simulés: {e}")
        print("   Continuation sans logs simulés...")
    
    # 3. Création du dataset hybride (supposé déjà fait)
    print("\n\n3. 🔗 CRÉATION DU DATASET HYBRIDE")
    print("-" * 40)
    
    hybrid_path = Path("data/processed/hybrid_security_system_dataset.csv")
    if hybrid_path.exists():
        print(f"✅ Dataset hybride déjà créé: {hybrid_path}")
        print(f"   Taille: {hybrid_path.stat().st_size / (1024*1024):.2f} MB")
    else:
        print("⚠️  Dataset hybride non trouvé")
        print("   Exécution de create_hybrid_dataset.py...")
        try:
            from create_hybrid_dataset import create_hybrid_dataset
            create_hybrid_dataset()
        except ImportError as e:
            print(f"❌ Impossible de créer le dataset hybride: {e}")
            return False
    
    # 4. Validation des données
    print("\n\n4. 🔍 VALIDATION DES DONNÉES")
    print("-" * 40)
    
    try:
        from data_validator import main as validate_main
        validate_main()
    except ImportError as e:
        print(f"⚠️  Impossible de valider les données: {e}")
    
    # Résumé final
    print("\n" + "=" * 60)
    print("🎉 PIPELINE DE COLLECTE TERMINÉ AVEC SUCCÈS!")
    print("=" * 60)
    
    # Lister les fichiers produits
    print("\n📁 FICHIERS PRODUITS:")
    
    data_dir = Path("data")
    for item in sorted(data_dir.rglob("*")):
        if item.is_file() and item.suffix in ['.csv', '.parquet', '.json']:
            size_mb = item.stat().st_size / (1024*1024)
            rel_path = item.relative_to(data_dir.parent)
            print(f"   📄 {rel_path} ({size_mb:.2f} MB)")
    
  
    print("\n Prêt à passer à Spark!")

if __name__ == "__main__":
    run_pipeline() 