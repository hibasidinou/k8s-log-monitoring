# src/data_collection/download_dataset.py
import subprocess
import os
import zipfile
from pathlib import Path
import sys

def setup_kaggle():
    """Vérifie et configure Kaggle"""
    try:
        subprocess.run(['kaggle', '--version'], capture_output=True, check=True)
        print("✅ Kaggle CLI est installé")
        return True
    except:
        print("❌ Kaggle CLI non trouvé. Installez-le avec:")
        print("   pip install kaggle")
        print("   Puis configurez votre token API dans ~/.kaggle/kaggle.json")
        return False

def download_kaggle_dataset(dataset_name, output_dir):
    """Télécharge un dataset Kaggle"""
    dataset_dir = Path(output_dir)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📥 Téléchargement: {dataset_name}")
    
    cmd = ['kaggle', 'datasets', 'download', '-d', dataset_name, '-p', str(dataset_dir)]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Chercher le fichier zip téléchargé
        zip_files = list(dataset_dir.glob("*.zip"))
        if zip_files:
            zip_file = zip_files[0]
            print(f"✅ Téléchargé: {zip_file.name}")
            
            # Décompresser
            with zipfile.ZipFile(zip_file, 'r') as zf:
                zf.extractall(dataset_dir)
                print(f"📦 Décompressé dans: {dataset_dir}")
            
            # Optionnel: supprimer le zip
            zip_file.unlink()
            print(f"🗑️  Fichier zip supprimé")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors du téléchargement: {e.stderr}")
        return False
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

def main():
    """Télécharge tous les datasets nécessaires"""
    
    print("=" * 60)
    print("📥 TÉLÉCHARGEMENT DES DATASETS KAGGLE")
    print("=" * 60)
    
    # Vérifier Kaggle
    if not setup_kaggle():
        sys.exit(1)
    
    # Liste des datasets à télécharger
    datasets = [
        # Sécurité
        {
            "name": "redamorsli/kube-ids0",
            "description": "Kubernetes Intrusion Detection Dataset",
            "output": "data/raw/kube-ids0"
        },
        {
            "name": "usmanjutt17/kubernetes-anomaly-detection-dataset",
            "description": "Kubernetes Anomaly Detection",
            "output": "data/raw"
        },
        {
            "name": "nickkinyae/kubernetes-resource-and-performancemetricsallocation",
            "description": "K8s Performance Metrics & Resource Allocation",
            "output": "data/raw"
        },
        # Supplémentaires (optionnels)
        {
            "name": "gymprathap/synthetic-kubernetes-and-istio-logs",
            "description": "Logs synthétiques K8s & Istio",
            "output": "data/raw/synthetic_logs",
            "optional": True
        },
        {
            "name": "thedevastator/kubernetes-kubectl-command-dataset",
            "description": "Kubernetes Commands Dataset",
            "output": "data/raw/kubectl_commands",
            "optional": True
        }
    ]
    
    base_dir = Path(".")
    success_count = 0
    
    for dataset in datasets:
        output_path = base_dir / dataset["output"]
        
        # Vérifier si déjà téléchargé
        if output_path.exists() and any(output_path.rglob("*.csv")):
            print(f"\n⏭️  Dataset déjà présent: {dataset['name']}")
            print(f"   Emplacement: {output_path}")
            success_count += 1
            continue
        
        print(f"\n{'='*40}")
        print(f"Dataset: {dataset['name']}")
        print(f"Description: {dataset['description']}")
        print(f"Destination: {output_path}")
        
        try:
            if download_kaggle_dataset(dataset["name"], output_path):
                success_count += 1
                # Lister les fichiers téléchargés
                files = list(output_path.rglob("*"))
                print(f"📁 Fichiers disponibles ({len(files)}):")
                for file in files[:5]:  # Afficher les 5 premiers
                    if file.is_file():
                        size_mb = file.stat().st_size / (1024*1024)
                        print(f"   - {file.name} ({size_mb:.2f} MB)")
                if len(files) > 5:
                    print(f"   ... et {len(files) - 5} autres fichiers")
            else:
                if dataset.get("optional", False):
                    print(f"⚠️  Dataset optionnel ignoré: {dataset['name']}")
                else:
                    print(f"❌ Échec du téléchargement obligatoire")
                    return False
                
        except KeyboardInterrupt:
            print("\n⏹️  Téléchargement interrompu par l'utilisateur")
            break
    
    # Résumé
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ DU TÉLÉCHARGEMENT")
    print("=" * 60)
    print(f"✅ {success_count}/{len(datasets)} datasets téléchargés avec succès")
    
    # Lister tous les fichiers raw disponibles
    raw_dir = base_dir / "data/raw"
    if raw_dir.exists():
        print(f"\n📁 CONTENU DE data/raw:")
        for item in raw_dir.iterdir():
            if item.is_dir():
                subfiles = list(item.rglob("*.csv")) + list(item.rglob("*.json"))
                print(f"   📂 {item.name}/ ({len(subfiles)} fichiers)")
            elif item.suffix in ['.csv', '.json', '.parquet']:
                size_mb = item.stat().st_size / (1024*1024)
                print(f"   📄 {item.name} ({size_mb:.2f} MB)")
    
    return success_count > 0

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎯 TÉLÉCHARGEMENT TERMINÉ - PRÊT POUR LE TRAITEMENT!")
    else:
        print("\n❌ ÉCHEC DU TÉLÉCHARGEMENT")
        sys.exit(1)