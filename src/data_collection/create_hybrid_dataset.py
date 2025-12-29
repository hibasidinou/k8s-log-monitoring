# src/data_collection/create_hybrid_dataset.py - VERSION CORRIGÉE
import pandas as pd
import numpy as np
from pathlib import Path
import os
from sklearn.preprocessing import LabelEncoder

def create_system_anomaly_dataset():
    """Crée le dataset d'anomalies système K8s"""
    print("🔧 Création du dataset anomalies système...")
    
    # Charger les datasets système
    df_metrics = pd.read_csv("data/raw/kubernetes_performance_metrics_dataset.csv")
    df_alloc = pd.read_csv("data/raw/kubernetes_resource_allocation_dataset.csv")
    
    # Fusionner
    df_system = pd.merge(df_metrics, df_alloc, on=['pod_name', 'namespace'], how='inner')
    
    # Définir les seuils réalistes (ajustés)
    thresholds = {
        'node_cpu_usage': df_system['node_cpu_usage'].quantile(0.95),  # Top 5%
        'node_memory_usage': df_system['node_memory_usage'].quantile(0.95),
        'network_latency': df_system['network_latency'].quantile(0.95),
        'restart_count': df_system['restart_count'].quantile(0.95),
        'disk_io': df_system['disk_io'].quantile(0.95),
    }
    
    print("📏 Seuils utilisés:")
    for key, value in thresholds.items():
        print(f"   - {key}: {value:.2f}")
    
    # Calculer un score d'anomalie
    anomaly_score = (
        (df_system['node_cpu_usage'] > thresholds['node_cpu_usage']).astype(int) +
        (df_system['node_memory_usage'] > thresholds['node_memory_usage']).astype(int) +
        (df_system['network_latency'] > thresholds['network_latency']).astype(int) +
        (df_system['restart_count'] > thresholds['restart_count']).astype(int) +
        (df_system['disk_io'] > thresholds['disk_io']).astype(int) +
        (df_system['event_type'] == 'Error').astype(int) +
        (df_system['pod_status'] == 'Failed').astype(int)
    )
    
    # Anomalie si score >= 2 (au moins 2 indicateurs problématiques)
    df_system['label'] = np.where(anomaly_score >= 2, 1, 0)
    df_system['anomaly_type'] = 'system_anomaly'
    
    print(f"✅ Anomalies système détectées: {df_system['label'].sum():,} / {len(df_system):,} ({(df_system['label'].sum()/len(df_system)*100):.2f}%)")
    
    return df_system

# MODIFIE cette fonction dans ton script :
def create_security_dataset():
    """Prépare le dataset de sécurité avec labels corrects"""
    print("🔒 Chargement du dataset sécurité...")
    
    security_path = "data/raw/kube-ids0/dvwa_dataset/processed/dvwa_dataset_ml_ready.csv"
    df_security = pd.read_csv(security_path)
    
    # VÉRIFIER ET CORRIGER LES LABELS
    print(f"   - Rows: {len(df_security):,}")
    print(f"   - Label min: {df_security['label'].min()}, max: {df_security['label'].max()}")
    
    # Si labels ne sont pas 0/1, les normaliser
    if df_security['label'].max() > 1 or df_security['label'].min() < 0:
        print("   ⚠️  Normalisation des labels...")
        # Supposons que 0 = normal, >0 = attaque
        df_security['label'] = np.where(df_security['label'] > 0, 1, 0)
    
    print(f"   - Labels 0 (normal): {(df_security['label'] == 0).sum():,}")
    print(f"   - Labels 1 (attaque): {df_security['label'].sum():,}")
    print(f"   - Taux d'attaques: {(df_security['label'].sum()/len(df_security)*100):.2f}%")
    
    df_security['anomaly_type'] = 'security_attack'
    
    return df_security

def create_hybrid_dataset():
    """Crée et merge les datasets sécurité + système"""
    
    print("=" * 60)
    print("🔄 CRÉATION DU DATASET HYBRIDE SÉCURITÉ + SYSTÈME")
    print("=" * 60)
    
    # 1. Créer les deux datasets
    df_system = create_system_anomaly_dataset()
    df_security = create_security_dataset()
    
    # 2. Sélectionner les colonnes compatibles
    print("\n🔗 Sélection des features communes...")
    
    # Features qu'on veut garder pour l'analyse
    desired_features = [
        # Métriques de base
        'cpu_usage', 'memory_usage',
        # Métriques réseau
        'packets_count', 'flow_bytes_per_second', 'flow_packets_per_second',
        # Métriques conteneurs (sécurité)
        'container_cpu_usage_seconds_rate', 'container_memory_usage_bytes',
        # Labels et types
        'label', 'anomaly_type'
    ]
    
    # Pour le système, créer des proxy pour les features manquantes
    if 'cpu_usage' not in df_system.columns and 'node_cpu_usage' in df_system.columns:
        df_system['cpu_usage'] = df_system['node_cpu_usage']
    if 'memory_usage' not in df_system.columns and 'node_memory_usage' in df_system.columns:
        df_system['memory_usage'] = df_system['node_memory_usage']
    
    # Pour la sécurité, renommer certaines colonnes
    if 'bytes_rate' in df_security.columns:
        df_security = df_security.rename(columns={'bytes_rate': 'flow_bytes_per_second'})
    if 'packets_rate' in df_security.columns:
        df_security = df_security.rename(columns={'packets_rate': 'flow_packets_per_second'})
    
    # Sélectionner les colonnes disponibles
    system_cols = [col for col in desired_features if col in df_system.columns]
    security_cols = [col for col in desired_features if col in df_security.columns]
    
    df_system_clean = df_system[system_cols].copy()
    df_security_clean = df_security[security_cols].copy()
    
    # 3. Équilibrer les échantillons
    print("\n⚖️  Équilibrage des datasets...")
    
    # Prendre 15k de chaque (ou moins si dataset plus petit)
    system_sample_size = min(15000, len(df_system_clean))
    security_sample_size = min(15000, len(df_security_clean))
    
    df_system_sample = df_system_clean.sample(n=system_sample_size, random_state=42)
    df_security_sample = df_security_clean.sample(n=security_sample_size, random_state=42)
    
    # 4. Combiner
    print("\n🔄 Fusion des datasets...")
    hybrid_df = pd.concat([df_system_sample, df_security_sample], ignore_index=True)
    
    # Encoder le type d'anomalie
    encoder = LabelEncoder()
    hybrid_df['anomaly_type_encoded'] = encoder.fit_transform(hybrid_df['anomaly_type'])
    
    # 5. Sauvegarder
    print("\n💾 Sauvegarde...")
    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Dataset hybride principal
    hybrid_path = output_dir / "hybrid_security_system_dataset.csv"
    hybrid_df.to_csv(hybrid_path, index=False)
    
    # Format Parquet pour Spark
    parquet_dir = output_dir / "parquet"
    parquet_dir.mkdir(exist_ok=True)
    hybrid_df.to_parquet(parquet_dir / "hybrid_dataset.parquet", index=False)
    
    # Datasets séparés aussi
    df_system_sample.to_csv(output_dir / "system_anomalies_only.csv", index=False)
    df_security_sample.to_csv(output_dir / "security_attacks_only.csv", index=False)
    
    # 6. Rapport
    print("\n" + "=" * 60)
    print("✅ DATASET HYBRIDE CRÉÉ !")
    print("=" * 60)
    
    print(f"\n📊 STATISTIQUES FINALES:")
    print(f"Dataset complet: {len(hybrid_df):,} entrées")
    print(f"  ├─ Anomalies système: {len(df_system_sample):,}")
    print(f"  │  ├─ Label 0 (normal): {(df_system_sample['label'] == 0).sum():,}")
    print(f"  │  └─ Label 1 (anomalie): {df_system_sample['label'].sum():,}")
    print(f"  └─ Attaques sécurité: {len(df_security_sample):,}")
    print(f"     ├─ Label 0 (normal): {(df_security_sample['label'] == 0).sum():,}")
    print(f"     └─ Label 1 (attaque): {df_security_sample['label'].sum():,}")
    
    print(f"\n📈 Distribution totale des labels:")
    print(f"  - Total normal (0): {(hybrid_df['label'] == 0).sum():,}")
    print(f"  - Total anomalies (1): {hybrid_df['label'].sum():,}")
    print(f"  - Taux d'anomalies: {(hybrid_df['label'].sum()/len(hybrid_df)*100):.2f}%")
    
    print(f"\n💾 FICHIERS SAUVEGARDÉS:")
    print(f"  - {hybrid_path}")
    print(f"  - {parquet_dir / 'hybrid_dataset.parquet'}")
    print(f"  - {output_dir / 'system_anomalies_only.csv'}")
    print(f"  - {output_dir / 'security_attacks_only.csv'}")
    
    print(f"\n🎯 READY FOR SPARK PROCESSING! 🚀")
    
    return hybrid_df

if __name__ == "__main__":
    df = create_hybrid_dataset()
    print(f"\n🔍 Aperçu du dataset hybride:")
    print(df.head())
    print(f"\nColonnes: {df.columns.tolist()}")