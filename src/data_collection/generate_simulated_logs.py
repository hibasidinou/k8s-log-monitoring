# src/data_collection/generate_simulated_logs.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path
import json

def generate_kubernetes_logs(num_records=100000, anomaly_rate=0.15):
    """Génère des logs Kubernetes simulés avec anomalies"""
    
    print(f"🎲 Génération de {num_records:,} logs K8s simulés...")
    print(f"   Taux d'anomalies: {anomaly_rate*100}%")
    
    # Services/types de pods
    services = ['frontend', 'backend', 'database', 'cache', 'auth-service', 
                'api-gateway', 'payment-service', 'user-service', 'notification',
                'monitoring', 'logging', 'ingress-controller']
    
    # Namespaces
    namespaces = ['default', 'production', 'staging', 'development', 'monitoring', 'kube-system']
    
    # Niveaux de log
    log_levels = ['INFO', 'WARNING', 'ERROR', 'DEBUG', 'CRITICAL']
    
    # Messages types (avec placeholders) - DEBUG AJOUTÉ
    log_templates = {
        'INFO': [
            'Pod {pod} started successfully in namespace {namespace}',
            'Deployment {deployment} scaled to {replicas} replicas',
            'Service {service} created successfully',
            'Container {container} ready',
            'Node {node} joined the cluster',
            'Volume {volume} mounted to pod {pod}',
            'ConfigMap updated for namespace {namespace}',
            'Secret {secret} created successfully'
        ],
        'WARNING': [
            'High memory usage detected for pod {pod}: {memory_usage}MB',
            'CPU usage above threshold on node {node}: {cpu_usage}%',
            'Network latency increased to {latency}ms',
            'Disk space running low on node {node}: {disk_free}GB free',
            'Pod {pod} restart count: {restart_count}',
            'High number of TCP retransmissions detected',
            'Service {service} response time above threshold'
        ],
        'ERROR': [
            'Pod {pod} crashed with exit code {exit_code}',
            'Container {container} OOMKilled in pod {pod}',
            'Volume {volume} mount failed',
            'Network connectivity lost for service {service}',
            'Deployment {deployment} rollout failed',
            'Failed to pull image for container {container}',
            'PersistentVolumeClaim {pvc} binding failed'
        ],
        'DEBUG': [  # TEMPLATES DEBUG AJOUTÉS
            'Debug: Pod {pod} initialization sequence started',
            'Debug: Container {container} health check passed',
            'Debug: Network policy applied to namespace {namespace}',
            'Debug: Storage volume {volume} provisioned successfully',
            'Debug: Service {service} endpoint updated',
            'Debug: Kubernetes API call to {api_endpoint} completed',
            'Debug: Pod {pod} scheduled on node {node}',
            'Debug: Config sync completed for deployment {deployment}',
            'Debug: Metrics collected for namespace {namespace}',
            'Debug: Audit log entry created for user {user}'
        ],
        'CRITICAL': [
            'Node {node} down - not responding to ping',
            'Database pod {pod} data corruption detected',
            'Security breach detected in namespace {namespace}',
            'Cluster-wide network outage',
            'Critical service {service} unavailable',
            'ETCD cluster unhealthy - leader election failed',
            'Kubernetes control plane component {component} failed'
        ]
    }
    
    logs = []
    start_time = datetime.now() - timedelta(days=30)  # 30 jours de logs
    
    # Générer des logs
    for i in range(num_records):
        if i % 10000 == 0 and i > 0:
            print(f"   Progression: {i:,}/{num_records:,} logs générés")
        
        # Temps (progression linéaire avec un peu de random)
        time_offset = timedelta(seconds=random.randint(0, 30*24*3600))
        timestamp = start_time + time_offset
        
        # Métadonnées de base
        service = random.choice(services)
        namespace = random.choice(namespaces)
        pod_name = f"{service}-pod-{random.randint(1, 100)}-{random.randint(1000, 9999)}"
        container_name = f"container-{random.randint(1, 5)}"
        node_name = f"node-{random.randint(1, 20)}"
        
        # Déterminer si c'est une anomalie
        is_anomaly = random.random() < anomaly_rate
        
        # Choisir le niveau de log (plus d'ERROR/CRITICAL pour anomalies)
        if is_anomaly:
            log_level = random.choices(log_levels, weights=[0.05, 0.15, 0.4, 0.1, 0.3])[0]
        else:
            log_level = random.choices(log_levels, weights=[0.5, 0.2, 0.1, 0.15, 0.05])[0]
        
        # Sélectionner un template de message
        template = random.choice(log_templates[log_level])
        
        # Remplir le template avec variables supplémentaires pour DEBUG
        template_vars = {
            'pod': pod_name,
            'namespace': namespace,
            'service': service,
            'container': container_name,
            'node': node_name,
            'deployment': f"deploy-{service}",
            'replicas': random.randint(1, 10),
            'volume': f"volume-{random.randint(1, 100)}",
            'memory_usage': random.randint(800, 4096),
            'cpu_usage': random.randint(80, 100) if is_anomaly else random.randint(10, 60),
            'latency': random.randint(100, 500) if is_anomaly else random.randint(5, 50),
            'disk_free': random.randint(1, 10),
            'restart_count': random.randint(5, 20) if is_anomaly else random.randint(0, 2),
            'exit_code': random.choice([137, 139, 1, 255]),
            'secret': f"secret-{random.randint(1, 50)}",
            'api_endpoint': random.choice(["/api/v1/pods", "/apis/apps/v1/deployments", 
                                          "/api/v1/namespaces", "/apis/networking.k8s.io/v1/ingresses"]),
            'user': random.choice(["system:kube-scheduler", "system:kube-controller-manager", 
                                  "admin", "ci-cd-bot", "monitoring-agent"]),
            'pvc': f"pvc-{random.randint(1000, 9999)}",
            'component': random.choice(["kube-apiserver", "kube-controller-manager", 
                                       "kube-scheduler", "kube-proxy", "kubelet"])
        }
        
        message = template.format(**template_vars)
        
        # Métriques associées
        cpu_usage = random.randint(85, 100) if is_anomaly else random.randint(5, 60)
        memory_usage = random.randint(2048, 4096) if is_anomaly else random.randint(256, 1024)
        network_bytes = random.randint(1000000, 10000000) if is_anomaly else random.randint(1000, 100000)
        
        log_entry = {
            'timestamp': timestamp.isoformat(),
            'log_level': log_level,
            'message': message,
            'pod_name': pod_name,
            'namespace': namespace,
            'container_name': container_name,
            'node_name': node_name,
            'service': service,
            'cpu_usage_percent': cpu_usage,
            'memory_usage_mb': memory_usage,
            'network_bytes': network_bytes,
            'is_anomaly': 1 if is_anomaly else 0,
            'anomaly_type': random.choice(['cpu_spike', 'memory_leak', 'pod_crash', 
                                          'network_flood', 'disk_io_bottleneck']) if is_anomaly else 'normal'
        }
        
        logs.append(log_entry)
    
    return pd.DataFrame(logs)

def save_logs_in_chunks(df, output_dir, chunk_size=20000):
    """Sauvegarde les logs en chunks pour éviter les problèmes de mémoire"""
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n💾 Sauvegarde des logs dans {output_dir}")
    
    # Sauvegarde complète
    full_path = output_dir / "simulated_k8s_logs_full.csv"
    df.to_csv(full_path, index=False)
    print(f"✅ Fichier complet: {full_path} ({len(df):,} lignes)")
    
    # Sauvegarde en chunks
    num_chunks = (len(df) // chunk_size) + 1
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))
        
        if start_idx >= len(df):
            break
            
        chunk = df.iloc[start_idx:end_idx]
        chunk_path = output_dir / f"simulated_k8s_logs_chunk_{i+1:03d}.csv"
        chunk.to_csv(chunk_path, index=False)
    
    print(f"✅ {num_chunks} fichiers chunks créés")
    
    # Sauvegarde en Parquet aussi (meilleur pour Spark)
    parquet_path = output_dir / "simulated_k8s_logs.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"✅ Version Parquet: {parquet_path}")
    
    # Sauvegarde un échantillon pour inspection rapide
    sample_path = output_dir / "simulated_k8s_logs_sample_1000.csv"
    df.sample(min(1000, len(df))).to_csv(sample_path, index=False)
    print(f"✅ Échantillon (1000 lignes): {sample_path}")
    
    return full_path

def main():
    """Fonction principale"""
    print("=" * 60)
    print("🎭 GÉNÉRATION DE LOGS KUBERNETES SIMULÉS")
    print("=" * 60)
    
    # Paramètres
    NUM_LOGS = 150000  # 150k logs
    ANOMALY_RATE = 0.12  # 12% d'anomalies
    
    # Générer les logs
    df_logs = generate_kubernetes_logs(NUM_LOGS, ANOMALY_RATE)
    
    # Statistiques
    print(f"\n📊 STATISTIQUES DES LOGS GÉNÉRÉS:")
    print(f"   - Total logs: {len(df_logs):,}")
    print(f"   - Anomalies: {df_logs['is_anomaly'].sum():,} ({(df_logs['is_anomaly'].sum()/len(df_logs)*100):.1f}%)")
    print(f"   - Période: {df_logs['timestamp'].min()} à {df_logs['timestamp'].max()}")
    
    print(f"\n📈 DISTRIBUTION DES NIVEAUX DE LOG:")
    level_dist = df_logs['log_level'].value_counts()
    for level, count in level_dist.items():
        pct = (count / len(df_logs) * 100)
        print(f"   - {level}: {count:,} ({pct:.1f}%)")
    
    print(f"\n🎯 DISTRIBUTION DES TYPES D'ANOMALIE:")
    if df_logs['is_anomaly'].sum() > 0:
        anomalies = df_logs[df_logs['is_anomaly'] == 1]
        anomaly_dist = anomalies['anomaly_type'].value_counts()
        for anomaly_type, count in anomaly_dist.items():
            pct = (count / len(anomalies) * 100)
            print(f"   - {anomaly_type}: {count:,} ({pct:.1f}%)")
    
    # Sauvegarder
    output_dir = Path("data/raw/simulated_logs")
    save_logs_in_chunks(df_logs, output_dir)
    
    # Exemple de logs
    print(f"\n🔍 EXEMPLE DE LOGS GÉNÉRÉS (premières 3 lignes):")
    for idx, row in df_logs.head(3).iterrows():
        print(f"\n[{row['timestamp']}] {row['log_level']} - {row['message']}")
        print(f"   Pod: {row['pod_name']}, Namespace: {row['namespace']}, Anomalie: {row['is_anomaly']}")
    
    print(f"\n🎉 LOGS SIMULÉS CRÉÉS AVEC SUCCÈS!")
    print(f"Emplacement: {output_dir}")
    print(f"Fichiers créés:")
    print(f"  - simulated_k8s_logs_full.csv ({len(df_logs):,} lignes)")
    print(f"  - simulated_k8s_logs.parquet (format Spark)")
    print(f"  - simulated_k8s_logs_sample_1000.csv (échantillon)")
    print(f"  - simulated_k8s_logs_chunk_XXX.csv ({((len(df_logs)-1)//20000)+1} chunks)")

if __name__ == "__main__":
    main()