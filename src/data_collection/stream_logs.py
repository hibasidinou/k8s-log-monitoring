# stream_logs.py

import pandas as pd
import time
import threading
import json
import os
from datetime import datetime
from kafka import KafkaProducer  # type: ignore[reportMissingImports]
from kafka.errors import NoBrokersAvailable  # type: ignore[reportMissingImports]

# ==================== CONFIGURATION ====================
CSV_PATH = "data/processed/k8s_logs_with_security.csv"
OUTPUT_LOG_FILE = "data/logs/streamed_logs.log"
NUM_SERVERS = 10
KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'kubernetes-logs'
DELAY_BETWEEN_LOGS = 0.0001  # 10,000 logs/sec
# =======================================================


class MultiServerLogStreamer:
    """
    Simule plusieurs serveurs qui génèrent des logs Kubernetes en parallèle
    et les envoient à Kafka + sauvegarde dans un fichier
    """
    
    def __init__(self, csv_path, num_servers=10):
        print(f"🚀 Initializing Multi-Server Log Streamer...")
        print(f"   Servers: {num_servers}")
        print(f"   Kafka: {KAFKA_BOOTSTRAP}")
        print(f"   Output file: {OUTPUT_LOG_FILE}")
        
        # Charger le dataset
        self.df = pd.read_csv(csv_path)
        self.num_servers = num_servers
        
        # Assigner chaque log à un serveur (round-robin)
        if 'server_id' not in self.df.columns:
            self.df['server_id'] = [
                f'server-{i % num_servers + 1:02d}' 
                for i in range(len(self.df))
            ]
        
        # Créer le dossier de logs si nécessaire
        os.makedirs(os.path.dirname(OUTPUT_LOG_FILE), exist_ok=True)
        
        # Lock pour l'écriture fichier thread-safe
        self.log_lock = threading.Lock()
        
        # Créer Kafka producer
        print("📡 Connecting to Kafka...")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                api_version=(0, 10, 1)
            )
            print("✅ Connected to Kafka successfully")
        except NoBrokersAvailable:
            print("\n" + "="*60)
            print("❌ ERROR: Cannot connect to Kafka broker")
            print("="*60)
            print(f"\nKafka is not available at {KAFKA_BOOTSTRAP}")
            print("\nTo start Kafka:")
            print("   1. Start Zookeeper: zookeeper-server-start.bat config/zookeeper.properties")
            print("   2. Start Kafka: kafka-server-start.bat config/server.properties")
            print("\nOr check if Kafka is running:")
            print("   netstat -an | findstr 9092")
            print("="*60 + "\n")
            raise
        except Exception as e:
            print(f"\n❌ ERROR: Failed to connect to Kafka: {e}")
            raise
        
        print(f"✅ Loaded {len(self.df)} logs\n")
    
    def format_log(self, row, server_id):
        """
        Formate le log au format Kubernetes
        
        Colonnes attendues dans le CSV :
        - timestamp, log_level, message, pod_name, namespace, node_name,
        - container_name, cpu_usage_percent, memory_usage_mb, network_bytes,
        - is_anomaly, anomaly_type, app_type, source_component
        """
        # Timestamp actuel (simule temps réel)
        current_timestamp = datetime.now()
        
        # Extraire les valeurs du CSV (avec valeurs par défaut si manquantes)
        log_level = str(row.get('log_level', 'INFO')).strip()
        message = str(row.get('message', 'No message')).strip()
        pod_name = str(row.get('pod_name', 'unknown-pod')).strip()
        namespace = str(row.get('namespace', 'default')).strip()
        node_name = str(row.get('node_name', 'unknown-node')).strip()
        container_name = str(row.get('container_name', 'unknown-container')).strip()
        
        # Métriques numériques
        cpu_usage = float(row.get('cpu_usage_percent', 0.0))
        memory_mb = int(float(row.get('memory_usage_mb', 0)))
        network_bytes = int(float(row.get('network_bytes', 0)))
        
        # Anomalie
        is_anomaly = int(row.get('is_anomaly', 0))
        anomaly_type = str(row.get('anomaly_type', 'normal')).strip()
        
        # Métadonnées
        app_type = str(row.get('app_type', 'unknown')).strip()
        source_component = str(row.get('source_component', 'unknown')).strip()
        
        # ========== FORMAT TEXTE (4 lignes) ==========
        log_text = (
            f"[{current_timestamp.strftime('%Y-%m-%d %H:%M:%S')}] [{log_level}] [{server_id}] [{pod_name}]\n"
            f"namespace={namespace} node={node_name} container={container_name}\n"
            f"cpu={cpu_usage:.2f}% mem={memory_mb}MB net={network_bytes}B\n"
            f"anomaly={is_anomaly} type={anomaly_type} app={app_type} source={source_component}\n"
            f"message: {message}\n"
        )
        # ==============================================
        
        # Format structuré (pour Kafka et Spark)
        log_data = {
            'timestamp': current_timestamp.isoformat(),
            'server_id': server_id,
            'log_level': log_level,
            'message': message,
            'pod_name': pod_name,
            'namespace': namespace,
            'node_name': node_name,
            'container_name': container_name,
            'cpu_usage_percent': cpu_usage,
            'memory_usage_mb': memory_mb,
            'network_bytes': network_bytes,
            'is_anomaly': is_anomaly,
            'anomaly_type': anomaly_type,
            'app_type': app_type,
            'source_component': source_component
        }
        
        return log_data, log_text
    
    def stream_server_logs(self, server_id):
        """
        Simule UN serveur qui génère des logs
        """
        # Filtrer les logs pour ce serveur
        server_logs = self.df[self.df['server_id'] == server_id].copy()
        server_logs = server_logs.fillna("")
        
        total_logs = len(server_logs)
        sent_count = 0
        
        print(f"[{server_id}] 🟢 Started - {total_logs} logs to stream")
        
        start_time = time.time()
        
        for idx, row in server_logs.iterrows():
            # Formater le log
            log_data, log_text = self.format_log(row, server_id)
            
            try:
                # ========== ENVOYER À KAFKA ==========
                self.producer.send(
                    KAFKA_TOPIC,
                    key=server_id,
                    value=log_data
                )
                # =====================================
                 # ========== AFFICHER DANS LA CONSOLE ==========
                print(log_text, end='')  # end='' car log_text a déjà \n à la fin
                # ==============================================
                
                # ========== SAUVEGARDER DANS LE FICHIER ==========
                with self.log_lock:  # Thread-safe writing
                    with open(OUTPUT_LOG_FILE, 'a', encoding='utf-8') as f:
                        f.write(log_text + "\n")
                # =================================================
                
                sent_count += 1
                
                # Progress (moins fréquent pour ne pas polluer la console)
                if sent_count % 10000 == 0:
                    print(f"[{server_id}] 📊 Progress: {sent_count}/{total_logs} logs")
                
            except Exception as e:
                print(f"[{server_id}] ❌ Error sending log: {e}")
            
            # Délai pour simuler temps réel
            time.sleep(DELAY_BETWEEN_LOGS)
        
        elapsed = time.time() - start_time
        throughput = sent_count / elapsed if elapsed > 0 else 0
        
        print(f"[{server_id}] ✅ Completed - {sent_count} logs streamed in {elapsed:.2f}s ({throughput:.0f} logs/s)")
    
    def start_streaming(self):
        """
        Démarre tous les serveurs en parallèle (multi-threading)
        """
        print(f"\n{'='*60}")
        print(f"🎬 Starting multi-server streaming")
        print(f"{'='*60}\n")
        
        threads = []
        start_time = time.time()
        
        # Créer un thread pour chaque serveur
        for i in range(1, self.num_servers + 1):
            server_id = f'server-{i:02d}'
            
            thread = threading.Thread(
                target=self.stream_server_logs,
                args=(server_id,),
                name=f"Thread-{server_id}"
            )
            
            threads.append(thread)
            thread.start()
            
            time.sleep(0.05)  # Échelonner les démarrages
        
        print("⏳ Streaming in progress...\n")
        
        # Attendre que tous les threads finissent
        for thread in threads:
            thread.join()
        
        # Fermer proprement
        self.producer.flush()
        self.producer.close()
        
        elapsed_time = time.time() - start_time
        
        print(f"\n{'='*60}")
        print(f"✅ STREAMING COMPLETED")
        print(f"   Total time: {elapsed_time:.2f} seconds")
        print(f"   Total logs: {len(self.df)}")
        print(f"   Throughput: {len(self.df)/elapsed_time:.0f} logs/sec")
        print(f"   Logs saved to: {OUTPUT_LOG_FILE}")
        print(f"{'='*60}\n")


# ==================== POINT D'ENTRÉE ====================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Multi-Server Kubernetes Log Streamer')
    parser.add_argument('--csv', default=CSV_PATH, help='Path to CSV dataset')
    parser.add_argument('--servers', type=int, default=NUM_SERVERS, help='Number of servers (default: 10)')
    parser.add_argument('--kafka', default=KAFKA_BOOTSTRAP, help='Kafka bootstrap server')
    parser.add_argument('--delay', type=float, default=DELAY_BETWEEN_LOGS, help='Delay between logs (seconds)')
    parser.add_argument('--output', default=OUTPUT_LOG_FILE, help='Output log file')
    
    args = parser.parse_args()
    
    # Mettre à jour les configs depuis les arguments
    KAFKA_BOOTSTRAP = args.kafka
    DELAY_BETWEEN_LOGS = args.delay
    OUTPUT_LOG_FILE = args.output
    
    # Créer et lancer le streamer
    streamer = MultiServerLogStreamer(
        csv_path=args.csv,
        num_servers=args.servers
    )
    
    streamer.start_streaming()