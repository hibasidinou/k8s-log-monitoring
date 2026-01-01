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
CSV_PATH = "data/processed/logs_cleaned.csv"
OUTPUT_LOG_FILE = "data/logs/streamed_logs.log"  # ← Fichier de sortie
NUM_SERVERS = 10  # ← CHANGÉ à 10 serveurs
KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'kubernetes-logs'
DELAY_BETWEEN_LOGS = 1
# =======================================================


class MultiServerLogStreamer:
    """
    Simule plusieurs serveurs qui génèrent des logs en parallèle
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
        
        # Assigner chaque log à un serveur
        if 'server_id' not in self.df.columns:
            self.df['server_id'] = [
                f'server-{i % num_servers + 1:02d}' 
                for i in range(len(self.df))
            ]
        
        # Créer le dossier de logs si nécessaire
        os.makedirs(os.path.dirname(OUTPUT_LOG_FILE), exist_ok=True)
        
        # Ouvrir le fichier de logs (mode append)
        self.log_file = open(OUTPUT_LOG_FILE, 'a', buffering=1)  # buffering=1 pour flush automatique
        self.log_lock = threading.Lock()  # Pour éviter les conflits entre threads
        
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
            print("   brew services start kafka")
            print("\nOr check if Kafka is running:")
            print("   brew services list | grep kafka")
            print("="*60 + "\n")
            self.log_file.close()
            raise
        except Exception as e:
            print(f"\n❌ ERROR: Failed to connect to Kafka: {e}")
            self.log_file.close()
            raise
        
        print(f"✅ Loaded {len(self.df)} logs\n")
    
    def format_log(self, row, server_id):
        """
        Formate le log exactement comme demandé
        """
        # Source (application)
        if "dvwa__src_ip" in row:
            source = "DVWA"
            prefix = "dvwa__"
        elif "boa__src_ip" in row:
            source = "BOA"
            prefix = "boa__"
        else:
            source = "UNKNOWN"
            prefix = ""
        
        # Timestamp actuel (simule temps réel)
        timestamp = datetime.now()
        
        # Network
        src_ip = row.get(f'{prefix}src_ip', '0.0.0.0')
        src_port = int(row.get(f'{prefix}src_port', 0))
        dst_ip = row.get(f'{prefix}dst_ip', '0.0.0.0')
        dst_port = int(row.get(f'{prefix}dst_port', 0))
        proto = row.get(f"{prefix}protocol", "UNK")
        
        # Traffic
        duration = round(float(row.get(f"{prefix}duration", 0)), 3)
        packets = int(row.get(f"{prefix}packets_count", 0))
        bytes_ = int(row.get(f"{prefix}total_payload_bytes", 0))
        
        # System metrics
        cpu = round(float(row.get(f"{prefix}container_cpu_usage_seconds_rate", 0)), 2)
        mem = int(float(row.get(f"{prefix}container_memory_usage_bytes", 0)) / (1024 * 1024))
        net_rx = int(float(row.get(f"{prefix}container_network_receive_bytes_rate", 0)) / 1024)
        
        # ========== FORMAT TEXTE (exactement comme demandé) ==========
        log_text = (
            f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] [WARN] [{source}] [{server_id}]\n"
            f"src={src_ip}:{src_port} dst={dst_ip}:{dst_port} proto={proto}\n"
            f"duration={duration}s packets={packets} bytes={bytes_}\n"
            f"cpu={cpu} mem={mem}MB net_rx={net_rx}KB/s\n"
        )
        # ==============================================================
        
        # Format structuré (pour Kafka et Spark)
        log_data = {
            'timestamp': timestamp.isoformat(),
            'server_id': server_id,
            'source': source,
            'src_ip': src_ip,
            'src_port': src_port,
            'dst_ip': dst_ip,
            'dst_port': dst_port,
            'protocol': proto,
            'duration': duration,
            'packets': packets,
            'bytes': bytes_,
            'cpu': cpu,
            'memory_mb': mem,
            'network_rx_kb': net_rx,
            'log_text': log_text
        }
        
        return log_data, log_text
    
    def stream_server_logs(self, server_id):
        """
        Simule UN serveur qui génère des logs
        """
        server_logs = self.df[self.df['server_id'] == server_id].copy()
        server_logs = server_logs.fillna("")
        
        total_logs = len(server_logs)
        sent_count = 0
        
        print(f"[{server_id}] 🟢 Started - {total_logs} logs to stream")
        
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
                print(log_text)  # ← AFFICHE le log formaté
                # ==============================================
                
                # ========== SAUVEGARDER DANS LE FICHIER ==========
                with self.log_lock:  # Thread-safe writing
                    self.log_file.write(log_text + "\n")
                # =================================================
                
                sent_count += 1
                
                # Progress (moins fréquent pour ne pas polluer la console)
                if sent_count % 5000 == 0:
                    print(f"[{server_id}] 📊 Progress: {sent_count}/{total_logs} logs")
                
            except Exception as e:
                print(f"[{server_id}] ❌ Error: {e}")
            
            # Délai pour simuler temps réel
            time.sleep(DELAY_BETWEEN_LOGS)
        
        print(f"[{server_id}] ✅ Finished - Sent {sent_count} logs")
    
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
            
            time.sleep(0.1)  # Échelonner les démarrages
        
        print("⏳ Streaming in progress...\n")
        
        # Attendre que tous les threads finissent
        for thread in threads:
            thread.join()
        
        # Fermer proprement
        self.producer.flush()
        self.producer.close()
        self.log_file.close()
        
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
    
    parser = argparse.ArgumentParser(description='Multi-Server Log Streamer with Kafka')
    parser.add_argument('--csv', default=CSV_PATH, help='Path to CSV dataset')
    parser.add_argument('--servers', type=int, default=NUM_SERVERS, help='Number of servers')
    parser.add_argument('--kafka', default=KAFKA_BOOTSTRAP, help='Kafka bootstrap server')
    
    args = parser.parse_args()
    
    # Créer et lancer le streamer
    streamer = MultiServerLogStreamer(
        csv_path=args.csv,
        num_servers=args.servers
    )
    
    streamer.start_streaming()