# src/data_collection/real_time_collector.py
"""
API de collecte de logs K8s en temps réel
Reçoit les logs du simulateur et les sauvegarde
"""

from flask import Flask, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
import threading
import queue
import time
import json
from pathlib import Path
import logging

# Configuration
app = Flask(__name__)
log_queue = queue.Queue(maxsize=10000)  # Buffer de 10k logs
buffer_lock = threading.Lock()

# Configuration des dossiers
BASE_DIR = Path("data/real_time")
LOG_TYPES = {
    'system': BASE_DIR / "system_logs",
    'security': BASE_DIR / "security_logs",
    'raw': BASE_DIR / "raw_buffer"
}

# Stats
stats = {
    'total_received': 0,
    'system_logs': 0,
    'security_logs': 0,
    'anomalies_detected': 0,
    'last_received': None,
    'errors': 0
}

def setup_directories():
    """Crée la structure de dossiers"""
    for log_type, path in LOG_TYPES.items():
        path.mkdir(parents=True, exist_ok=True)
    
    # Sous-dossiers par date
    today = datetime.now().strftime("%Y-%m-%d")
    for path in LOG_TYPES.values():
        (path / today).mkdir(exist_ok=True)
    
    print("✅ Directories created:")
    for log_type, path in LOG_TYPES.items():
        print(f"   • {log_type}: {path}")

def save_logs_periodically():
    """Sauvegarde périodique des logs en mémoire vers le disque"""
    while True:
        time.sleep(60)  # Toutes les 60 secondes
        
        logs_to_save = []
        while not log_queue.empty() and len(logs_to_save) < 1000:
            try:
                logs_to_save.append(log_queue.get_nowait())
            except queue.Empty:
                break
        
        if logs_to_save:
            save_logs_to_disk(logs_to_save)

def save_logs_to_disk(logs):
    """Sauvegarde les logs sur disque dans différents formats"""
    if not logs:
        return
    
    df = pd.DataFrame(logs)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    today = datetime.now().strftime("%Y-%m-%d")
    
    try:
        # 1. Sauvegarde CSV brut
        raw_path = LOG_TYPES['raw'] / today / f"logs_{timestamp}.csv"
        df.to_csv(raw_path, index=False)
        
        # 2. Séparer par type si possible
        if 'log_type' in df.columns or 'source' in df.columns:
            # Logs système
            system_mask = df['source'].str.contains('kubernetes', na=False) if 'source' in df.columns else False
            if system_mask.any():
                system_df = df[system_mask]
                system_path = LOG_TYPES['system'] / today / f"k8s_system_{timestamp}.csv"
                system_df.to_csv(system_path, index=False)
                
                # Version Parquet pour Spark
                parquet_path = LOG_TYPES['system'] / today / f"k8s_system_{timestamp}.parquet"
                system_df.to_parquet(parquet_path, index=False)
            
            # Logs sécurité
            security_mask = df['source'].str.contains('security|network', na=False, case=False) if 'source' in df.columns else False
            if security_mask.any():
                security_df = df[security_mask]
                security_path = LOG_TYPES['security'] / today / f"security_{timestamp}.csv"
                security_df.to_csv(security_path, index=False)
                
                # Version Parquet pour Spark
                parquet_path = LOG_TYPES['security'] / today / f"security_{timestamp}.parquet"
                security_df.to_parquet(parquet_path, index=False)
        
        # 3. Buffer pour Spark Streaming
        buffer_path = BASE_DIR / "streaming_buffer.parquet"
        if buffer_path.exists():
            # Ajouter aux données existantes
            existing_df = pd.read_parquet(buffer_path)
            combined_df = pd.concat([existing_df.tail(10000), df])  # Garder les 10k plus récents
            combined_df.to_parquet(buffer_path, index=False)
        else:
            # Créer nouveau buffer
            df.to_parquet(buffer_path, index=False)
        
        # Mettre à jour les stats
        with buffer_lock:
            stats['total_received'] += len(logs)
            if 'is_anomaly' in df.columns:
                stats['anomalies_detected'] += df['is_anomaly'].sum()
        
        print(f"💾 Saved {len(logs)} logs to disk")
        
    except Exception as e:
        print(f"❌ Error saving logs: {e}")
        # Sauvegarde d'urgence
        emergency_path = BASE_DIR / f"emergency_{timestamp}.json"
        with open(emergency_path, 'w') as f:
            json.dump(logs, f, default=str)

@app.route('/api/logs', methods=['POST'])
def receive_log():
    """Endpoint principal pour recevoir des logs"""
    try:
        log_data = request.json
        
        if not log_data:
            return jsonify({"error": "No data provided"}), 400
        
        # Ajouter des métadonnées
        log_data['received_at'] = datetime.now().isoformat()
        log_data['api_version'] = '1.0'
        log_data['processed'] = False
        
        # Mettre en queue
        try:
            log_queue.put_nowait(log_data)
            
            # Mettre à jour les stats
            with buffer_lock:
                stats['total_received'] += 1
                stats['last_received'] = datetime.now().isoformat()
                
                if log_data.get('source') == 'kubernetes':
                    stats['system_logs'] += 1
                elif log_data.get('source') == 'security_monitor':
                    stats['security_logs'] += 1
                
                if log_data.get('is_anomaly', 0) == 1:
                    stats['anomalies_detected'] += 1
            
            return jsonify({
                "status": "success",
                "message": "Log received",
                "log_id": stats['total_received'],
                "queue_size": log_queue.qsize()
            }), 200
            
        except queue.Full:
            stats['errors'] += 1
            return jsonify({"error": "Queue full, try again later"}), 503
            
    except Exception as e:
        stats['errors'] += 1
        return jsonify({"error": str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Endpoint pour obtenir les statistiques"""
    with buffer_lock:
        return jsonify({
            "status": "running",
            "stats": stats,
            "queue_size": log_queue.qsize(),
            "uptime": str(datetime.now() - start_time),
            "storage": {
                "system_logs": str(LOG_TYPES['system']),
                "security_logs": str(LOG_TYPES['security']),
                "raw_buffer": str(LOG_TYPES['raw'])
            }
        }), 200

@app.route('/api/health', methods=['GET'])
def health_check():
    """Endpoint de santé"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }), 200

@app.route('/api/logs/batch', methods=['POST'])
def receive_batch_logs():
    """Endpoint pour recevoir plusieurs logs en une fois"""
    try:
        logs = request.json
        
        if not isinstance(logs, list):
            return jsonify({"error": "Expected a list of logs"}), 400
        
        received_count = 0
        for log in logs:
            try:
                log['received_at'] = datetime.now().isoformat()
                log_queue.put_nowait(log)
                received_count += 1
            except queue.Full:
                break
        
        with buffer_lock:
            stats['total_received'] += received_count
        
        return jsonify({
            "status": "partial" if received_count < len(logs) else "success",
            "received": received_count,
            "total": len(logs),
            "queue_size": log_queue.qsize()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def cleanup_old_files():
    """Nettoie les anciens fichiers (garder 7 jours)"""
    while True:
        time.sleep(3600)  # Toutes les heures
        
        cutoff_date = datetime.now() - timedelta(days=7)
        
        for log_type, base_path in LOG_TYPES.items():
            if base_path.exists():
                for date_dir in base_path.iterdir():
                    if date_dir.is_dir():
                        try:
                            dir_date = datetime.strptime(date_dir.name, "%Y-%m-%d")
                            if dir_date < cutoff_date:
                                # Supprimer le dossier et son contenu
                                import shutil
                                shutil.rmtree(date_dir)
                                print(f"🗑️  Cleaned up old directory: {date_dir}")
                        except ValueError:
                            pass

if __name__ == "__main__":
    # Configuration
    setup_directories()
    start_time = datetime.now()
    
    # Démarrer le thread de sauvegarde
    save_thread = threading.Thread(target=save_logs_periodically, daemon=True)
    save_thread.start()
    
    # Démarrer le thread de nettoyage
    cleanup_thread = threading.Thread(target=cleanup_old_files, daemon=True)
    cleanup_thread.start()
    
    print("\n" + "="*60)
    print("🚀 K8s Real-time Log Collector API")
    print("="*60)
    print(f"📂 Data directory: {BASE_DIR}")
    print(f"📊 Storage structure:")
    print(f"   • System logs: {LOG_TYPES['system']}")
    print(f"   • Security logs: {LOG_TYPES['security']}")
    print(f"   • Raw buffer: {LOG_TYPES['raw']}")
    print(f"🌐 API endpoints:")
    print(f"   • POST /api/logs         - Receive single log")
    print(f"   • POST /api/logs/batch   - Receive multiple logs")
    print(f"   • GET  /api/stats        - Get statistics")
    print(f"   • GET  /api/health       - Health check")
    print(f"🔄 Auto-save: Every 60 seconds")
    print(f"🗑️  Auto-cleanup: Older than 7 days")
    print("="*60)
    print(f"🔗 API running on: http://localhost:5000")
    print("="*60 + "\n")
    
    # Démarrer Flask
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
