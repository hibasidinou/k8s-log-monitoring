# src/data_collection/k8s_real_time_simulator.py
import pandas as pd
import numpy as np
import time
import requests
from datetime import datetime
import random
from pathlib import Path

class K8sRealTimeSimulator:
    def __init__(self, k8s_dataset_path=None):
        """Simulateur qui utilise ton dataset K8s amélioré"""
        
        # Chercher le dataset automatiquement
        if k8s_dataset_path is None:
            k8s_dataset_path = self.find_k8s_dataset()
        
        print(f"📂 Loading K8s dataset from: {k8s_dataset_path}")
        
        # Charger ton dataset hybride amélioré
        self.k8s_data = pd.read_csv(k8s_dataset_path)
        
        print(f"✅ Dataset loaded: {len(self.k8s_data)} rows")
        print(f"   Columns: {self.k8s_data.columns.tolist()}")
        
        # Préparer les patterns
        if 'anomaly_type' in self.k8s_data.columns:
            self.system_patterns = self.k8s_data[
                self.k8s_data['anomaly_type'] == 'system_anomaly'
            ]
            self.security_patterns = self.k8s_data[
                self.k8s_data['anomaly_type'] == 'security_attack'
            ]
        else:
            # Fallback si pas de colonne anomaly_type
            self.system_patterns = self.k8s_data.iloc[:len(self.k8s_data)//2]
            self.security_patterns = self.k8s_data.iloc[len(self.k8s_data)//2:]
        
        print(f"   System patterns: {len(self.system_patterns)} rows")
        print(f"   Security patterns: {len(self.security_patterns)} rows")
        
        # VMs simulées avec métadonnées K8s réalistes
        self.vms = self.create_virtual_cluster()
    
    def find_k8s_dataset(self):
        """Cherche automatiquement le dataset K8s ou crée un dataset de secours"""
        possible_paths = [
            "data/hybrid_dataset_enhanced.csv",
            "notebooks/data/hybrid_dataset_enhanced.csv",
            "data/processed/enhanced/hybrid_dataset_enhanced.csv"
        ]
        
        for path in possible_paths:
            if Path(path).exists():
                return path
        
        print("⚠️ No K8s dataset found. Creating a synthetic baseline for simulation...")
        dummy_data = pd.DataFrame({
            'cpu_usage': np.random.uniform(10, 80, 100),
            'memory_usage': np.random.uniform(1024, 4096, 100),
            'packets_count': np.random.randint(50, 5000, 100),
            'flow_bytes_per_second': np.random.uniform(5000, 50000, 100),
            'anomaly_type': ['normal']*80 + ['system_anomaly']*10 + ['security_attack']*10
        })
        dummy_path = Path("data/synthetic_simulator_baseline.csv")
        dummy_path.parent.mkdir(parents=True, exist_ok=True)
        dummy_data.to_csv(dummy_path, index=False)
        return str(dummy_path)

    def create_virtual_cluster(self):
        """Crée un cluster K8s virtuel réaliste"""
        vms = {}
        
        # 5 VMs pour système K8s
        for i in range(1, 6):
            vms[f"k8s-node-{i:02d}"] = {
                'type': 'system',
                'role': 'worker' if i <= 4 else 'master',
                'namespace': random.choice(["default", "kube-system", "monitoring", "production"]),
                'anomaly_rate': 0.12,
                'services': random.sample(["api-server", "etcd", "kubelet", "kube-proxy", "coredns"], 3)
            }
        
        # 5 VMs pour sécurité/réseau
        for i in range(6, 11):
            vms[f"sec-gateway-{i-5:02d}"] = {
                'type': 'security',
                'role': 'gateway',
                'monitored_services': ["ingress", "firewall", "ids", "waf"],
                'anomaly_rate': 0.20,
                'traffic_type': random.choice(["internal", "external", "dmz"])
            }
        
        return vms
    
    def generate_k8s_system_log(self, vm_name, vm_info):
        """Génère un log système K8s réaliste"""
        if len(self.system_patterns) > 0:
            pattern = self.system_patterns.sample(1).iloc[0]
        else:
            pattern = {}
        
        # Métriques réalistes K8s
        log_types = [
            "PodEvent", "NodeStatus", "Deployment", "Service", "Volume",
            "ConfigUpdate", "ScaleEvent", "HealthCheck", "Scheduler"
        ]
        
        is_anomaly = random.random() < vm_info['anomaly_rate']
        
        log = {
            "timestamp": datetime.now().isoformat(),
            "source": "kubernetes",
            "vm_name": vm_name,
            "vm_role": vm_info['role'],
            "namespace": vm_info['namespace'],
            "log_type": random.choice(log_types),
            
            # Métriques système
            "cpu_usage_percent": float(pattern.get('cpu_usage', random.uniform(5, 95))),
            "memory_usage_mb": float(pattern.get('memory_usage', random.uniform(512, 8192))),
            "pod_count": random.randint(3, 50),
            "container_count": random.randint(5, 100),
            "node_status": random.choice(["Ready", "NotReady", "Pressure"]),
            
            # Labels
            "is_anomaly": 1 if is_anomaly else 0,
            "anomaly_type": "system_anomaly" if is_anomaly else "normal",
            "severity": random.choice(["INFO", "WARNING", "ERROR"]) if is_anomaly else "INFO",
            
            # Message réaliste
            "message": self.generate_system_message(vm_info, is_anomaly)
        }
        
        # Ajouter des champs spécifiques si disponibles
        for field in ['server_name', 'data_center', 'timestamp']:
            if field in pattern and pd.notna(pattern[field]):
                log[f"dataset_{field}"] = pattern[field]
        
        return log
    
    def generate_security_log(self, vm_name, vm_info):
        """Génère un log sécurité réseau"""
        if len(self.security_patterns) > 0:
            pattern = self.security_patterns.sample(1).iloc[0]
        else:
            pattern = {}
        
        is_anomaly = random.random() < vm_info['anomaly_rate']
        
        # Types d'attaques basés sur ton dataset
        attack_types = ["DOS", "SQLi", "BruteForce", "PortScan", "Malware", "DDoS"]
        
        log = {
            "timestamp": datetime.now().isoformat(),
            "source": "security_monitor",
            "vm_name": vm_name,
            "traffic_type": vm_info['traffic_type'],
            "log_type": "NetworkTraffic",
            
            # Métriques réseau
            "packets_per_second": int(pattern.get('packets_count', random.randint(10, 10000))),
            "bytes_per_second": float(pattern.get('flow_bytes_per_second', random.uniform(1000, 1000000))),
            "connections": random.randint(10, 1000),
            "protocol": random.choice(["TCP", "UDP", "HTTP", "HTTPS", "DNS"]),
            "src_port": random.randint(1024, 65535),
            "dst_port": random.choice([80, 443, 22, 3306, 5432, 6379]),
            
            # Sécurité
            "is_anomaly": 1 if is_anomaly else 0,
            "anomaly_type": "security_attack" if is_anomaly else "normal",
            "threat_level": random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"]) if is_anomaly else "LOW",
            "attack_type": random.choice(attack_types) if is_anomaly else None,
            
            "message": self.generate_security_message(vm_info, is_anomaly)
        }
        
        return log
    
    def generate_system_message(self, vm_info, is_anomaly):
        """Génère un message système réaliste"""
        if is_anomaly:
            messages = [
                f"High CPU usage detected on {vm_info['role']} node",
                f"Memory pressure in namespace {vm_info['namespace']}",
                f"Pod crash loop detected",
                f"Volume mount failed on {random.choice(vm_info['services'])}",
                f"Node {vm_info['role']} not responding to health checks"
            ]
        else:
            messages = [
                f"Pod scheduled successfully on {vm_info['role']}",
                f"Service {random.choice(vm_info['services'])} healthy",
                f"Deployment scaled to {random.randint(1, 10)} replicas",
                f"ConfigMap updated in {vm_info['namespace']}",
                f"Node {vm_info['role']} status: Ready"
            ]
        return random.choice(messages)
    
    def generate_security_message(self, vm_info, is_anomaly):
        """Génère un message sécurité réaliste"""
        if is_anomaly:
            messages = [
                f"Suspicious traffic from {random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
                f"Port scan detected on {vm_info['traffic_type']} network",
                f"Multiple failed login attempts",
                f"Possible {random.choice(['DOS', 'SQLi'])} attack in progress",
                f"Anomalous traffic pattern in {vm_info['traffic_type']} zone"
            ]
        else:
            messages = [
                f"Normal {vm_info['traffic_type']} traffic pattern",
                f"Security scan completed - no threats found",
                f"Firewall rules applied successfully",
                f"IDS monitoring active on {vm_info['traffic_type']} interface",
                f"All security services operational"
            ]
        return random.choice(messages)
    
    def run(self, api_url="http://localhost:5000/api/logs", interval=3):
        """Exécute le simulateur avec gestion d'erreurs améliorée"""
        print("\n" + "="*60)
        print("🚀 K8s REAL-TIME SIMULATOR STARTED")
        print("="*60)
        print(f"📊 Based on your enhanced K8s dataset")
        print(f"🔧 10 Virtual K8s Nodes:")
        
        for vm_name, info in self.vms.items():
            print(f"   • {vm_name} ({info['type']} - {info.get('role', 'gateway')})")
        
        print(f"📡 Sending to: {api_url}")
        print(f"⏱️  Interval: {interval}s")
        print("="*60 + "\n")
        
        iteration = 0
        while True:
            iteration += 1
            print(f"\n📈 Batch {iteration} - {datetime.now().strftime('%H:%M:%S')}")
            
            for vm_name, vm_info in self.vms.items():
                try:
                    if vm_info['type'] == 'system':
                        log = self.generate_k8s_system_log(vm_name, vm_info)
                    else:
                        log = self.generate_security_log(vm_name, vm_info)
                    
                    # Envoyer à l'API
                    response = requests.post(
                        api_url,
                        json=log,
                        timeout=5
                    )
                    
                    if response.status_code == 200:
                        status = "🚨" if log['is_anomaly'] else "✅"
                        print(f"  {status} {vm_name}: {log['message'][:50]}...")
                    else:
                        print(f"  ❌ {vm_name}: API Error {response.status_code}")
                        
                except requests.exceptions.ConnectionError:
                    print(f"  ⚠️  {vm_name}: Connection failed. Is the API running at {api_url}?")
                except Exception as e:
                    print(f"  ⚠️  {vm_name}: {str(e)}")
            
            time.sleep(interval)

# Version simplifiée pour test rapide
def quick_test():
    """Test rapide du simulateur"""
    print("🧪 Quick test of K8s Real-time Simulator")
    
    try:
        simulator = K8sRealTimeSimulator()
        
        # Générer quelques logs sans envoyer
        print("\n📝 Sample logs generated:")
        for vm_name, vm_info in list(simulator.vms.items())[:2]:
            if vm_info['type'] == 'system':
                log = simulator.generate_k8s_system_log(vm_name, vm_info)
            else:
                log = simulator.generate_security_log(vm_name, vm_info)
            
            print(f"\n{vm_name} ({vm_info['type']}):")
            print(f"  Message: {log['message']}")
            print(f"  Anomaly: {'YES' if log['is_anomaly'] else 'NO'}")
            print(f"  CPU: {log.get('cpu_usage_percent', 'N/A')}%")
            print(f"  Packets: {log.get('packets_per_second', 'N/A')}/s")
        
        print("\n✅ Simulator ready to use!")
        print("To run: python src/data_collection/k8s_real_time_simulator.py --run")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 First run the enhancement script:")
        print("   python src/data_collection/enhance_datasets.py")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='K8s Real-time Log Simulator')
    parser.add_argument('--run', action='store_true', help='Run the simulator')
    parser.add_argument('--test', action='store_true', help='Quick test')
    parser.add_argument('--api', default='http://localhost:5000/api/logs', help='API URL')
    parser.add_argument('--interval', type=int, default=3, help='Interval in seconds')
    
    args = parser.parse_args()
    
    if args.test:
        quick_test()
    elif args.run:
        simulator = K8sRealTimeSimulator()
        simulator.run(api_url=args.api, interval=args.interval)
    else:
        print("Usage: python k8s_real_time_simulator.py --test or --run")
