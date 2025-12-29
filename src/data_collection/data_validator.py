# src/data_collection/data_validator.py
import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime

class DataValidator:
    """Valide la qualité des datasets"""
    
    def __init__(self, data_dir="data"):
        self.data_dir = Path(data_dir)
        self.reports = {}
        
    def validate_dataset(self, file_path, dataset_name):
        """Valide un dataset spécifique"""
        print(f"\n🔍 Validation de: {dataset_name}")
        print(f"   Fichier: {file_path}")
        
        try:
            # Lire le dataset
            if file_path.suffix == '.csv':
                df = pd.read_csv(file_path)
            elif file_path.suffix == '.parquet':
                df = pd.read_parquet(file_path)
            else:
                print(f"❌ Format non supporté: {file_path.suffix}")
                return None
            
            report = {
                'dataset_name': dataset_name,
                'file_path': str(file_path),
                'timestamp': datetime.now().isoformat(),
                'basic_stats': self._get_basic_stats(df),
                'data_quality': self._check_data_quality(df),
                'schema_info': self._get_schema_info(df),
                'anomaly_stats': self._get_anomaly_stats(df) if 'label' in df.columns else None,
                'recommendations': []
            }
            
            # Générer des recommandations
            report['recommendations'] = self._generate_recommendations(report)
            
            self.reports[dataset_name] = report
            self._print_report(report)
            
            return report
            
        except Exception as e:
            print(f"❌ Erreur lors de la validation: {e}")
            return None
    
    def _get_basic_stats(self, df):
        """Récupère les statistiques basiques"""
        return {
            'num_rows': len(df),
            'num_columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024*1024),
            'column_names': list(df.columns)
        }
    
    def _check_data_quality(self, df):
        """Vérifie la qualité des données"""
        missing = df.isnull().sum()
        missing_pct = (missing / len(df) * 100).round(2)
        
        duplicates = df.duplicated().sum()
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        outlier_info = {}
        
        for col in numeric_cols[:10]:  # Vérifier les 10 premières colonnes numériques
            if df[col].notna().sum() > 0:
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
                outlier_info[col] = {
                    'outliers': int(outliers),
                    'outlier_pct': round((outliers / len(df) * 100), 2)
                }
        
        return {
            'missing_values': missing[missing > 0].to_dict(),
            'missing_percentage': missing_pct[missing_pct > 0].to_dict(),
            'duplicate_rows': int(duplicates),
            'duplicate_percentage': round((duplicates / len(df) * 100), 2),
            'outliers': outlier_info
        }
    
    def _get_schema_info(self, df):
        """Récupère les informations du schéma"""
        schema = {}
        for col in df.columns:
            schema[col] = {
                'dtype': str(df[col].dtype),
                'unique_values': df[col].nunique() if df[col].dtype == 'object' else None,
                'sample_values': df[col].dropna().unique()[:3].tolist() if df[col].dtype == 'object' else None
            }
        return schema
    
    def _get_anomaly_stats(self, df):
        """Récupère les statistiques d'anomalies"""
        if 'label' not in df.columns:
            return None
        
        label_counts = df['label'].value_counts().to_dict()
        anomaly_pct = (label_counts.get(1, 0) / len(df) * 100) if len(df) > 0 else 0
        
        stats = {
            'label_distribution': label_counts,
            'anomaly_percentage': round(anomaly_pct, 2),
            'class_imbalance': round(abs(label_counts.get(0, 0) - label_counts.get(1, 0)) / len(df) * 100, 2)
        }
        
        if 'anomaly_type' in df.columns:
            stats['anomaly_type_distribution'] = df['anomaly_type'].value_counts().to_dict()
        
        return stats
    
    def _generate_recommendations(self, report):
        """Génère des recommandations basées sur les problèmes détectés"""
        recommendations = []
        
        # Vérifier les valeurs manquantes
        missing_pct = report['data_quality']['missing_percentage']
        for col, pct in missing_pct.items():
            if pct > 20:
                recommendations.append(f"❌ Colonne '{col}' a {pct}% de valeurs manquantes - envisager la suppression")
            elif pct > 5:
                recommendations.append(f"⚠️  Colonne '{col}' a {pct}% de valeurs manquantes - nécessite une imputation")
        
        # Vérifier les doublons
        if report['data_quality']['duplicate_percentage'] > 10:
            recommendations.append(f"⚠️  {report['data_quality']['duplicate_percentage']}% de doublons - envisager la déduplication")
        
        # Vérifier le déséquilibre des classes
        if report['anomaly_stats']:
            imbalance = report['anomaly_stats']['class_imbalance']
            if imbalance > 30:
                recommendations.append(f"⚠️  Déséquilibre de classes élevé ({imbalance}%) - envisager le rééchantillonnage")
        
        # Vérifier les outliers
        for col, info in report['data_quality']['outliers'].items():
            if info['outlier_pct'] > 10:
                recommendations.append(f"⚠️  Colonne '{col}' a {info['outlier_pct']}% d'outliers - vérifier la validité")
        
        if not recommendations:
            recommendations.append("✅ Dataset de bonne qualité - prêt pour le traitement")
        
        return recommendations
    
    def _print_report(self, report):
        """Affiche le rapport de validation"""
        print(f"   📊 Lignes: {report['basic_stats']['num_rows']:,}")
        print(f"   📊 Colonnes: {report['basic_stats']['num_columns']}")
        print(f"   💾 Mémoire: {report['basic_stats']['memory_usage_mb']:.2f} MB")
        
        if report['anomaly_stats']:
            print(f"   🏷️  Distribution labels: {report['anomaly_stats']['label_distribution']}")
            print(f"   📈 Taux d'anomalies: {report['anomaly_stats']['anomaly_percentage']}%")
        
        if report['data_quality']['missing_values']:
            print(f"   ❓ Valeurs manquantes: {len(report['data_quality']['missing_values'])} colonnes")
        
        print(f"   💡 Recommandations:")
        for rec in report['recommendations']:
            print(f"      - {rec}")
    
    def validate_all_datasets(self):
        """Valide tous les datasets dans le projet"""
        print("=" * 60)
        print("🔍 VALIDATION COMPLÈTE DES DONNÉES")
        print("=" * 60)
        
        datasets_to_validate = [
            ("data/processed/hybrid_security_system_dataset.csv", "Dataset Hybride Principal"),
            ("data/processed/system_anomalies_only.csv", "Anomalies Système"),
            ("data/processed/security_attacks_only.csv", "Attaques Sécurité"),
            ("data/raw/simulated_logs/simulated_k8s_logs_full.csv", "Logs Simulés"),
        ]
        
        for file_path, name in datasets_to_validate:
            path = Path(file_path)
            if path.exists():
                self.validate_dataset(path, name)
            else:
                print(f"\n⚠️  Fichier non trouvé: {file_path}")
        
        # Générer un rapport global
        self._generate_global_report()
        
        return self.reports
    
    def _generate_global_report(self):
        """Génère un rapport global"""
        print("\n" + "=" * 60)
        print("📋 RAPPORT GLOBAL DE QUALITÉ DES DONNÉES")
        print("=" * 60)
        
        total_rows = sum(r['basic_stats']['num_rows'] for r in self.reports.values())
        total_size = sum(r['basic_stats']['memory_usage_mb'] for r in self.reports.values())
        
        print(f"📊 TOTAL: {len(self.reports)} datasets validés")
        print(f"📈 {total_rows:,} entrées au total")
        print(f"💾 {total_size:.2f} MB de données")
        
        # Sauvegarder les rapports
        report_dir = Path("reports/validation")
        report_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = report_dir / f"data_validation_report_{timestamp}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(self.reports, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Rapport sauvegardé: {report_path}")
        print("✅ VALIDATION TERMINÉE!")

def main():
    """Fonction principale"""
    validator = DataValidator()
    reports = validator.validate_all_datasets()
    
    # Afficher un résumé
    print("\n🎯 RÉSUMÉ DE LA VALIDATION:")
    for name, report in reports.items():
        status = "✅" if all(not r.startswith("❌") for r in report['recommendations']) else "⚠️ "
        print(f"   {status} {name}: {report['basic_stats']['num_rows']:,} lignes")

if __name__ == "__main__":
    main()