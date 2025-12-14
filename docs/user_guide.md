# Guide Utilisateur - Dashboard de Monitoring K8s

## 📋 Table des matières

1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Lancement du Dashboard](#lancement-du-dashboard)
4. [Navigation](#navigation)
5. [Pages du Dashboard](#pages-du-dashboard)
6. [Fonctionnalités](#fonctionnalités)
7. [Filtres et Options](#filtres-et-options)
8. [Dépannage](#dépannage)

## Introduction

Le Dashboard de Monitoring Kubernetes est une application Streamlit interactive qui permet de visualiser et analyser les logs Kubernetes en temps réel. Il offre une vue d'ensemble complète de l'état de vos serveurs, des anomalies détectées, et des métriques de performance.

## Installation

### Prérequis

- Python 3.8 ou supérieur
- pip (gestionnaire de paquets Python)

### Étapes d'installation

1. **Cloner le projet** (si ce n'est pas déjà fait)
   ```bash
   git clone <repository-url>
   cd k8s-log-monitoring
   ```

2. **Créer un environnement virtuel** (recommandé)
   ```bash
   python -m venv venv
   
   # Sur Windows
   venv\Scripts\activate
   
   # Sur Linux/Mac
   source venv/bin/activate
   ```

3. **Installer les dépendances**
   ```bash
   pip install -r requirements.txt
   ```

4. **Vérifier que les données sont disponibles**
   - Les données traitées doivent être dans `data/output/`
   - Les anomalies dans `data/output/anomalies_detected.json`
   - Le statut des serveurs dans `data/output/dashboard/server_status.parquet`

## Lancement du Dashboard

### Sur Linux/Mac

```bash
./scripts/run_dashboard.sh
```

Ou avec un port personnalisé :
```bash
./scripts/run_dashboard.sh 8502
```

### Sur Windows

```cmd
scripts\run_dashboard.bat
```

Ou avec un port personnalisé :
```cmd
scripts\run_dashboard.bat 8502
```

### Lancement manuel

```bash
streamlit run src/dashboard/app.py
```

Le dashboard sera accessible à l'adresse : **http://localhost:8501**

## Navigation

Le dashboard est organisé en plusieurs pages accessibles via la barre latérale :

- **Overview** : Vue d'ensemble avec KPIs et résumé
- **Server Map** : Carte interactive des serveurs
- **Alerts** : Panneau d'alertes et anomalies
- **Timeline** : Timeline des incidents
- **Metrics** : Métriques et analyses temporelles
- **Server Details** : Détails d'un serveur spécifique

## Pages du Dashboard

### 📊 Overview

La page d'aperçu affiche :

- **Cartes KPI** : Métriques clés (Total Logs, Anomalies, Serveurs, Taux d'erreur)
- **Résumé de santé des serveurs** : Statut de chaque serveur
- **Alertes récentes** : Les 10 dernières alertes
- **Graphiques temporels** : Évolution des métriques

### 🗺️ Server Map

Visualisation interactive de tous les serveurs :

- **Carte des serveurs** : Représentation graphique avec indicateurs de statut
- **Liste des serveurs** : Liste détaillée avec statut, erreurs et anomalies
- **Tableau de statut** : Tableau complet avec toutes les informations

**Légende des couleurs** :
- 🟢 Vert : Serveur sain (healthy)
- 🟠 Orange : Avertissement (warning)
- 🔴 Rouge : Critique (critical)

### 🚨 Alerts

Panneau d'alertes avec :

- **Panneau d'alertes** : Alertes groupées par sévérité
  - Critical (Rouge)
  - High (Orange Rouge)
  - Warning (Orange)
  - Medium (Jaune)
  - Low (Jaune clair)
  - Info (Bleu)

- **Tableau d'anomalies** : Tableau interactif avec filtres
  - Filtre par serveur
  - Filtre par sévérité
  - Filtre par type d'anomalie

### 📅 Timeline

Visualisation temporelle des incidents :

- **Timeline interactive** : Graphique temporel des incidents avec Plotly
- **Statistiques de timeline** : Plage temporelle, premier/dernier incident
- **Distribution par sévérité** : Graphique en barres
- **Heatmap horaire** : Distribution des incidents par jour et heure

### 📈 Metrics

Métriques et analyses :

- **Métriques temporelles** : Graphiques linéaires des métriques clés
- **Log Count** : Nombre de logs au fil du temps
- **Error Rate** : Taux d'erreur au fil du temps
- **Métriques par serveur** : Filtrage possible par serveur

### 🖥️ Server Details

Détails complets d'un serveur spécifique :

- **Informations du serveur** : Statut, logs totaux, erreurs, avertissements
- **Anomalies du serveur** : Liste des anomalies détectées
- **Métriques du serveur** : Graphiques spécifiques au serveur

**Note** : Sélectionnez un serveur dans la barre latérale pour voir ses détails.

## Fonctionnalités

### Auto-refresh

Activez l'auto-refresh dans la barre latérale pour actualiser automatiquement les données toutes les 30 secondes.

### Filtres

- **Filtre par serveur** : Visualisez les données d'un serveur spécifique ou de tous
- **Filtres dans les tableaux** : Filtrez les anomalies par serveur, sévérité ou type

### Actualisation manuelle

Cliquez sur le bouton "🔄 Refresh Data" dans la barre latérale pour actualiser manuellement les données.

## Filtres et Options

### Barre latérale

La barre latérale contient :

1. **Navigation** : Sélection de la page à afficher
2. **Filtres** :
   - Sélection du serveur (All ou serveur spécifique)
3. **Options** :
   - Auto-refresh (30s)
   - Bouton de rafraîchissement manuel

### Filtres dans les pages

Certaines pages ont des filtres intégrés :

- **Alerts** : Filtres par serveur, sévérité et type
- **Timeline** : Filtre par serveur
- **Metrics** : Filtre par serveur

## Dépannage

### Le dashboard ne se lance pas

1. Vérifiez que Python est installé : `python --version`
2. Vérifiez que Streamlit est installé : `pip list | grep streamlit`
3. Installez les dépendances : `pip install -r requirements.txt`

### Pas de données affichées

1. Vérifiez que les données existent dans `data/output/`
2. Vérifiez les fichiers suivants :
   - `data/output/anomalies_detected.json`
   - `data/output/dashboard/server_status.parquet`
   - `data/output/aggregated_metrics.parquet`
3. Si les fichiers n'existent pas, exécutez d'abord le pipeline Spark

### Erreurs d'import

1. Vérifiez que vous êtes dans le répertoire racine du projet
2. Vérifiez que la structure des dossiers est correcte
3. Vérifiez que tous les modules `__init__.py` sont présents

### Le dashboard est lent

1. Réduisez le nombre de lignes affichées dans les tableaux
2. Utilisez les filtres pour limiter les données
3. Vérifiez la taille des fichiers de données
4. Le cache Streamlit se rafraîchit toutes les 5 minutes (configurable dans `data_loader.py`)

### Port déjà utilisé

Si le port 8501 est déjà utilisé :

```bash
# Linux/Mac
./scripts/run_dashboard.sh 8502

# Windows
scripts\run_dashboard.bat 8502
```

## Structure des données attendues

### anomalies_detected.json

Format JSON avec liste d'anomalies :
```json
[
  {
    "timestamp": "2024-01-01T10:00:00Z",
    "server_id": "server_01",
    "severity": "critical",
    "message": "Error message",
    "anomaly_type": "intrusion",
    "log_level": "ERROR"
  }
]
```

### server_status.parquet

DataFrame Parquet avec colonnes :
- `server_id` : Identifiant du serveur
- `status` : Statut (healthy, warning, critical)
- `last_update` : Dernière mise à jour
- `total_logs` : Nombre total de logs
- `error_count` : Nombre d'erreurs
- `warning_count` : Nombre d'avertissements

### aggregated_metrics.parquet

DataFrame Parquet avec colonnes :
- `timestamp` : Horodatage
- `server_id` : Identifiant du serveur
- `log_count` : Nombre de logs
- `error_rate` : Taux d'erreur
- `avg_response_time` : Temps de réponse moyen

## Support

Pour toute question ou problème :

1. Vérifiez ce guide utilisateur
2. Consultez la documentation technique dans `docs/architecture.md`
3. Contactez l'équipe de développement

---

**Version** : 1.0.0  
**Dernière mise à jour** : 2024





