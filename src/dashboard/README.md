# Dashboard Module

Module de visualisation Streamlit pour le monitoring de logs Kubernetes.

## Structure

```
src/dashboard/
├── app.py                 # Application principale Streamlit
├── components/             # Composants de visualisation
│   ├── server_map.py     # Carte des serveurs
│   ├── alerts_panel.py   # Panneau d'alertes
│   ├── timeline.py       # Timeline des incidents
│   └── metrics.py        # Widgets métriques
└── utils/
    └── data_loader.py    # Chargement des données
```

## Dépendances

- streamlit >= 1.28.0
- pandas >= 2.0.0
- plotly >= 5.17.0
- pyarrow >= 12.0.0

## Utilisation

### Lancement rapide

```bash
# Linux/Mac
./scripts/run_dashboard.sh

# Windows
scripts\run_dashboard.bat
```

### Lancement manuel

```bash
streamlit run src/dashboard/app.py
```

## Pages disponibles

1. **Overview** : Vue d'ensemble avec KPIs
2. **Server Map** : Carte interactive des serveurs
3. **Alerts** : Panneau d'alertes et anomalies
4. **Timeline** : Timeline des incidents
5. **Metrics** : Métriques temporelles
6. **Server Details** : Détails d'un serveur

## Format des données

Le dashboard attend les données dans `data/output/` :

- `anomalies_detected.json` : Anomalies détectées
- `dashboard/server_status.parquet` : Statut des serveurs
- `dashboard/latest_alerts.json` : Dernières alertes
- `aggregated_metrics.parquet` : Métriques agrégées

Voir `docs/user_guide.md` pour plus de détails.





