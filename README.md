# Entrepôt de Données de Santé - CHU

Projet Big Data M2 organisé selon les deux livrables de la fiche sujet.

## Organisation

| Dossier | Livrable |
|---|---|
| `dossier/` | Partie 1 - dossier de conception et interface d'analyse |
| `data/` | Partie 2 - automatisation du pipeline de données |

## Architecture cible

```text
Filestorage -> Lake -> Bronze -> Silver -> Gold -> Metabase
                 Python      transformations SQL ClickHouse
```

Python sera limité à la copie des fichiers, à la pseudonymisation nécessaire avant le
lake, au chargement et au déclenchement des requêtes SQL. Les transformations Bronze,
Silver et Gold seront exécutées dans ClickHouse, sans traitement métier en mémoire avec
pandas.

## Progression

- [x] Étape 1 - initialiser le dépôt et ClickHouse avec Docker
- [x] Étape 2 - copier et pseudonymiser les fichiers dans le lake
- [x] Étape 3 - charger les fichiers dans Bronze
- [ ] Étape 4 - transformer Bronze vers Silver en SQL
- [ ] Étape 5 - construire Gold en SQL
- [ ] Étape 6 - planifier et tracer le pipeline
- [ ] Étape 7 - construire les dashboards et le cloisonnement Metabase

Chaque étape fait l'objet d'un commit dédié et d'une mise à jour du README concerné.
