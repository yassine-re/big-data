# Partie 2 - Automatisation du pipeline

Ce dossier contiendra l'automatisation de la collecte et des transformations, avec
gestion des erreurs, journalisation, traçabilité et documentation de reprise.

## Principe d'architecture

```text
Filestorage (lecture seule) -> Lake -> Bronze -> Silver -> Gold
                               Python   SQL exécuté dans ClickHouse
```

Python est limité à la copie, à la pseudonymisation nécessaire à l'entrée du lake, au
chargement et à l'envoi des requêtes SQL. Les futures transformations Bronze vers Silver
puis Gold resteront dans ClickHouse. Aucune transformation métier n'est effectuée avec
pandas.

## Étape 1 - socle ClickHouse

Le service ClickHouse crée quatre bases encore vides : `bronze`, `silver`, `gold` et
`control`.

Pour lancer ClickHouse :

```bash
docker compose up -d clickhouse
docker compose ps
```

Son interface SQL est disponible sur <http://localhost:8123/play> lorsqu'il est en état
`healthy`.

## Étape 2 - copie pseudonymisée dans le lake

Le service Python `lake-loader` lit les fichiers source sans les modifier et recrée leur
arborescence dans `lake/`.

| Source | Traitement autorisé avant le lake |
|---|---|
| `patients.csv` | `patient_id` devient `patient_sk`, la date de naissance devient l'année, NIR/nom/prénom sont supprimés |
| `sejours.csv` | `stay_id` et `patient_id` deviennent `stay_sk` et `patient_sk` |
| `diagnostics.json` | `stay_id` devient `stay_sk`, la structure imbriquée est conservée |
| `monitoring.parquet` | `stay_id` devient `stay_sk`, le fichier est traité par lots de 10 000 lignes |
| Référentiels CSV | copie identique, sans transformation |

Les pseudonymes sont des HMAC-SHA-256 déterministes : un même identifiant source produit
la même clé dans tous les fichiers avec le même secret. Les deux secrets sont différents
pour séparer les domaines patient et séjour.

Cette étape n'applique aucun contrôle métier : elle ne corrige pas les dates, ne filtre
pas les constantes et ne calcule aucune alerte.

### Configuration

Depuis `data/` :

```bash
cp .env.example .env
```

Dans `.env`, remplacer au minimum :

- `CLICKHOUSE_PASSWORD` ;
- `HMAC_PATIENT_SECRET` ;
- `HMAC_STAY_SECRET`.

Les secrets HMAC doivent contenir au moins 32 caractères et être différents. `.env` et
le contenu de `lake/` sont exclus de Git.

### Exécution

```bash
docker compose run --rm --build lake-loader
```

Le résultat conserve l'organisation par domaine et par jour :

```text
lake/
├── patients/<AAAA-MM-JJ>/patients.csv
├── sejours/<AAAA-MM-JJ>/sejours.csv
├── diagnostics/<AAAA-MM-JJ>/diagnostics.json
├── monitoring/<AAAA-MM-JJ>/monitoring.parquet
└── referentiels/<AAAA-MM-JJ>/{services,cim10}.csv
```

### Limites de cette étape

- une nouvelle exécution réécrit atomiquement les fichiers cibles ;
- l'incrémentalité, les checksums et la table de traçabilité seront ajoutés plus tard ;
- aucune donnée n'est encore chargée dans Bronze.

Pour arrêter ClickHouse sans supprimer ses volumes :

```bash
docker compose down
```
