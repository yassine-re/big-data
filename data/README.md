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
- le lake ne conserve pas encore l'historique d'un fichier modifié sous le même chemin.

## Étape 3 - chargement dans Bronze

Le service `bronze-loader` ne lit pas les lignes dans Python. Il découvre les fichiers,
calcule leur checksum SHA-256 puis demande à ClickHouse de les lire avec la fonction SQL
`file()`.

| Table | Grain Bronze | Format lu par ClickHouse |
|---|---|---|
| `bronze.patients` | un patient par snapshot | CSV |
| `bronze.stays` | un séjour source | CSV |
| `bronze.stay_diagnoses` | un diagnostic par séjour | JSON + `ARRAY JOIN` |
| `bronze.monitoring` | un relevé par séjour et horodatage | Parquet |
| `bronze.services` | un code service | CSV |
| `bronze.cim10` | un code diagnostic | CSV |

Bronze conserve les valeurs reçues après pseudonymisation et ajoute à chaque ligne :

- `source_day` ;
- `source_file` ;
- `file_checksum` ;
- `batch_id` ;
- `ingested_at`.

Les dates de séjour restent volontairement en texte avec le suffixe `_raw`. Une date
incorrecte pourra ainsi être détectée et rejetée en Silver sans faire échouer tout le
chargement Bronze. Le monitoring conserve les types numériques et temporels du Parquet.

### Idempotence

`control.ingested_files` enregistre l'état de chaque chargement. Le `batch_id` est calculé
de manière déterministe à partir du chemin et du checksum :

```text
même chemin + même contenu -> même batch_id -> fichier ignoré
même chemin + contenu modifié -> nouveau batch_id -> nouveau chargement
```

### Exécution

Le lake doit avoir été créé par l'étape 2. Lancer ensuite :

```bash
docker compose up -d clickhouse
docker compose run --rm --build bronze-loader
```

Une seconde exécution doit afficher uniquement des lignes `SKIPPED`.

Validation réalisée sur les fichiers fournis :

```text
Premier chargement : 14 fichiers SUCCESS, 135 275 lignes Bronze
Rejeu identique    : 0 fichier chargé, 14 fichiers SKIPPED
Colonnes directement identifiantes dans Bronze : 0
```

Pour consulter les fichiers chargés dans l'interface SQL :

```sql
SELECT
    source_file,
    status,
    rows_loaded,
    file_checksum
FROM control.v_ingested_files_current
ORDER BY source_day, source_file;
```

Pour consulter les volumes Bronze :

```sql
SELECT database, table, total_rows
FROM system.tables
WHERE database = 'bronze'
ORDER BY table;
```

### Limites de cette étape

- aucune règle de qualité métier n'est encore appliquée ;
- les répétitions d'un patient entre plusieurs snapshots sont conservées dans Bronze ;
- la déduplication fonctionnelle et les rejets seront traités en SQL dans Silver ;
- la planification et la journalisation globale du pipeline restent à ajouter.

## Étape 4 - transformation Bronze vers Silver

Le service `silver-transformer` ne récupère aucune ligne métier dans Python. Il calcule
uniquement l'empreinte des lots Bronze au statut `SUCCESS`, crée un identifiant
d'exécution déterministe, puis envoie à ClickHouse les requêtes `INSERT ... SELECT` de
`clickhouse/transform/31_silver_transform.sql`.

### Objets Silver publiés

| Vue Silver | Grain | Traitements principaux |
|---|---|---|
| `silver.patients` | un patient | dernier snapshot, sexe normalisé, année de naissance et région contrôlées |
| `silver.services` | un service | dernier libellé non vide par code |
| `silver.cim10` | un code CIM-10 | dernier libellé non vide par code normalisé |
| `silver.stays` | un séjour | dates converties, modes normalisés, références contrôlées, durée calculée |
| `silver.stay_diagnoses` | un diagnostic par séjour, code et type | déduplication et contrôle du séjour, du type et du code CIM-10 |
| `silver.monitoring` | un relevé par séjour et horodatage | déduplication, contrôle du séjour et des plages techniques |
| `silver.quality_events` | une anomalie par ligne et règle | motif, sévérité et lignage de chaque rejet ou avertissement |

Les valeurs conservées dans Silver respectent notamment les règles suivantes :

- une sortie ne peut pas précéder l'admission ;
- les modes d'admission acceptés sont `urgence`, `programme` et `mutation` ;
- les types de diagnostic acceptés sont `principal` et `associe` ;
- un diagnostic doit référencer un séjour Silver et un code du référentiel CIM-10 ;
- un relevé doit référencer un séjour Silver ;
- les plages techniques sont 20–250 pour la fréquence cardiaque, 50–100 pour la SpO2
  et 30–45 °C pour la température.

Ces plages éliminent les valeurs techniquement impossibles. Elles ne constituent pas
encore les seuils d'alerte clinique, qui seront calculés dans Gold. Un mode de sortie
absent sur un séjour terminé produit un événement `WARNING`, mais le séjour reste
utilisable.

### Publication atomique et rejeu

Les tables physiques suffixées par `_versions` contiennent le résultat associé à un
`run_id`. Les vues `silver.*` n'exposent que la dernière exécution enregistrée au statut
`SUCCESS` dans `control.silver_runs`.

```text
RUNNING -> écritures dans les tables de versions -> contrôles -> SUCCESS -> publication
```

Si une requête échoue, le statut devient `FAILED` et les vues continuent d'exposer la
dernière version réussie. L'identifiant dépend de l'empreinte Bronze et de la version
`silver-v1` : un rejeu strictement identique est ignoré. Lorsqu'une règle SQL change, la
version de transformation doit être incrémentée pour autoriser une nouvelle publication.

### Exécution

Après le chargement Bronze :

```bash
docker compose up -d clickhouse
docker compose run --rm --build silver-transformer
```

Une seconde exécution sans nouveau lot Bronze doit afficher :

```text
SKIPPED version=silver-v1
```

Pour contrôler les volumes publiés :

```sql
SELECT 'patients' AS objet, count() FROM silver.patients
UNION ALL SELECT 'stays', count() FROM silver.stays
UNION ALL SELECT 'stay_diagnoses', count() FROM silver.stay_diagnoses
UNION ALL SELECT 'monitoring', count() FROM silver.monitoring;
```

Pour examiner les anomalies :

```sql
SELECT severity, rule_code, count() AS events
FROM silver.quality_events
GROUP BY severity, rule_code
ORDER BY severity, events DESC;
```

Validation réalisée sur les lots Bronze fournis :

| Objet | Lignes publiées |
|---|---:|
| Patients | 6 000 |
| Services | 8 |
| CIM-10 | 10 |
| Séjours | 14 864 |
| Diagnostics de séjour | 37 040 |
| Monitoring | 64 799 |
| **Total Silver** | **122 721** |

Les 4 329 événements qualité se répartissent ainsi : 1 369 fréquences cardiaques hors
plage, 849 références vers un séjour rejeté, 136 sorties antérieures à l'admission et
1 975 modes de sortie manquants. Les contrôles finaux ne trouvent plus aucune date de
séjour incohérente, valeur de monitoring hors plage ou diagnostic orphelin dans les vues
Silver.

### Limites de cette étape

- les horodatages sans fuseau explicite sont interprétés en UTC ;
- les tables de versions conservent les publications précédentes pour la traçabilité ;
- les règles de calcul des alertes et des indicateurs métier appartiennent à Gold ;
- l'orchestration est rejouable et journalisée, mais sa planification périodique sera
  ajoutée dans une étape dédiée.

Pour arrêter ClickHouse sans supprimer ses volumes :

```bash
docker compose down
```
