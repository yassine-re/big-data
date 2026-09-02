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
Premier chargement : 89 fichiers SUCCESS, 79 316 lignes Bronze
Rejeu identique    : 0 fichier chargé, 89 fichiers SKIPPED
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

### Tables Silver

| Table Silver | Grain | Traitements principaux |
|---|---|---|
| `silver.dim_patients` | un patient par exécution | dernier snapshot, sexe normalisé, année de naissance et région contrôlées |
| `silver.dim_services` | un service par exécution | dernier libellé non vide par code |
| `silver.dim_cim10` | un code CIM-10 par exécution | dernier libellé non vide par code normalisé |
| `silver.fact_stays` | un séjour par exécution | dates et modes normalisés, durée, compteur et réadmission à 30 jours calculés |
| `silver.fact_stay_diagnoses` | un diagnostic par séjour, code, type et exécution | ajout du patient et de son âge approximatif, déduplication, contrôles et compteur unitaire |
| `silver.fact_monitoring` | un relevé par séjour, horodatage et exécution | contrôles, compteur et indicateurs d'alerte versionnés |
| `silver.fact_quality_events` | une anomalie par ligne, règle et exécution | motif, sévérité et lignage de chaque rejet ou avertissement |

La nomenclature `dim_`/`fact_` matérialise directement le modèle analytique retenu. Il
n'existe pas de table intermédiaire suffixée par `_versions`. Le `run_id` est une colonne
de chacune des tables classiques et fait partie de leur clé de tri ClickHouse.

Les valeurs conservées dans Silver respectent notamment les règles suivantes :

- une sortie ne peut pas précéder l'admission ;
- `age_at_diagnosis_approx` est calculé par `année d'admission du séjour - année de naissance` et
  doit être compris entre 0 et 120 ans ;
- `is_readmission_30d` vaut 1 lorsque l'admission survient entre 0 et 30 jours après la
  sortie du séjour précédent du même patient, les séjours étant ordonnés par admission ;
- les modes d'admission acceptés sont `urgence`, `programme` et `mutation` ;
- les types de diagnostic acceptés sont `principal` et `associe` ;
- un diagnostic doit référencer un séjour Silver et un code du référentiel CIM-10 ;
- un relevé doit référencer un séjour Silver ;
- les plages techniques sont 20–250 pour la fréquence cardiaque, 50–100 pour la SpO2
  et 30–45 °C pour la température.

Ces plages éliminent les valeurs techniquement impossibles. Elles ne constituent pas
les seuils d'alerte. Après ce contrôle, `monitoring-alert-v1` applique les règles métier
suivantes : SpO2 strictement inférieure à 92 %, fréquence cardiaque strictement inférieure
à 50 ou supérieure à 100 bpm, température strictement supérieure à 38,5 °C. Les colonnes
`heart_rate_alert`, `spo2_alert` et `temp_alert` identifient chaque motif ; `is_alert`
vaut 1 si au moins l'un d'eux est présent.

Les colonnes `stay_count`, `diagnosis_count` et `reading_count` valent 1 au grain de leur
table et pourront être additionnées dans Gold. Le `patient_sk` est propagé dans
`fact_stay_diagnoses` afin de joindre directement un diagnostic à `dim_patients`. Un
mode de sortie absent sur un séjour terminé produit un événement `WARNING`, mais le
séjour reste utilisable.

### Traçabilité et rejeu

`control.silver_runs` conserve le statut de chaque transformation. Les consommateurs de
Silver et les futurs calculs Gold doivent sélectionner le `run_id` de la dernière
exécution réussie :

```sql
WHERE run_id = (
    SELECT run_id
    FROM control.v_latest_successful_silver_run
)
```

Cette condition empêche une exécution `RUNNING` ou `FAILED` d'être utilisée. L'identifiant
dépend de l'empreinte Bronze et de la version `silver-v5` : un rejeu strictement identique
est ignoré. Lorsqu'une règle SQL change, la version de transformation doit être
incrémentée pour autoriser une nouvelle exécution.

### Exécution

Après le chargement Bronze :

```bash
docker compose up -d clickhouse
docker compose run --rm --build silver-transformer
```

Une seconde exécution sans nouveau lot Bronze doit afficher :

```text
SKIPPED version=silver-v5
```

Pour contrôler les volumes publiés :

```sql
WITH (
    SELECT run_id FROM control.v_latest_successful_silver_run
) AS current_run
SELECT 'dim_patients' AS objet, count()
FROM silver.dim_patients FINAL WHERE run_id = current_run
UNION ALL
SELECT 'fact_stays', count()
FROM silver.fact_stays FINAL WHERE run_id = current_run
UNION ALL
SELECT 'fact_stay_diagnoses', count()
FROM silver.fact_stay_diagnoses FINAL WHERE run_id = current_run
UNION ALL
SELECT 'fact_monitoring', count()
FROM silver.fact_monitoring FINAL WHERE run_id = current_run;
```

Pour examiner les anomalies :

```sql
SELECT severity, rule_code, count() AS events
FROM silver.fact_quality_events FINAL
WHERE run_id = (
    SELECT run_id FROM control.v_latest_successful_silver_run
)
GROUP BY severity, rule_code
ORDER BY severity, events DESC;
```

Validation réalisée sur les lots Bronze fournis :

| Objet | Lignes publiées |
|---|---:|
| Patients | 6 000 |
| Services | 8 |
| CIM-10 | 13 |
| Séjours | 6 729 |
| Diagnostics de séjour | 12 593 |
| Monitoring | 40 400 |
| **Total Silver** | **65 743** |

Les 1 573 événements qualité se répartissent ainsi : 858 fréquences cardiaques hors
plage technique, 647 références vers un séjour rejeté et 68 sorties antérieures à
l'admission. Les contrôles finaux ne trouvent plus aucune date de séjour incohérente,
valeur de monitoring hors plage ou diagnostic orphelin dans les tables Silver du dernier
`run_id` réussi.

Le contrôle de l'enrichissement confirme que `fact_stays` ne contient aucune colonne
d'âge. Les 12 593 valeurs `age_at_diagnosis_approx` sont non nulles, différentes de zéro,
comprises entre 1 et 95 ans et correspondent toutes au calcul documenté. Chaque diagnostic
porte également le même `patient_sk` que son séjour.

La dernière exécution identifie 780 réadmissions à 30 jours. Sur les 40 400 relevés
Silver, 1 091 déclenchent l'alerte de fréquence cardiaque, 1 108 l'alerte SpO2 et 1 071
l'alerte de température. Au total, 3 270 relevés présentent au moins une alerte ; un
même relevé peut apparaître dans plusieurs catégories.

### Limites de cette étape

- les horodatages sans fuseau explicite sont interprétés en UTC ;
- l'âge associé au diagnostic est approximatif à un an près, car seule l'année de
  naissance est conservée après pseudonymisation ;
- les tables Silver conservent les exécutions successives grâce à leur colonne `run_id` ;
- toute requête Silver doit filtrer le dernier `run_id` au statut `SUCCESS` ;
- les seuils `monitoring-alert-v1` sont des règles projet à faire valider par le métier
  avant un usage réel ;
- Gold calculera les taux et agrégations à partir des indicateurs individuels Silver ;
- l'orchestration est rejouable et journalisée, mais sa planification périodique sera
  ajoutée dans une étape dédiée.

Pour arrêter ClickHouse sans supprimer ses volumes :

```bash
docker compose down
```
