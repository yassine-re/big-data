# Partie 1 - Interface d'analyse

Ce document est complété progressivement avec les choix réellement implémentés dans le
projet. Il constituera le dossier accompagnant les dashboards Metabase.

## 1. Besoin

Le CHU reçoit chaque jour des données provenant de plusieurs systèmes et dans plusieurs
formats. Le projet doit les centraliser, les fiabiliser et les restituer pour deux usages :

- le pilotage hospitalier : activité des services et qualité des soins ;
- la recherche clinique : constitution et description de cohortes.

La solution doit être incrémentale, rejouable et traçable. Elle doit également empêcher
l'entrée de données directement identifiantes dans l'entrepôt et séparer les accès des
deux publics.

## 2. Sources

Le filestorage fourni est considéré comme une source externe en lecture seule.

| Source | Format | Contenu utile |
|---|---|---|
| Patients | CSV | Identité source, naissance, sexe et région |
| Séjours | CSV | Patient, service, admission, sortie et modes de prise en charge |
| Diagnostics | JSON imbriqué | Codes CIM-10 et types de diagnostic par séjour |
| Monitoring | Parquet | Horodatage, fréquence cardiaque, SpO2 et température |
| Services | CSV | Référentiel des services |
| CIM-10 | CSV | Référentiel des diagnostics |

Les identifiants directs présents dans les fichiers patients sont nécessaires uniquement
à l'entrée du pipeline. Ils ne sont pas conservés dans le lake ni dans ClickHouse.

## 3. Architecture et justification

```mermaid
flowchart LR
    source[Filestorage CHU<br/>lecture seule]
    lake[Lake<br/>copie pseudonymisée]
    bronze[ClickHouse Bronze<br/>tables typées]
    silver[ClickHouse Silver<br/>nettoyage et qualité]
    gold[ClickHouse Gold<br/>indicateurs par usage]
    metabase[Metabase<br/>dashboards]

    source -->|Python| lake
    lake --> bronze
    bronze -->|SQL ClickHouse| silver
    silver -->|SQL ClickHouse| gold
    gold --> metabase
```

Choix retenus :

- le filestorage est monté en lecture seule afin de ne jamais modifier les dépôts du CHU ;
- Python copie les fichiers, applique la pseudonymisation obligatoire et pilotera les
  chargements et les requêtes ;
- ClickHouse exécutera les transformations Bronze vers Silver puis Gold en SQL ;
- aucune transformation métier ne sera réalisée en mémoire avec pandas ;
- Metabase fournira les interfaces d'analyse sans développement d'une application web.

Cette séparation permet au monitoring Parquet d'être traité par lots sans sortir les
données de ClickHouse lors des futurs nettoyages et agrégations.

## 4. Traitements

### 4.1 Traitement actuellement implémenté : entrée du lake

Le premier traitement fonctionnel réalise uniquement la copie et la protection des
identifiants. Il ne nettoie pas encore les données métier.

| Source | Traitement appliqué |
|---|---|
| Patients | HMAC-SHA-256 de `patient_id`, conservation de l'année de naissance, suppression du NIR, du nom et du prénom |
| Séjours | HMAC-SHA-256 de `stay_id` et `patient_id` avec des secrets distincts |
| Diagnostics | Remplacement de `stay_id` par `stay_sk`, sans modifier les diagnostics imbriqués |
| Monitoring | Remplacement de `stay_id` par `stay_sk`, lecture et écriture Parquet par lots de 10 000 lignes |
| Référentiels | Copie identique des fichiers CSV |

Les pseudonymes sont déterministes : un même identifiant produit la même clé dans les
différents fichiers et permet donc les futures jointures. Les secrets restent en dehors
du dépôt Git.

Le traitement a été validé sur les 14 fichiers fournis. Les sorties ne contiennent plus
les colonnes directement identifiantes et les pseudonymes sont cohérents entre les
séjours, les diagnostics et le monitoring.

### 4.2 Chargement Bronze actuellement implémenté

ClickHouse lit directement les fichiers pseudonymisés avec la fonction SQL `file()`.
Python se limite à découvrir les fichiers, calculer leur checksum et envoyer les requêtes
SQL. Les lignes ne sont pas chargées dans une structure pandas.

| Table Bronze | Contenu |
|---|---|
| `bronze.patients` | snapshots patients pseudonymisés |
| `bronze.stays` | séjours avec dates et modes conservés comme valeurs brutes |
| `bronze.stay_diagnoses` | diagnostics aplatis par `ARRAY JOIN` dans ClickHouse |
| `bronze.monitoring` | constantes typées lues directement depuis Parquet |
| `bronze.services` | référentiel des services |
| `bronze.cim10` | référentiel des diagnostics |

Chaque ligne contient le jour source, le chemin du fichier, son checksum, un identifiant
de lot et l'horodatage de chargement. La table `control.ingested_files` empêche de charger
deux fois un fichier ayant le même chemin et le même contenu.

Les dates de séjour restent en texte dans Bronze. Cette décision permet de conserver une
ligne mal formée jusqu'aux contrôles Silver au lieu de faire échouer l'ensemble du lot.

Le test de chargement a enregistré 14 fichiers au statut `SUCCESS` et 135 275 lignes
Bronze. Un rejeu identique a ignoré les 14 fichiers sans modifier les volumes. Le schéma
Bronze ne contient aucune colonne directement identifiante.

### 4.3 Transformation Silver actuellement implémentée

La transformation est exécutée avec des requêtes `INSERT ... SELECT` dans ClickHouse.
Python calcule l'identifiant de l'exécution, transmet le SQL et journalise son statut ;
il ne charge jamais les lignes métier en mémoire.

Silver utilise directement la nomenclature du modèle analytique : `dim_patients`,
`dim_services`, `dim_cim10`, `fact_stays`, `fact_stay_diagnoses` et `fact_monitoring`.
La table technique des anomalies est nommée `fact_quality_events`. Chacune contient son
`run_id` ; aucune table intermédiaire suffixée par `_versions` n'est utilisée.

Pour chaque exécution, Silver conserve une ligne canonique par patient, séjour et
référentiel, une ligne par diagnostic de séjour et une ligne par relevé de monitoring.
Les traitements comprennent :

- la sélection du dernier snapshot disponible et la déduplication ;
- la normalisation des codes, libellés, sexes, modes et types de diagnostic ;
- la conversion des dates de séjour, le calcul de leur durée en minutes et de l'âge
  approximatif du patient à l'admission ;
- le calcul de l'indicateur individuel de réadmission dans les 30 jours suivant la sortie
  du séjour précédent ;
- la vérification des liens patient–séjour, service–séjour, séjour–diagnostic et
  séjour–monitoring ;
- le contrôle des plages techniques du monitoring ;
- le calcul des alertes individuelles du monitoring avec une règle versionnée ;
- l'enregistrement des rejets et avertissements avec leur fichier, jour et lot source.

Les plages 20–250 pour la fréquence cardiaque, 50–100 pour la SpO2 et 30–45 °C pour la
température servent à éliminer les valeurs techniquement impossibles. Sur les relevés
restants, la règle `monitoring-alert-v1` signale une SpO2 inférieure à 92 %, une fréquence
cardiaque inférieure à 50 ou supérieure à 100 bpm, et une température supérieure à
38,5 °C. `is_alert` vaut 1 dès qu'au moins l'une de ces conditions est satisfaite. Les
agrégations quotidiennes seront construites dans Gold.

`fact_stay_diagnoses` contient le `patient_sk` récupéré depuis le séjour. Un diagnostic
peut ainsi être relié directement à `dim_patients` pour constituer une cohorte. L'âge
individuel est calculé dans `fact_stays` avec la formule
`année d'admission - année de naissance`. Il est nommé `age_at_admission_approx`, car la
suppression de la date de naissance complète entraîne une précision à un an près. Les
tranches d'âge et les distributions agrégées seront construites dans Gold.

Les traitements Gold sélectionneront uniquement le `run_id` retourné par
`control.v_latest_successful_silver_run`. Une exécution en cours ou en échec n'est donc
pas utilisée. Le rejeu d'une même version avec les mêmes lots Bronze est ignoré.

Sur les données fournies, Silver publie 122 721 lignes : 6 000 patients, 8 services,
10 codes CIM-10, 14 864 séjours, 37 040 diagnostics et 64 799 relevés. Les 4 329
événements qualité restent consultables dans `silver.fact_quality_events`. Les tables du
dernier `run_id` réussi ne contiennent aucune sortie antérieure à l'admission, valeur de
monitoring hors plage technique ou diagnostic orphelin.

Les enrichissements Silver identifient 748 réadmissions à 30 jours et 5 192 relevés en
alerte. Les compteurs unitaires des séjours, diagnostics et relevés permettront de
construire les sommes et taux Gold sans recompter les grains.

### 4.4 Traitements à venir

- calcul des indicateurs dans Gold ;
- planification périodique de la collecte et des transformations ;
- journalisation globale du pipeline et procédure de reprise sur incident.

## 5. Indicateurs attendus

Les indicateurs viennent du besoin métier. Ils seront documentés avec leur requête et
leur résultat uniquement après la construction de Gold.

### Pilotage hospitalier

- durée moyenne de séjour par service ;
- activité des urgences par jour ;
- taux de réadmission à 30 jours ;
- relevés de constantes en alerte par jour.

### Recherche clinique

- taille des cohortes par diagnostic ;
- distribution d'une cohorte par âge et sexe ;
- masquage des groupes de moins de cinq patients.

## 6. Visualisations attendues

Deux dashboards Metabase seront construits après validation des vues Gold :

- un dashboard Pilotage hospitalier ;
- un dashboard Recherche clinique.

Le cloisonnement sera démontré avec des droits ClickHouse et Metabase distincts. Aucune
visualisation n'est annoncée comme terminée à cette étape.

## 7. Limites et recommandations actuelles

- le lake réécrit un fichier existant sous le même chemin et ne conserve pas encore ses
  anciennes versions ;
- les horodatages sans fuseau explicite sont actuellement interprétés en UTC ;
- l'âge à l'admission est approximatif à un an près ;
- les requêtes sur Silver doivent sélectionner le dernier `run_id` réussi ;
- aucune donnée n'est encore disponible dans Gold ;
- les seuils d'alerte `monitoring-alert-v1` devront être validés par le métier avant un
  usage réel ;
- les règles et chiffres finaux devront rester justifiables par leur fichier source et
  leur date de traitement.
