# Partie 2 - Automatisation du pipeline

Ce dossier contiendra l'automatisation de la collecte et des transformations, avec
gestion des erreurs, journalisation, traçabilité et documentation de reprise.

## Principe d'architecture

```text
Filestorage (lecture seule) -> Lake -> Bronze -> Silver -> Gold
                               Python   SQL exécuté dans ClickHouse
```

Python pilotera la copie, la pseudonymisation à l'entrée du lake, le chargement et
l'envoi des requêtes. Les transformations Bronze vers Silver puis Gold resteront dans
ClickHouse. Aucune transformation métier ne sera effectuée avec pandas.

## Étape actuelle - socle ClickHouse

Cette première étape installe uniquement ClickHouse avec Docker et crée les bases vides :

- `bronze` ;
- `silver` ;
- `gold` ;
- `control`.

Les tables et le pipeline seront ajoutés dans les prochains commits.

## Lancement

Depuis `data/` :

```bash
cp .env.example .env
```

Modifier le mot de passe dans `.env`, puis lancer :

```bash
docker compose up -d
docker compose ps
```

L'interface SQL intégrée sera disponible à l'adresse suivante lorsque ClickHouse sera
en état `healthy` :

```text
http://localhost:8123/play
```

Pour arrêter le service sans supprimer les données :

```bash
docker compose down
```

La commande `docker compose down -v` supprime les volumes et ne doit être utilisée que
pour réinitialiser volontairement l'environnement de développement.
