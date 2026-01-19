# Affiliation Meditatio
Les données d'affiliation Meditatio sont obtenues comme ceci :  
1- Récupération des fichiers Adjust sur S3 dans un unique CSV  
2- Traitement du fichier unique CSV( ajout de colonnes)  
3- TCS dans Google Sheet  

Ce projet permet de générer et traiter des fichiers CSV Adjust stockés sur l'AWS S3 meditatio.

## Prérequis

- Python 3.8+
- le gestionnaire d'environnements [uv](https://github.com/astral-sh/uv)
- Un compte AWS avec accès au S3 de meditatio (A obtenir avec equipe tech Meditatio)

## Installation de uv

```bash
pip install uv
```

## Configuration des credentials AWS S3

Configurez vos identifiants AWS dans `~/.aws/credentials` ou via les variables d'environnement :

```bash
export AWS_ACCESS_KEY_ID=VOTRE_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=VOTRE_SECRET_KEY
export AWS_DEFAULT_REGION=eu-west-1
```

## Installation des dépendances du projet

A la racine du projet :

```bash
uv sync
```

## Utilisation

### Générer le CSV

```bash
uv run generate_csv.py
```

### Traiter le CSV

```bash
uv run process_csv.py
```

Un fichier `processed_attribution_data.csv`sera généré et pourra être exploité pour les TCD
