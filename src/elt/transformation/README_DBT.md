# Dbt

Dbt is used for the entire Transformation Process.

Never worked with dbt? Start here: https://www.blef.fr/get-started-dbt/

Docs are here: https://docs.getdbt.com/docs/introduction

ToDo - Do or remove this:
- Important: dbt-postgres installs psycopg2-binary by default, which is fine for development but not optimal for production PyPIdbt. For your 2-day sprint, stick with the default, but note for production you'd want to compile psycopg2 from source for better performance.

## Flow
go to src/elt/transformation -> This is the dbt working directory

## Dev vs Prod?!

## DB Connection

* Is under ~/.dbt/profiles/yml

## Key dbt Components

* Models: SQL files that define a transformation or query that produces a table or view in your data warehouse
* Sources: References to raw data tables already present in your warehouse
* Snapshots: Tools for tracking historical changes to your data
* Tests: Assertions about your data to ensure quality
* Documentation: Automatic generation of docs for your dbt project
* Macros: Reusable SQL snippets (similar to functions)

## Materialization Types

Dbt supports several ways to persist your transformed data:

* Table: Creates a table for each model
* View: Creates a view for each model
* Incremental: Updates only new/changed records rather than rebuilding entire tables
* Ephemeral: Does not persist in the database but can be referenced by other models
