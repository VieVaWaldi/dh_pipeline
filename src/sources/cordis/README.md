# Source: Cordis

* [API & Data Schema](documentation/CORDIS_API_Defintion.pdf)
* [Searching](https://cordis.europa.eu/about/search/en) the cordis website
* [Swagger](https://cordis.europa.eu/dataextractions/api-docs-ui) for cordis

## Data Model

The Cordis data model is quite extensive, find more information in the pdf attached above. 

The CORDIS data schema is structured in XML format, with each field represented by an xPath. The schema is organized into several main content types, each containing a block of main content fields followed by a block of relations to other content. These relations include associations (links to other content) and categories (classification based on taxonomies and lists).

Main Components of the CORDIS Data Schema:
1. Projects
2. Programmes (including topics and legal bases)
3. Articles (news and briefs)
4. Results (including report summaries, project publications, and project deliverables)
5. Topics - which are modeled using EuroSciVoc, see more here [data.europa.eu](https://data.europa.eu/en/publications/datastories/linking-data-european-science-vocabulary) and under 'Advanced View' you can look at the hierarchy here
[op.europa.eu](https://op.europa.eu/en/web/eu-vocabularies/dataset/-/resource?uri=http://publications.europa.eu/resource/dataset/euroscivoc).

All with various metadata like organizations and authors etc.

## Checkpoint

So the big issue here is that projects may lie in the future. so we have to save the actual date of the last cordis extraction as a checkpoint.
Issue: we want the new extraction to overwrite the old data, therefore we want it to go in the same directory, meaning it would be sensible to use 
last_startDate_2020-01-01 for all data before 2025, and last_startDate_2025-01-01 as the stable checkpoint for all data until we reach the year 2030,
as dumb as that sounds thats the easiest way. It automatically overwrites the raw cordis data with the latest updates.
-> Always load from last checkpoint

## Rate Limits

* Only 1 extraction at the same time.
* 5 extractions saved at max, must remove old extractions.
* Limit of 25k records per extraction.
* Archived Content older than 5 years → Archived Content is enabled!


## Transformation notes

...

## Watch out

* API doesn't have sorting or max number of records (Web UI does have that though)