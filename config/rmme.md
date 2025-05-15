# Core Research Database Analysis Report

## Introduction

This report presents an analysis of the core research database, which primarily contains data on research outputs, institutions, and projects. This analysis focuses exclusively on research outputs , ignoring institutions and projects which are also main entities in the dataset. The analysis also does not yet include self-referential papers, though this is planned for future work.

## Data Model

The database is organized around the central entity `researchoutput`, which can represent various types of academic outputs including publications, data plans, reports, deliverables, and other research artifacts. The database uses a PostgreSQL schema with the following key entities:

- **Research Output**: The central entity storing research output data
- **Person**: Authors and contributors to research outputs
- **Topic**: Subject classifications of research outputs
- **Journal**: Publication venues

Each entity is connected through junction tables that establish many-to-many relationships (e.g., research outputs to authors, research outputs to topics).

## Dataset Description

The database contains a total of 451,800 research outputs across all three sources:

| Source   | Count   | Description | Qery |
|----------|---------|-------------|-----|
| CORDIS   | 204,372 | European Commission research project outputs | cultural OR heritage |
| COREAC   | 201,116 | Core.ac.uk publications | ((computing AND cultural) OR (computing AND heritage)) |
| ARXIV    | 46,312  | ArXiv preprints | all:computing+AND+(all:humanities+OR+all:heritage) |

Queries can be modified within the pipeline to adapt the dataset focus as needed.

### Metadata Coverage

The following table shows the coverage percentage of various metadata fields across the three sources:

| Source System | Total Papers | DOI Coverage % | Language Coverage % | Abstract Coverage % | Full Text Coverage % |
|---------------|--------------|----------------|--------------------|--------------------|---------------------|
| CORDIS        | 204,372      | 36.93          | 0.00               | 24.32              | 24.48               |
| COREAC        | 201,116      | 32.43          | 56.83              | 85.76              | 99.41               |
| ARXIV         | 46,312       | 16.94          | 0.00               | 100.00             | 95.84               |

### Additional Statistics

- **Topics**: 1,980 topics are cataloged
- **People**: 697,904 unique persons
- **Journals**: 44,259 unique journals
- **Links**: 891,010 links recorded

## Deduplication

The table above refers to the data before deduplication. After unifying all data to the core data model, multiple deduplication steps were required. This is still WIP and not reflected in the databse:

- Cordis research outputs with full text do not yet have a title
- Cordis outputs containing specific keywords were excluded to improve matching quality:
  - "data management"
  - "report"
  - "congress" 
  - "communication plan"
  - "dissemination strategy"
  - "call for the"

Together, these filtering approaches removed approximately 80,000 papers that could not be reliably processed for deduplication. This left about 350,000 papers for the deduplication process.

Using similarity title matching, approximately 70,000 duplicates were detected and consolidated.

3,112 papers were found reusing the same DOI.

## Content Analysis

### Top Journals

The most represented journals in the dataset are:

1. Our Mythical Childhood Survey (1,517 papers)
2. Scientific Reports (1,254 papers)
3. Nature Communications (854 papers)
4. Sustainability (569 papers)
5. Physical Review B (472 papers)

The average number of papers per journal is 2.5, suggesting a broad distribution of publications across many journals.

### Topic Analysis

Topic analysis is currently limited as only ArXiv has comprehensive topic information. The most prominent ArXiv topics are:

1. Computer Vision (CV)
2. Computation and Language (CL)
3. Artificial Intelligence (AI)
4. Machine Learning (LG)
5. Human-Computer Interaction (HC)
6. Computer and Society (CY)
7. Robotics (RO)
8. Image and Video Processing (eess.IV)
9. Machine Learning (stat.ML)
10. Neural and Evolutionary Computing (NE)

This reflects ArXiv's technical focus.

## Temporal Analysis

### Publications Per Year

I excluded the year 2024 and 2020 because i suspect there might be something wrong with those years.

Look at the figure "papers_per_year_per_source.png" which shows the yearly publication trends by source.

- **CORDIS**: Have to investigate the big drop in cordis for the last year further

### Seasonal Publication Patterns

Analysis of publication months reveals interesting patterns. Look at the figure "papers_per_month_per_source.png" which shows the sum of publications per month over all years.

- **COREAC**: Consistently shows higher publication counts in January compared to other months
- **CORDIS**: Shows clear seasonal patterns with publication peaks in May, October, and December

### Keyword Analysis

Text analysis of titles and abstracts reveals common themes and terminology in the research outputs. 

Look at the figures "wordcloud_titles.png" and "wordcloud_abstracts.png" which visualize the most frequent terms in titles and abstracts respectively. These word clouds highlight the predominant terminology across the research corpus.

## Figures

All referenced figures can be found in the accompanying zip file:

1. papers_per_month_per_source.png - Shows the sum of publications per month over all years
2. papers_per_year_per_source.png - Displays yearly publication trends by source
3. wordcloud_titles.png - Word cloud visualization of publication titles
4. wordcloud_abstracts.png - Word cloud visualization of publication abstracts