# Source: Arxiv

* [API](https://info.arxiv.org/help/api/basics.html#quickstart) we are using 
* [Data Schema](https://info.arxiv.org/help/api/user-manual.html#52-details-of-atom-results-returned)

## Data Model

1. Entry -> Publication
2. Author, junction to entry
3. Link, junction to entry

## Checkpoint

Arxiv data gets returned as total results.
We simply use the index 'start' and a 'range' to get the next n results.
Simple restart query from last start

## Rate Limits

* Fetch at once a batch of 2000 entries and add 3 seconds delay before fetching a new batch 
* Because of speed limitations in our implementation of the API, the maximum number of results returned from a single call (max_results) is limited to 30000 in slices of at most 2000 at a time, using the max_results and start query parameters. 
* A request for 30000 results will typically take a little over 2 minutes to return a response of over 15MB. Requests for fewer results are much faster and correspondingly smaller.

## Transformation notes

* Removed all @schema because they dont matter
* Must ensure category.@term, author.ns0:name and author.ns1:affiliation are a list
* category[_].@term and affiliation[_] are list fields for entry and author


## WTF ...

so yeah, 2k range is lil bit too much

[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ll -t
total 0
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 11  2024 last_start_46000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 11  2024 last_start_44000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 11  2024 last_start_42000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 11  2024 last_start_40000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 11  2024 last_start_38000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 11  2024 last_start_36000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 11  2024 last_start_34000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 11  2024 last_start_32000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 11  2024 last_start_30000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_28000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_26000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_24000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_22000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_20000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_18000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_16000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_14000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_12000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_10000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_8000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_6000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug 10  2024 last_start_4000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug  9  2024 last_start_2000
drwx------ 2 lu72hip domänen-benutzer 4096 Aug  9  2024 last_start_0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_4000 | wc -l
2001
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_6000 | wc -l
2001
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ 
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_8000 | wc -l
2001
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_10000 | wc -l
2001
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_12000 | wc -l
0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_14000 | wc -l
2001
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_16000 | wc -l
2001
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_18000 | wc -l
0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_20000 | wc -l
0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_22000 | wc -l
0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_24000 | wc -l
0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_26000 | wc -l
2001
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_28000 | wc -l
0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_30000 | wc -l
2001
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_32000 | wc -l
0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_34000 | wc -l
0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_36000 | wc -l
0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_40000 | wc -l
0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_42000 | wc -l
0
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ ls last_start_46000 | wc -l
43
[lu72hip@login2 arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB]$ 