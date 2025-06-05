# Source: Arxiv

* [See the API here](https://info.arxiv.org/help/api/basics.html#quickstart)
* [See the Data Schema here](https://info.arxiv.org/help/api/user-manual.html#52-details-of-atom-results-returned)

Sort these notes
* Arxiv automatically stems words eg same results for query: digital or digital, and humanities or human

My new query
http://export.arxiv.org/api/query?search_query=
(all:digital+OR+all:heritage+OR+all:humanities+OR+all:cultural)
+AND+submittedDate:[199001010000+TO+202605010000]
&sortBy=submittedDate&sortOrder=ascending

[1990 01 01 00 00+TO+2026 05 01 00 00]



## Data Model

1. Entry -> Publication
2. Author, junction to entry
3. Link, junction to entry

## Checkpoint



NOOOO:
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

## Watch Out

* Something Arxiv doenst return entries, even though it has them. Give the server some time with an exponential backoff or try later.