# Source: Arxiv

* [API](https://info.arxiv.org/help/api/basics.html#quickstart) we are using 
* [Data Schema](https://info.arxiv.org/help/api/user-manual.html#52-details-of-atom-results-returned)

## Data Model

1. Entry -> Publication
2. Author, junction to entry
3. Link, junction to entry

## Checkpoint

... The last startDate. The date where the project was started ...

## Rate Limits

* Fetch at once a batch of 2000 entries and add 3 seconds delay before fetching a new batch 
* Because of speed limitations in our implementation of the API, the maximum number of results returned from a single call (max_results) is limited to 30000 in slices of at most 2000 at a time, using the max_results and start query parameters. 
* A request for 30000 results will typically take a little over 2 minutes to return a response of over 15MB. Requests for fewer results are much faster and correspondingly smaller.

## Transformation notes

* Removed all @schema because they dont matter
* Must ensure category.@term, author.ns0:name and author.ns1:affiliation are a list
* category[_].@term and affiliation[_] are list fields for entry and author
