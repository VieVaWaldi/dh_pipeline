# Source: Arxiv

## Important Links

*

## Data Model

1.

## Rate Limits

*

## Watch out

*

## Notes on document transformation

* Removed all @schema because they dont matter
* Must ensure category.@term, author.ns0:name and author.ns1:affiliation are a list
* category[_].@term and affiliation[_] are list fields for entry and author
