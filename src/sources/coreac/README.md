# Source: CoreAc

## Important Links

* API Docs https://api.core.ac.uk/docs/v3
* Work API https://api.core.ac.uk/docs/v3#tag/Works <-- We are using this API
* Get Work by original_id https://api.core.ac.uk/v3/works/{original_id}

## Data Model
1. Work
2. Link junction to work
3. Reference junction to work
4. DataProvider junction to work

* I removed the identifiers for simplification, but we have the original_id's anyway

## Rate Limits

Use Academic which gives you 200.000 tokens per day. Its 10k tokens for being registered and 1k when unauthenticated.

* Simple queries cost 1 token; complex operations (recommender, scroll search, bulk) cost 3-5 tokens
* Token costs may change based on server load
* Monitor usage via HTTP headers: X-RateLimitRemaining, X-RateLimit-Retry-After, and X-RateLimit-Limit
* Academic and Non-academic subscriptions designed for reasonable usage with limits that may change based on server load

## Watch out

* Authors seem to be sorted alphabetically

## Notes on document transformation
* We keep all id's from the source and rename them to id_original
* oai_ids[_], authors[_].name, contributors[_], journals[_].title, outputs[_]  and sourceFulltextUrls[_] are simply attached as string lists to the work entity
* language_code and language_name are the unpacked values of the language object, we use both values directly in the work entity
* For journals we only want to save the titles as TEXT[] in the work entity 
* We ignore the identifiers because we have the original IDs