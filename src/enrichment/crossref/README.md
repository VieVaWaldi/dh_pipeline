# API: Crossref

Provides various meta-data given a doi. From bibliographic to funding data, license information, links etc. 
Find the  Docs here: https://www.crossref.org/documentation/retrieve-metadata/rest-api/

## Simple DOI Query  
* To fetch metadata for a DOI (e.g., `10.1037/0003-066X.59.1.29`):  
* https://api.crossref.org/works/10.1037/0003-066X.59.1.29
* Returns JSON with full metadata

**Key Parameters for Filtering**
* Use `select` to limit fields (e.g., `DOI,title,author`):
* https://api.crossref.org/works?select=DOI,title&filter=doi:10.1037/0003-066X.59.1.29
* Filter by `type` (e.g., `journal-article`) or `has-orcid:true` for ORCID-linked works

**JSON response schema**
* FInd that here https://github.com/crossref/rest-api-doc/blob/master/api_format.md

## Rate Limits & Performance  
- **Free tier**: 50 requests/second. Exceeding this triggers HTTP 429/503 errors
- **Polite pool**: Include `mailto` parameter (e.g., `?mailto=your@email.com`) for higher tolerance  
- **Bulk queries**: Use `cursor=*` for deep paging (avoids `offset` timeouts)

## Optimization Tips  
- **Narrow queries**: Use `filter` (e.g., `from-pub-date:2020`) to reduce scope
- **Avoid expensive queries**: Long `query.bibliographic` strings slow responses (e.g., multi-word titles)  
- **Batch DOIs**: Query multiple DOIs in one call via `/works?filter=doi:10.1000/123,10.2000/456`  

## Do’s and Don’ts  
- **Do**: URL-encode DOIs (e.g., `%2F` for `/`)
- **Do**: Cache responses to minimize repeat queries
- **Don’t**: Use non-Crossref DOIs (returns 404)