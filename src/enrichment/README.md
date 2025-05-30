# Enrichment

pub = publication = ro = research output

## Topic Modelling
Ensure deduplication before this goes live.

Test with 1 batch first.
- Understand the average confidence intervals.

x sql alchemy model of publication (called ro) with (id, fulltext)
x Load and prep open alex topic csv. Turn keywords into sets with id's for the related information
x Batch Request script that gets batch_size entities from postgres DB
x Normalisation function where i want to just rely on python libs
5. Topic Assigning function. Takes full text, returns topic id with highest confidence depending on keyword density
x Then for each batch in parallel 
   x Normalize Batch 
   x assign topics 
   3. create junction and topic entity for all ROs in the DB

Things i forgot:
* logging
* understanding what i already processed, 
* Only get what i didnt process

-> also assign empty topic if below threshold so all pubs that have an open alex keyword topic,
can be ignored by the batch requester. then the br needs more input than just the RO.