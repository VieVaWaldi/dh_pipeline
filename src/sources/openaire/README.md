# Source: Openaire WIP 

* https://graph.openaire.eu/docs/
* https://explore.openaire.eu/search/advanced/projects?f0=q&fv0=cultural&f1=q,or&fv1=heritage

Graph Has cursor, but without sorting.
Getting orgs from graph seems impossible

Search only allows AND for queries, so we got to use multiple runs.

Probably the best strategy is to query by year. Error when results in year > 9000.

Make checkpoint easy and just query the projects again beginning from todays year.

Start at 1957-01-01 for first entry

1. search get project json https://api.openaire.eu/search/projects?keywords=cultural&sortBy=projectstartyear,descending&format=json&page=1&startYear=2023
    -> take project id
2. get research products: https://api.openaire.eu/graph/researchProducts?relProjectId=fwf_________::042f0852ba401b09c46635f1e0ee4a71&pageSize=100

## Understanding OpenAIRE's Funding Structure

@According to Claude and the open aire docs:

Based on the documentation, OpenAIRE represents funding relationships like this:

Projects have funders (like European Commission)
Funders have funding streams (like H2020, FP7)
Funding streams can be hierarchical (e.g., EC::H2020::RIA - Research and Innovation action)
Projects have grant information (currency, amount funded, total cost)
Some projects have specific programme information (like H2020 programmes)

