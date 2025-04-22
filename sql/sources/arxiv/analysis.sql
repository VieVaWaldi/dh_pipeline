-----------------------------------------------
-- Analysing Work -- 20_598 publications

-----------------------------------------------
-- Function to use a subset of the data 

-- BUG: highlight entire query to execute on pgadmin when query cant be executed
CREATE OR REPLACE FUNCTION arxiv.get_entry_subset(search_terms text)
RETURNS SETOF arxiv.entry AS $$
BEGIN
    RETURN QUERY
    SELECT *
    FROM arxiv.entry
    WHERE 
	-- USE
	-- to_tsquery to allow |, &
	-- use phraseto_tsquery to search for exact phrases
        to_tsvector('english', title) @@ phraseto_tsquery('english', search_terms) OR
        to_tsvector('english', COALESCE(summary, '')) @@ phraseto_tsquery('english', search_terms);
END;
$$ LANGUAGE plpgsql;

-----------------------------------------------
-- Results for subset with keywords

select count(*) from arxiv.get_entry_subset('cultural heritage');
-- 71 results for exact phrase
select count(*) from arxiv.get_entry_subset('cultural & heritage');
-- 76 results for & search

select count(*) from arxiv.get_entry_subset('digital heritage');
-- 5 results for exact phrase
select count(*) from arxiv.get_entry_subset('digital & heritage');;
-- 25 results for & search

select count(*) from arxiv.get_entry_subset('jew | jewish | judaism | judaic');
-- 2 results for | search
select count(*) from arxiv.get_entry_subset('sami | saami | sámi | lapp | laplander');
-- 0 results for | search
select count(*) from arxiv.get_entry_subset('ladin | ladino | ladins | dolomites | dolomitic');
-- 0 results for | search

-----------------------------------------------
-- Completeness overview

SELECT
'With title' as metadata_type, COUNT(*) as count FROM arxiv.entry WHERE title IS NOT NULL
UNION ALL
SELECT 'With DOI' as metadata_type, COUNT(*) FROM arxiv.entry WHERE doi IS NOT NULL
UNION ALL
SELECT 'With summary' as metadata_type, COUNT(*) FROM arxiv.entry WHERE summary IS NOT NULL
UNION ALL
SELECT 'With full_text' as metadata_type, COUNT(*) FROM arxiv.entry WHERE full_text IS NOT NULL
UNION ALL
SELECT 'With journal_ref' as metadata_type, COUNT(*) FROM arxiv.entry WHERE journal_ref IS NOT NULL
UNION ALL
SELECT 'With published_date' as metadata_type, COUNT(*) FROM arxiv.entry WHERE published_date IS NOT NULL
ORDER BY metadata_type;

-- All Papers 20598
-- "With DOI"	3830
-- "With full_text"	18755
-- "With journal_ref"	3204
-- "With published_date"	20598
-- "With summary"	20598
-- "With title"	20598

-- cultural & heritage

-- digital & heritage

-----------------------------------------------
-- Duplicate appearance of ID's

SELECT doi, COUNT(*) as count
FROM arxiv.entry
WHERE doi IS NOT NULL
GROUP BY doi
HAVING COUNT(*) > 1
ORDER BY count DESC;
-- 1 doi was used twice

-----------------------------------------------
-- References

-- Note: I checked a couple of papers manually, arxiv seems to only get the references for very few papers because they are in the PDFs

select count(distinct w.id) as papers_with_ref
from arxiv.entry as w
inner join arxiv.j_entry_reference as wr on w.id = wr.entry_id;

-- 4k papers with at least one reference for All papers
-- so should be 96 references per paper on average for these

-- 45 papers with at least one reference for digital & heritage
-- 17 papers with at least one reference for digital & heritage

select count(*)
from arxiv.entry as w
where not exists (
	select 1 
	from arxiv.j_entry_reference as wr
	where wr.entry_id = w.id
);
-- 192k papers without a reference

-----------------------------------------------
-- Temporal Analysis

-- Note: I dont have all papers for 2024 so far and none for 2025, thats why the number is lower

select year_published, count(*) as pub_count
from arxiv.entry
where year_published is not null
group by year_published
order by year_published;

-- All papers
-- "1990"	508
-- "1991"	1073
-- "1992"	1194
-- "1993"	1126
-- "1994"	1354
-- "1995"	1505
-- "1996"	1559
-- "1997"	1580
-- "1998"	1649
-- "1999"	1785
-- "2000"	2070
-- "2001"	2102
-- "2002"	2285
-- "2003"	2629
-- "2004"	3213
-- "2005"	3345
-- "2006"	3626
-- "2007"	3679
-- "2008"	4051
-- "2009"	4293
-- "2010"	4847
-- "2011"	5385
-- "2012"	6324
-- "2013"	6895
-- "2014"	7970
-- "2015"	9472
-- "2016"	10456
-- "2017"	10974
-- "2018"	12513
-- "2019"	13108
-- "2020"	13828
-- "2021"	14292
-- "2022"	16805
-- "2023"	16010
-- "2024"	3101

-- cultural & heritage
-- "1991"	2
-- "1992"	1
-- "1993"	1
-- "1994"	1
-- "1995"	2
-- "1996"	1
-- "1997"	1
-- "1998"	1
-- "1999"	7
-- "2000"	5
-- "2001"	10
-- "2002"	12
-- "2003"	8
-- "2004"	19
-- "2005"	15
-- "2006"	21
-- "2007"	26
-- "2008"	23
-- "2009"	31
-- "2010"	40
-- "2011"	43
-- "2012"	62
-- "2013"	50
-- "2014"	93
-- "2015"	113
-- "2016"	132
-- "2017"	137
-- "2018"	144
-- "2019"	193
-- "2020"	164
-- "2021"	179
-- "2022"	212
-- "2023"	214
-- "2024"	40

-- digital & heritage
-- "1998"	1
-- "2000"	2
-- "2001"	6
-- "2002"	4
-- "2003"	5
-- "2004"	12
-- "2005"	9
-- "2006"	10
-- "2007"	13
-- "2008"	15
-- "2009"	15
-- "2010"	16
-- "2011"	25
-- "2012"	27
-- "2013"	24
-- "2014"	50
-- "2015"	62
-- "2016"	83
-- "2017"	71
-- "2018"	84
-- "2019"	96
-- "2020"	76
-- "2021"	101
-- "2022"	114
-- "2023"	111
-- "2024"	18

-----------------------------------------------
-- Document Type Distribution

SELECT 
    document_type,
    COUNT(*) as count,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM arxiv.entry)), 2) AS percentage
FROM 
    arxiv.entry
GROUP BY 
    document_type
ORDER BY 
    count DESC;

-----------------------------------------------
-- Most frequent authors USELESS INFO

SELECT 
    unnest(authors) as author,
    COUNT(*) as publication_count
FROM 
    arxiv.entry
GROUP BY 
    author
ORDER BY 
    publication_count DESC
LIMIT 20;

-----------------------------------------------
-- Most frequent publishers

SELECT 
    publisher,
    COUNT(*) as publication_count
FROM 
    arxiv.get_entry_subset('cultural & heritage')
WHERE 
    publisher IS NOT NULL
GROUP BY 
    publisher
ORDER BY 
    publication_count DESC
LIMIT 10;

-- All papers
-- "'Elsevier BV'"	3755
-- "'Springer Science and Business Media LLC'"	3622
-- "AIS Electronic Library (AISeL)"	3255
-- "'MDPI AG'"	2311
-- "'Informa UK Limited'"	2045
-- "'Wiley'"	1710
-- "'Association for Computing Machinery (ACM)'"	1382
-- "The University of Edinburgh"	1226
-- "UCL (University College London)"	1107
-- "'IntechOpen'"	1089

-- cultural & heritage
-- "'Universitat Politecnica de Valencia'"	75
-- "'MDPI AG'"	74
-- "'Springer Science and Business Media LLC'"	56
-- "'Copernicus GmbH'"	52
-- "'Association for Computing Machinery (ACM)'"	48
-- "'Elsevier BV'"	35
-- "'Informa UK Limited'"	28
-- "HAL CCSD"	27
-- "'Institute of Electrical and Electronics Engineers (IEEE)'"	26
-- "Alma Mater Studiorum - Università di Bologna"	16

-- digital & heritage
-- "'Universitat Politecnica de Valencia'"	62
-- "'Copernicus GmbH'"	33
-- "'MDPI AG'"	31
-- "Hunter Library Digital Collections, Western Carolina University, Cullowhee, NC 28723;"	30
-- "'Association for Computing Machinery (ACM)'"	28
-- "'Informa UK Limited'"	20
-- "'Springer Science and Business Media LLC'"	19
-- "'Elsevier BV'"	12
-- "HAL CCSD"	12
-- "'Institute of Electrical and Electronics Engineers (IEEE)'"	8