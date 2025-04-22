-----------------------------------------------
-- Analysing Work -- 196_614 publications

-----------------------------------------------
-- Function to use a subset of the data 

-- BUG: highlight entire query to execute on pgadmin when query cant be executed
CREATE OR REPLACE FUNCTION coreac.get_work_subset(search_terms text)
RETURNS SETOF coreac.work AS $$
BEGIN
    RETURN QUERY
    SELECT *
    FROM coreac.work
    WHERE 
	-- USE
	-- to_tsquery to allow |, &
	-- use phraseto_tsquery to search for exact phrases
        to_tsvector('english', title) @@ to_tsquery('english', search_terms) OR
        to_tsvector('english', COALESCE(abstract, '')) @@ to_tsquery('english', search_terms);
END;
$$ LANGUAGE plpgsql;

-----------------------------------------------
-- Results for subset with keywords

select count(*) from coreac.get_work_subset('cultural heritage');
-- 1_571 results for exact phrase
select count(*) from coreac.get_work_subset('cultural & heritage');
-- 2_003 results for & search

select count(*) from coreac.get_work_subset('digital heritage');
-- 87 results for exact phrase
select count(*) from coreac.get_work_subset('digital & heritage');
-- 1_050 results for & search

select count(*) from coreac.get_work_subset('jew | jewish | judaism | judaic');
-- 120 results for | search
select count(*) from coreac.get_work_subset('sami | saami | sámi | lapp | laplander');
-- 53 results for | search
select count(*) from coreac.get_work_subset('ladin | ladino | ladins | dolomites | dolomitic');
-- 16 results for | search

-----------------------------------------------
-- Completeness overview

SELECT
'With DOI' as metadata_type, COUNT(*) as count FROM coreac.get_work_subset('digital & heritage') WHERE doi IS NOT NULL
UNION ALL
SELECT 'With Publication Year' as metadata_type, COUNT(*) FROM coreac.get_work_subset('digital & heritage') WHERE year_published IS NOT NULL
UNION ALL
SELECT 'With Language Code' as metadata_type, COUNT(*) FROM coreac.get_work_subset('digital & heritage') WHERE language_code IS NOT NULL
UNION ALL
SELECT 'With Abstract' as metadata_type, COUNT(*) FROM coreac.get_work_subset('digital & heritage') WHERE abstract IS NOT NULL
UNION ALL
SELECT 'With Fulltext' as metadata_type, COUNT(*) FROM coreac.get_work_subset('digital & heritage') WHERE fulltext IS NOT NULL
UNION ALL
SELECT 'With References' as metadata_type, COUNT(DISTINCT w.id) FROM coreac.get_work_subset('digital & heritage') w JOIN coreac.j_work_reference wr ON w.id = wr.work_id
ORDER BY metadata_type;

-- All Papers
-- "With Abstract"	168736
-- "With DOI"	63280
-- "With Fulltext"	195447
-- "With Language Code"	111367
-- "With Publication Year"	196614
-- "With References"	4540

-- cultural & heritage
-- "With Abstract"	1918
-- "With DOI"	1037
-- "With Fulltext"	1789
-- "With Language Code"	997
-- "With Publication Year"	2003
-- "With References"	35

-- digital & heritage
-- "With Abstract"	1033
-- "With DOI"	540
-- "With Fulltext"	899
-- "With Language Code"	491
-- "With Publication Year"	1050
-- "With References"	17

-----------------------------------------------
-- Duplicate appearance of ID's

SELECT doi, COUNT(*) as count
FROM coreac.work
WHERE doi IS NOT NULL
GROUP BY doi
HAVING COUNT(*) > 1
ORDER BY count DESC;
-- 500 dois were used more than once

SELECT arxiv_id, COUNT(*) as count
FROM coreac.work
WHERE arxiv_id IS NOT NULL
GROUP BY arxiv_id
HAVING COUNT(*) > 1
ORDER BY count DESC;
-- 2 arxiv ids were used twice

SELECT mag_id, COUNT(*) as count
FROM coreac.work
WHERE mag_id IS NOT NULL
GROUP BY mag_id
HAVING COUNT(*) > 1
ORDER BY count DESC;
-- 2 mag ids were used twice

SELECT pubmed_id, COUNT(*) as count
FROM coreac.work
WHERE pubmed_id IS NOT NULL
GROUP BY pubmed_id
HAVING COUNT(*) > 1
ORDER BY count DESC;
-- 5 pubmed ids were used twice

-----------------------------------------------
-- Language

select language_code, count(*) as count
from coreac.get_work_subset('digital & heritage')
group by language_code
order by count desc;

-- All papers
-- "en"	97675
-- "null" 85247
-- "es"	5491
-- "pt"	4539
-- "de"	575

-- cultural & heritage
-- "null" 1006
-- "en"	857
-- "es"	45
-- "pt"	29
-- "it"	24

-- digital & heritage
-- "null" 559
-- "en"	414
-- "es"	29
-- "pt"	17
-- "it"	12

-----------------------------------------------
-- References

-- Note: I checked a couple of papers manually, coreac seems to only get the references for very few papers because they are in the PDFs

select count(distinct w.id) as papers_with_ref
from coreac.get_work_subset('cultural & heritage') as w
inner join coreac.j_work_reference as wr on w.id = wr.work_id;
-- 4k papers with at least one reference for All papers
-- so should be 96 references per paper on average for these

-- 45 papers with at least one reference for digital & heritage
-- 17 papers with at least one reference for digital & heritage

select count(*)
from coreac.work as w
where not exists (
	select 1 
	from coreac.j_work_reference as wr
	where wr.work_id = w.id
);
-- 192k papers without a reference

-----------------------------------------------
-- Temporal Analysis

-- Note: I dont have all papers for 2024 so far and none for 2025, thats why the number is lower

select year_published, count(*) as pub_count
from coreac.get_work_subset('digital & heritage')
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
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM coreac.work)), 2) AS percentage
FROM 
    coreac.work
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
    coreac.work
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
    coreac.get_work_subset('cultural & heritage')
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