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
-- Temporal Analysis

-- Note: I dont have all papers for 2024 so far and none for 2025, thats why the number is lower

select extract(year from published_date) as year, count(*) as pub_count
from arxiv.entry
where published_date is not null
group by year
order by year;

-- All papers
-- 1993	1
-- 1994	20
-- 1995	19
-- 1996	7
-- 1997	14
-- 1998	10
-- 1999	18
-- 2000	13
-- 2001	15
-- 2002	26
-- 2003	34
-- 2004	36
-- 2005	45
-- 2006	51
-- 2007	76
-- 2008	92
-- 2009	130
-- 2010	198
-- 2011	217
-- 2012	311
-- 2013	467
-- 2014	598
-- 2015	161
-- 2016	516
-- 2017	1899
-- 2018	2752
-- 2019	2831
-- 2020	3999
-- 2021 ????? 0 ... ! ! ! ! ???? error????
-- 2022	2000, sounds weird
-- 2023	2000, sounds weird
-- 2024	2042

-- cultural & heritage

-- digital & heritage

-----------------------------------------------
-- Most frequent authors


-----------------------------------------------
-- Most frequent Journal Refs

SELECT 
    journal_ref,
    COUNT(*) as publication_count
FROM 
    arxiv.entry
WHERE 
    journal_ref IS NOT NULL
GROUP BY 
    journal_ref
ORDER BY 
    publication_count DESC
LIMIT 10;

-- "Journal of Computing, Vol. 2, No. 6, June 2010, NY, USA, ISSN 2151-9617"	4
-- "EMNLP 2020"	4
-- "Proceedings of the 2023 CHI Conference on Human Factors in Computing Systems"	4
-- "2021 IEEE International Conference on Robotics and Automation (ICRA)"	3
-- "CVPR 2018"	3
-- "CVPR 2023"	3
-- "ECCV 2018"	3
-- "CVPR 2019"	3
-- "AAAI 2020"	3
-- "IEEE Transactions on Pattern Analysis and Machine Intelligence, 2021"	3

