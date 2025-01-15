SELECT COUNT(*) FROM researchoutputs; -- 140.982

SELECT * FROM topics;

-- All RO's with an institiution 136.061 (and 4.921 with an institiution)
SELECT r.title, ir.institution_id, ir.publication_id
FROM researchoutputs AS r
LEFT JOIN institutions_researchoutputs AS ir ON r.id = ir.publication_id
WHERE institution_id IS NULL;

-- All institutions that have research outputs attached to them -- 4.921
SELECT i.name AS institution, r.title AS research_output
FROM institutions AS i
INNER JOIN institutions_researchoutputs AS ir ON i.id = ir.institution_id
INNER JOIN researchoutputs AS r ON r.id = ir.publication_id
-- WHERE i.name LIKE '%jena%'
ORDER BY i.name, r.title;

-- All RO's with a DOI 66.875 (or 74.107 without one)
SELECT r.id, r.title, d.doi
FROM researchoutputs as r
INNER JOIN dois AS d ON r.doi_id = d.id;

-- Using crossref we can  
--
