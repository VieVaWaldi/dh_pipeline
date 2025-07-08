-----------------------------------------------
-- Cordis Proxy Field Search by Keyword


SELECT *
FROM cordis.project
WHERE (
    to_tsvector('english', title) @@ to_tsquery('english', 'digital & heritage')
    OR to_tsvector('english', coalesce(objective, '')) @@ to_tsquery('english', 'digital & heritage')
)
AND start_date >= '2015-01-01';
-- AND start_date <= '2024-07-01';

SELECT *
FROM cordis.project
WHERE to_tsvector('english', title || ' ' || coalesce(objective, '')) @@ to_tsquery('english', 'cultural & heritage')
AND start_date >= '2015-01-01';

-- cultural & heritage: 1098, 612 since 2015 (8 more if concat both)
-- digital & heritage: 358, 210 since 2015 (6 more if concat both)

-- Cordis Data Model