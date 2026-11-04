SELECT e, count(*) FROM facts, unnest(facts.tags) AS u(e) GROUP BY 1;
