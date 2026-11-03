SELECT max(r) FROM (SELECT sum(v) OVER (ORDER BY id) AS r FROM facts);
