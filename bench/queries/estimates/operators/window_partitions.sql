SELECT max(r) FROM (SELECT row_number() OVER (PARTITION BY k1 ORDER BY v) AS r FROM facts);
