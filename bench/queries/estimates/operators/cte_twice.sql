WITH c AS MATERIALIZED (SELECT k1, sum(v) AS s FROM facts GROUP BY 1) SELECT count(*) FROM c a JOIN c b ON b.k1 = a.k1;
