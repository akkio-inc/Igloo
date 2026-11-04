SELECT max(n) FROM (SELECT v / sum(v) OVER () AS n FROM facts);
