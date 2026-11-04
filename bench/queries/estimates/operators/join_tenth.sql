SELECT d.cat, sum(f.v) FROM facts f JOIN dims d ON d.k = f.k1 GROUP BY 1;
