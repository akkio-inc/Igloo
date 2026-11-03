SELECT count(d.name), count(*) FROM facts f LEFT JOIN dims d ON d.k = f.k1;
