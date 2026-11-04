SELECT count(*), sum(f.v) FROM facts f JOIN lists l ON l.k = f.k1;
