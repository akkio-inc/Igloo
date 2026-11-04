SELECT k2, quantile_cont(v, 0.9) FROM facts GROUP BY 1;
