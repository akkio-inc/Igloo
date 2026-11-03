SELECT count(*) FROM facts WHERE k1 IN (SELECT k FROM dims WHERE cat < 10);
