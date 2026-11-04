SELECT max(b.s1), max(b.s2), count(*) FROM facts a JOIN facts_copy b ON b.id = a.id;
