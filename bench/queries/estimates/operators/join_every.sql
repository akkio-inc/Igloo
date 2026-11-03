SELECT count(*), sum(a.v) FROM facts a JOIN facts_copy b ON b.id = a.id;
