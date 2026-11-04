SELECT x.kind, x.value, count(DISTINCT id) FROM facts, unnest([{'kind': 'a', 'value': s2}, {'kind': 'b', 'value': CAST(k3 AS VARCHAR)}, {'kind': 'c', 'value': 'all'}]) AS u(x) GROUP BY 1, 2;
