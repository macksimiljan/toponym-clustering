# case1: suffix nodes with only cities as children where city count is not correct
match (n:suffix)-[:IS_NAME_OF]->(c:city)
with n, count(c) as sumCities
where not (n)-[:IS_SUFFIX_OF]->()
and n.subsumedCities <> sumCities
return n, sumCities

# case2: suffix nodes with suffix and (optionally) citiy nodes as children where sum is not correct
match (n:suffix)-[:IS_SUFFIX_OF]->(s:suffix)
with n, sum(s.subsumedCities) as sumSuff
optional match (n)-[:IS_NAME_OF]->(c:city)
with n, sumSuff, count(c) as sumCities
where n.subsumedCities <> (sumSuff + sumCities)
return n, sumSuff, sumCities

# UPDATE: have to be repeated until no rows are effected
match (n:suffix)-[:IS_SUFFIX_OF]->(s:suffix)
with n, sum(s.subsumedCities) as sumSuff
optional match (n)-[:IS_NAME_OF]->(c:city)
with n, sumSuff, count(c) as sumCities
where n.subsumedCities <> (sumSuff + sumCities)
set n.subsumedCities = (sumSuff + sumCities)

# direct node context
match (n:suffix)-->(m) where n.str='pfuhl' return n,m

# CHECK: this should retun the number of cities, i.e. 70477
match (root:suffix)-->(n) where root.str=~'.' return sum(n.subsumedCities)
