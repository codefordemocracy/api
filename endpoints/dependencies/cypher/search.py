#########################################################
# search for entities
#########################################################

def graph_search_candidates(tx, cand_name, cand_pty_affiliation, cand_office, cand_office_st, cand_office_district, cand_election_yr, cand_ici, context, skip, limit, concise):
    c = ""
    if cand_name is not None:
        c+= "CALL db.index.fulltext.queryNodes('candidate_name', $cand_name) "
        c+= "YIELD node, score "
        c+= "WHERE score > 2 "
        c+= "WITH node AS a "
    if context is True:
        c+= "MATCH p=(a)<-[:ASSOCIATED_WITH]-(x:Committee) "
    else:
        c+= "MATCH p=(a) "
    c+= "WHERE a:Candidate "
    if cand_pty_affiliation is not None:
        c+= "AND a.cand_pty_affiliation = toUpper($cand_pty_affiliation) "
    if cand_office is not None:
        c+= "AND a.cand_office = toUpper($cand_office) "
    if cand_office_st is not None:
        c+= "AND a.cand_office_st = toUpper($cand_office_st) "
    if cand_office_district is not None:
        c+= "AND a.cand_office_district = $cand_office_district "
    if cand_election_yr is not None:
        c+= "AND a.cand_election_yr = $cand_election_yr "
    if cand_ici is not None:
        c+= "AND a.cand_ici = toUpper($cand_ici) "
    if concise is True:
        c+= "RETURN a.cand_id AS cand_id "
    else:
        c+= "RETURN p "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    response = tx.run(c, cand_name=cand_name, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, skip=skip, limit=limit)
    if concise is True:
        return response.data()
    else:
        return response.graph()

def graph_search_committees(tx, cmte_nm, cmte_pty_affiliation, cmte_dsgn, cmte_tp, context, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day, concise):
    c = ""
    if cmte_nm is not None:
        c+= "CALL db.index.fulltext.queryNodes('committee_name', $cmte_nm) "
        c+= "YIELD node, score "
        c+= "WHERE score > 2 "
        c+= "WITH node AS a "
    if context is True:
        c+= "MATCH (a)<-[:CONTRIBUTED_TO]-(x:Contribution)<-[:CONTRIBUTED_TO]-(c) "
        c+= "MATCH (x)-[:HAPPENED_ON]->(b:Day) "
        c+= "MATCH p=(a)-[:CONTRIBUTED_TO]-(c) "
    else:
        c+= "MATCH p=(a) "
    c+= "WHERE a:Committee "
    if cmte_pty_affiliation is not None:
        c+= "AND a.cmte_pty_affiliation = toUpper($cmte_pty_affiliation) "
    if cmte_dsgn is not None:
        c+= "AND a.cmte_dsgn = toUpper($cmte_dsgn) "
    if cmte_tp is not None:
        c+= "AND a.cmte_tp = toUpper($cmte_tp) "
    if context is True:
        c+= "AND b.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND b.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if concise is True:
        c+= "RETURN a.cmte_id AS cmte_id "
    else:
        c+= "RETURN p "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    response = tx.run(c, cmte_nm=cmte_nm, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    if concise is True:
        return response.data()
    else:
        return response.graph()

def graph_search_donors(tx, name, employer, occupation, state, zip_code, entity_tp, context, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day, concise):
    c = ""
    if name is not None:
        c+= "CALL db.index.fulltext.queryNodes('donor_name', $name) "
        c+= "YIELD node, score "
        c+= "WHERE score > 2 "
        c+= "WITH collect(node) AS nodes "
    if employer is not None:
        c+= "CALL db.index.fulltext.queryNodes('donor_employer', $employer) "
        c+= "YIELD node, score "
        c+= "WHERE score > 2 "
        if name is not None:
            c+= "WITH apoc.coll.intersection(nodes, collect(node)) AS nodes "
        else:
            c+= "WITH collect(node) AS nodes "
    if occupation is not None:
        c+= "CALL db.index.fulltext.queryNodes('donor_occupation', $occupation) "
        c+= "YIELD node, score "
        c+= "WHERE score > 2 "
        if name is not None or employer is not None:
            c+= "WITH apoc.coll.intersection(nodes, collect(node)) AS nodes "
        else:
            c+= "WITH collect(node) AS nodes "
    if name is not None or employer is not None or occupation is not None:
        c+= "UNWIND nodes AS a "
    if context is True:
        c+= "MATCH (a)-[:CONTRIBUTED_TO]->(x:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
        c+= "MATCH (x)-[:HAPPENED_ON]->(b:Day) "
        c+= "MATCH p=(a)-[:CONTRIBUTED_TO]-(c) "
    else:
        c+= "MATCH p=(a) "
    c+= "WHERE a:Donor "
    if state is not None:
        c+= "AND a.state = toUpper($state) "
    if zip_code is not None:
        c+= "AND a.zip_code = toString($zip_code) "
    if entity_tp is not None:
        c+= "AND a.entity_tp = toUpper($entity_tp) "
    if context is True:
        c+= "AND b.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND b.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if concise is True:
        c+= "RETURN a.name AS name, a.zip_code as zip_code "
    else:
        c+= "RETURN p "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    response = tx.run(c, name=name, employer=employer, occupation=occupation, state=state, zip_code=zip_code, entity_tp=entity_tp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    if concise is True:
        return response.data()
    else:
        return response.graph()

def graph_search_payees(tx, name, context, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day, concise):
    c = ""
    if name is not None:
        c+= "CALL db.index.fulltext.queryNodes('payee_name', $name) "
        c+= "YIELD node, score "
        c+= "WITH node AS a "
    if context is True:
        c+= "MATCH p=(a:Payee)<-[:PAID]-(c:Expenditure)<-[:SPENT]-(:Committee) "
        c+= "MATCH (c)-[:HAPPENED_ON]->(b:Day)"
        if context is True:
            c+= "WHERE b.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
            c+= "AND b.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
        c+= "OPTIONAL MATCH q=(c)-[:IDENTIFIES]->(:Candidate) "
    else:
        c+= "MATCH p=(a) "
    if concise is True:
        c+= "RETURN a.name AS name "
    else:
        if context is True:
            c+= "RETURN p, q "
        else:
            c+= "RETURN p "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    response = tx.run(c, name=name, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    if concise is True:
        return response.data()
    else:
        return response.graph()

def graph_search_tweeters(tx, name, username, candidate, cand_pty_affiliation, cand_election_yr, context, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day, concise):
    c = ""
    if name is not None:
        c+= "CALL db.index.fulltext.queryNodes('tweeter_name', $name) "
        c+= "YIELD node, score "
        c+= "WHERE score > 2 "
        c+= "WITH collect(node) AS nodes "
    if username is not None:
        c+= "CALL db.index.fulltext.queryNodes('tweeter_username', $username) "
        c+= "YIELD node, score "
        c+= "WHERE score > 2 "
        if name is not None:
            c+= "WITH apoc.coll.intersection(nodes, collect(node)) AS nodes "
        else:
            c+= "WITH collect(node) AS nodes "
    if name is not None or username is not None:
        c+= "UNWIND nodes AS a "
    if candidate is True:
        c+= "MATCH (a)<-[:ASSOCIATED_WITH]-(c) "
        c+= "WHERE c:Candidate "
        if cand_pty_affiliation is not None:
            c+= "AND c.cand_pty_affiliation = toUpper($cand_pty_affiliation) "
        if cand_election_yr is not None:
            c+= "AND c.cand_election_yr = $cand_election_yr "
    if context is True:
        c+= "MATCH p=(a)<-[:PUBLISHED_BY]-(:Tweet)-[:PUBLISHED_ON]->(b:Day) "
    else:
        c+= "MATCH p=(a) "
    c+= "WHERE a:Tweeter "
    if context is True:
        c+= "AND b.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND b.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if concise is True:
        c+= "RETURN a.user_id AS user_id "
    else:
        c+= "RETURN p "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    response = tx.run(c, name=name, username=username, cand_pty_affiliation=cand_pty_affiliation, cand_election_yr=cand_election_yr, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    if concise is True:
        return response.data()
    else:
        return response.graph()

def graph_search_sources(tx, domain, bias_score, factually_questionable_flag, conspiracy_flag, hate_group_flag, propaganda_flag, satire_flag, context, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day, concise):
    c = ""
    if domain is not None:
        c+= "CALL db.index.fulltext.queryNodes('source_domain', $domain) "
        c+= "YIELD node, score "
        c+= "WHERE score > 2 "
        c+= "WITH node AS a "
    if context is True:
        c+= "MATCH p=(a)<-[:ASSOCIATED_WITH]-(:Domain)<-[:ASSOCIATED_WITH]-(c:Link)<-[:MENTIONS]-(:Tweet)-[:PUBLISHED_ON]->(b:Day) "
    else:
        c+= "MATCH p=(a) "
    c+= "WHERE a:Source "
    if bias_score is not None:
        c+= "AND a.bias_score IN $bias_score "
    if factually_questionable_flag == 1:
        c+= "AND a.factually_questionable_flag = 1 "
    if conspiracy_flag == 1:
        c+= "AND a.conspiracy_flag = 1 "
    if hate_group_flag == 1:
        c+= "AND a.hate_group_flag = 1 "
    if propaganda_flag == 1:
        c+= "AND a.propaganda_flag = 1 "
    if satire_flag == 1:
        c+= "AND a.satire_flag = 1 "
    if context is True:
        c+= "AND b.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND b.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if concise is True:
        c+= "RETURN a.domain AS domain "
    else:
        c+= "RETURN p "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    response = tx.run(c, domain=domain, bias_score=bias_score, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    if concise is True:
        return response.data()
    else:
        return response.graph()

def graph_search_buyers(tx, name, context, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day, concise):
    c = ""
    if name is not None:
        c+= "CALL db.index.fulltext.queryNodes('buyer_name', $name) "
        c+= "YIELD node, score "
        c+= "WHERE score > 1 "
        c+= "WITH node AS a "
    if context is True:
        c+= "MATCH p=(a)<-[:PAID_BY]-(c:Ad)-[:CREATED_ON|DELIVERED_ON]->(b:Day) "
        c+= "OPTIONAL MATCH q=(c)-[:PUBLISHED_BY]->(:Page) "
    else:
        c+= "MATCH p=(a) "
    c+= "WHERE a:Buyer "
    if context is True:
        c+= "AND b.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND b.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if concise is True:
        c+= "RETURN a.name AS name "
    else:
        c+= "RETURN p "
        if context is True:
            c+= ", q "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    response = tx.run(c, name=name, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    if concise is True:
        return response.data()
    else:
        return response.graph()

def graph_search_pages(tx, name, context, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day, concise):
    c = ""
    if name is not None:
        c+= "CALL db.index.fulltext.queryNodes('page_name', $name) "
        c+= "YIELD node, score "
        c+= "WHERE score > 1 "
        c+= "WITH node AS a "
    if context is True:
        c+= "MATCH p=(a)<-[:PUBLISHED_BY]-(c:Ad)-[:CREATED_ON|DELIVERED_ON]->(b:Day) "
        c+= "OPTIONAL MATCH q=(c)-[:PAID_BY]->(:Buyer) "
    else:
        c+= "MATCH p=(a) "
    c+= "WHERE a:Page "
    if context is True:
        c+= "AND b.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND b.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if concise is True:
        c+= "RETURN a.name AS name "
    else:
        c+= "RETURN p "
        if context is True:
            c+= ", q "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    response = tx.run(c, name=name, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    if concise is True:
        return response.data()
    else:
        return response.graph()
