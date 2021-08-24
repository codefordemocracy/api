#########################################################
# find graph elements
#########################################################

def graph_find_elements_id(tx, nodes, edges):
    c = "MATCH (a) WHERE ID(a) IN $nodes "
    if edges is not None:
        c+= "OPTIONAL MATCH (a)-[r]-() WHERE ID(r) IN $edges "
    c+= "RETURN a"
    if edges is not None:
        c+= ", r"
    return tx.run(c, nodes=nodes, edges=edges).graph()

def graph_find_elements_uuid(tx, nodes, edges):
    c = "OPTIONAL MATCH (ad:Ad) WHERE ad.uuid in $nodes "
    c+= "WITH collect(ad) AS nodes "
    c = "OPTIONAL MATCH (annotation:Annotation) WHERE annotation.uuid in $nodes "
    c+= "WITH collect(annotation) AS nodes "
    c+= "OPTIONAL MATCH (buyer:Buyer) WHERE buyer.uuid in $nodes "
    c+= "WITH collect(buyer) + nodes AS nodes "
    c+= "OPTIONAL MATCH (candidate:Candidate) WHERE candidate.uuid in $nodes "
    c+= "WITH collect(candidate) + nodes AS nodes "
    c+= "OPTIONAL MATCH (committee:Committee) WHERE committee.uuid in $nodes "
    c+= "WITH collect(committee) + nodes AS nodes "
    c+= "OPTIONAL MATCH (contribution:Contribution) WHERE contribution.uuid in $nodes "
    c+= "WITH collect(contribution) + nodes AS nodes "
    c+= "OPTIONAL MATCH (day:Day) WHERE day.uuid in $nodes "
    c+= "WITH collect(day) + nodes AS nodes "
    c+= "OPTIONAL MATCH (domain:Domain) WHERE domain.uuid in $nodes "
    c+= "WITH collect(domain) + nodes AS nodes "
    c+= "OPTIONAL MATCH (donor:Donor) WHERE donor.uuid in $nodes "
    c+= "WITH collect(donor) + nodes AS nodes "
    c+= "OPTIONAL MATCH (employer:Employer) WHERE employer.uuid in $nodes "
    c+= "WITH collect(employer) + nodes AS nodes "
    c+= "OPTIONAL MATCH (expenditure:Expenditure) WHERE expenditure.uuid in $nodes "
    c+= "WITH collect(expenditure) + nodes AS nodes "
    c+= "OPTIONAL MATCH (hashtag:Hashtag) WHERE hashtag.uuid in $nodes "
    c+= "WITH collect(hashtag) + nodes AS nodes "
    c+= "OPTIONAL MATCH (job:Job) WHERE job.uuid in $nodes "
    c+= "WITH collect(job) + nodes AS nodes "
    c+= "OPTIONAL MATCH (link:Link) WHERE link.uuid in $nodes "
    c+= "WITH collect(link) + nodes AS nodes "
    c+= "OPTIONAL MATCH (message:Message) WHERE message.uuid in $nodes "
    c+= "WITH collect(message) + nodes AS nodes "
    c+= "OPTIONAL MATCH (month:Month) WHERE month.uuid in $nodes "
    c+= "WITH collect(month) + nodes AS nodes "
    c+= "OPTIONAL MATCH (page:Page) WHERE page.uuid in $nodes "
    c+= "WITH collect(page) + nodes AS nodes "
    c+= "OPTIONAL MATCH (party:Party) WHERE party.uuid in $nodes "
    c+= "WITH collect(party) + nodes AS nodes "
    c+= "OPTIONAL MATCH (payee:Payee) WHERE payee.uuid in $nodes "
    c+= "WITH collect(payee) + nodes AS nodes "
    c+= "OPTIONAL MATCH (race:Race) WHERE race.uuid in $nodes "
    c+= "WITH collect(race) + nodes AS nodes "
    c+= "OPTIONAL MATCH (source:Source) WHERE source.uuid in $nodes "
    c+= "WITH collect(source) + nodes AS nodes "
    c+= "OPTIONAL MATCH (state:State) WHERE state.uuid in $nodes "
    c+= "WITH collect(state) + nodes AS nodes "
    c+= "OPTIONAL MATCH (tweet:Tweet) WHERE tweet.uuid in $nodes "
    c+= "WITH collect(tweet) + nodes AS nodes "
    c+= "OPTIONAL MATCH (tweeter:Tweeter) WHERE tweeter.uuid in $nodes "
    c+= "WITH collect(tweeter) + nodes AS nodes "
    c+= "OPTIONAL MATCH (year:Year) WHERE year.uuid in $nodes "
    c+= "WITH collect(year) + nodes AS nodes "
    c+= "OPTIONAL MATCH (zip:Zip) WHERE zip.uuid in $nodes "
    c+= "WITH collect(zip) + nodes AS nodes "
    c+= "UNWIND nodes AS x "
    if edges is not None:
        c+= "OPTIONAL MATCH (x)-[r]-() WHERE r.uuid IN $edges "
    c+= "RETURN x "
    if edges is not None:
        c+= ", r"
    return tx.run(c, nodes=nodes, edges=edges).graph()

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
        c+= "MATCH p=(a)<-[:CONTRIBUTED_TO]-(x:Contribution)<-[:CONTRIBUTED_TO]-() "
        c+= "MATCH (x)-[:HAPPENED_ON]->(b:Day)"
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
        c+= "MATCH p=(a)-[:CONTRIBUTED_TO]->(x:Contribution)-[:CONTRIBUTED_TO]->(:Committee) "
        c+= "MATCH (x)-[:HAPPENED_ON]->(b:Day)"
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

#########################################################
# traverse graph
#########################################################

def graph_traverse_neighbors(tx, ids, labels, skip, limit):
    c = "MATCH (a)-[x]-(b) "
    c+= "WHERE ID(a) IN $ids "
    if labels is not None:
        c+= "AND (" + labels + ") "
    c+= "RETURN a, x, b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, skip=skip, limit=limit).graph()

def graph_traverse_associations_candidate_committee(tx, ids, ids2, cmte_pty_affiliation, cmte_dsgn, cmte_tp, intermediaries, sup_opp, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    if intermediaries == "linkage":
        c = "MATCH (a:Candidate)<-[:ASSOCIATED_WITH]-(b:Committee) "
        c+= "WHERE ID(a) IN $ids "
    else:
        c = "MATCH (a:Candidate)<-[:IDENTIFIES]-(d:Expenditure)<-[:SPENT]-(b:Committee) "
        c+= "MATCH (d)-[:HAPPENED_ON]->(e:Day) "
        c+= "WHERE ID(a) IN $ids "
        c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
        if sup_opp is not None:
            c+= "AND d.sup_opp = toUpper($sup_opp) "
    if ids2 is not None:
        if intermediaries == "linkage":
            c+= "MATCH (t:Candidate)<-[:ASSOCIATED_WITH]-(b:Committee) "
            c+= "WHERE ID(t) IN $ids2 "
        else:
            c+= "MATCH (t:Candidate)<-[:IDENTIFIES]-(u:Expenditure)<-[:SPENT]-(b:Committee) "
            c+= "MATCH (u)-[:HAPPENED_ON]->(v:Day) "
            c+= "WHERE ID(t) IN $ids2 "
            c+= "AND v.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
            c+= "AND v.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
            if sup_opp is not None:
                c+= "AND u.sup_opp = toUpper($sup_opp) "
    if cmte_pty_affiliation is not None:
        c+= "AND b.cmte_pty_affiliation = toUpper($cmte_pty_affiliation) "
    if cmte_dsgn is not None:
        c+= "AND b.cmte_dsgn = toUpper($cmte_dsgn) "
    if cmte_tp is not None:
        c+= "AND b.cmte_tp = toUpper($cmte_tp) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, sup_opp=sup_opp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_candidate_tweeter(tx, ids, ids2, skip, limit):
    c = "MATCH (a:Candidate)-[:ASSOCIATED_WITH]->(b:Tweeter) "
    c+= "WHERE ID(a) IN $ids "
    if ids2 is not None:
        c+= "MATCH (t:Candidate)-[:ASSOCIATED_WITH]->(b:Tweeter) "
        c+= "WHERE ID(t) IN $ids2 "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, skip=skip, limit=limit).graph()

def graph_traverse_associations_committee_candidate(tx, ids, ids2, cand_pty_affiliation, cand_office, cand_office_st, cand_office_district, cand_election_yr, cand_ici, intermediaries, sup_opp, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    if intermediaries == "linkage":
        c = "MATCH (a:Committee)-[:ASSOCIATED_WITH]->(b:Candidate) "
        c+= "WHERE ID(a) IN $ids "
    else:
        c = "MATCH (a:Committee)-[:SPENT]->(d:Expenditure)-[:IDENTIFIES]->(b:Candidate) "
        c+= "MATCH (d)-[:HAPPENED_ON]->(e:Day) "
        c+= "WHERE ID(a) IN $ids "
        c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
        if sup_opp is not None:
            c+= "AND d.sup_opp = toUpper($sup_opp) "
    if ids2 is not None:
        if intermediaries == "linkage":
            c = "MATCH (t:Committee)-[:ASSOCIATED_WITH]->(b:Candidate) "
            c+= "WHERE ID(t) IN $ids2 "
        else:
            c = "MATCH (t:Committee)-[:SPENT]->(u:Expenditure)-[:IDENTIFIES]->(b:Candidate) "
            c+= "MATCH (u)-[:HAPPENED_ON]->(v:Day) "
            c+= "WHERE ID(t) IN $ids2 "
            c+= "AND v.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
            c+= "AND v.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
            if sup_opp is not None:
                c+= "AND u.sup_opp = toUpper($sup_opp) "
    if cand_pty_affiliation is not None:
        c+= "AND b.cand_pty_affiliation = toUpper($cand_pty_affiliation) "
    if cand_office is not None:
        c+= "AND b.cand_office = toUpper($cand_office) "
    if cand_office_st is not None:
        c+= "AND b.cand_office_st = toUpper($cand_office_st) "
    if cand_office_district is not None:
        c+= "AND b.cand_office_district = $cand_office_district "
    if cand_election_yr is not None:
        c+= "AND b.cand_election_yr = $cand_election_yr "
    if cand_ici is not None:
        c+= "AND b.cand_ici = toUpper($cand_ici) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, sup_opp=sup_opp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_committee_committee(tx, ids, ids2, cmte_pty_affiliation, cmte_dsgn, cmte_tp, direction, intermediaries, sup_opp, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    if intermediaries == "contribution":
        if direction == "receipts":
            c = "MATCH (a:Committee)<-[:CONTRIBUTED_TO]-(c:Contribution)<-[:CONTRIBUTED_TO]-(b:Committee) "
        elif direction == "disbursements":
            c = "MATCH (a:Committee)-[:CONTRIBUTED_TO]->(c:Contribution)-[:CONTRIBUTED_TO]->(b:Committee) "
        else:
            c = "MATCH (a:Committee)-[:CONTRIBUTED_TO]-(c:Contribution)-[:CONTRIBUTED_TO]-(b:Committee) "
        c+= "MATCH (c)-[:HAPPENED_ON]->(d:Day) "
        c+= "WHERE ID(a) IN $ids "
        c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    else:
        c = "MATCH (a:Committee)-[:SPENT]->(c:Expenditure)-[:IDENTIFIES]->(:Candidate)<-[IDENTIFIES]-(d:Expenditure)<-[:SPENT]-(b:Committee) "
        c+= "MATCH (c)-[:HAPPENED_ON]->(e:Day)<-[:HAPPENED_ON]-(d) "
        c+= "WHERE ID(a) IN $ids "
        c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
        if sup_opp is not None:
            c+= "AND c.sup_opp = toUpper($sup_opp) "
            c+= "AND d.sup_opp = toUpper($sup_opp) "
    if ids2 is not None:
        if intermediaries == "contribution":
            if direction == "receipts":
                c+= "MATCH (t:Committee)<-[:CONTRIBUTED_TO]-(u:Contribution)<-[:CONTRIBUTED_TO]-(b:Committee) "
            elif direction == "disbursements":
                c+= "MATCH (t:Committee)-[:CONTRIBUTED_TO]->(u:Contribution)-[:CONTRIBUTED_TO]->(b:Committee) "
            else:
                c+= "MATCH (t:Committee)-[:CONTRIBUTED_TO]-(u:Contribution)-[:CONTRIBUTED_TO]-(b:Committee) "
            c+= "MATCH (u)-[:HAPPENED_ON]->(v:Day) "
            c+= "WHERE ID(t) IN $ids2 "
            c+= "AND v.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
            c+= "AND v.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
        else:
            c+= "MATCH (t:Committee)-[:SPENT]->(u:Expenditure)-[:IDENTIFIES]->(:Candidate)<-[IDENTIFIES]-(v:Expenditure)<-[:SPENT]-(b:Committee) "
            c+= "MATCH (u)-[:HAPPENED_ON]->(w:Day)<-[:HAPPENED_ON]-(v) "
            c+= "WHERE ID(t) IN $ids2 "
            c+= "AND w.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
            c+= "AND w.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
            if sup_opp is not None:
                c+= "AND u.sup_opp = toUpper($sup_opp) "
                c+= "AND v.sup_opp = toUpper($sup_opp) "
    if cmte_pty_affiliation is not None:
        c+= "AND b.cmte_pty_affiliation = toUpper($cmte_pty_affiliation) "
    if cmte_dsgn is not None:
        c+= "AND b.cmte_dsgn = toUpper($cmte_dsgn) "
    if cmte_tp is not None:
        c+= "AND b.cmte_tp = toUpper($cmte_tp) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, sup_opp=sup_opp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_committee_donor(tx, ids, ids2, employer, occupation, state, zip_code, entity_tp, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Committee)<-[:CONTRIBUTED_TO]-(c:Contribution)<-[:CONTRIBUTED_TO]-(b:Donor) "
    c+= "MATCH (c)-[:HAPPENED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if ids2 is not None:
        c+= "MATCH (t:Committee)<-[:CONTRIBUTED_TO]-(u:Contribution)<-[:CONTRIBUTED_TO]-(b:Donor) "
        c+= "MATCH (u)-[:HAPPENED_ON]->(v:Day) "
        c+= "WHERE ID(t) IN $ids2 "
        c+= "AND v.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND v.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if employer is not None:
        c+= "AND b.employer CONTAINS toUpper($employer) "
    if occupation is not None:
        c+= "AND b.occupation CONTAINS toUpper($occupation) "
    if state is not None:
        c+= "AND b.state = toUpper($state) "
    if zip_code is not None:
        c+= "AND b.zip_code = toString($zip_code) "
    if entity_tp is not None:
        c+= "AND b.entity_tp = toUpper($entity_tp) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, employer=employer, occupation=occupation, state=state, zip_code=zip_code, entity_tp=entity_tp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_committee_payee(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Committee)-[:SPENT]->(c:Expenditure)-[:PAID]->(b:Payee) "
    c+= "MATCH (c)-[:HAPPENED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if ids2 is not None:
        c+= "MATCH (t:Committee)-[:SPENT]->(u:Expenditure)-[:PAID]->(b:Payee) "
        c+= "MATCH (u)-[:HAPPENED_ON]->(v:Day) "
        c+= "WHERE ID(t) IN $ids2 "
        c+= "AND v.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND v.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_donor_committee(tx, ids, ids2, cmte_pty_affiliation, cmte_dsgn, cmte_tp, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Donor)-[:CONTRIBUTED_TO]->(c:Contribution)-[:CONTRIBUTED_TO]->(b:Committee) "
    c+= "MATCH (c)-[:HAPPENED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if ids2 is not None:
        c+= "MATCH (t:Donor)-[:CONTRIBUTED_TO]->(u:Contribution)-[:CONTRIBUTED_TO]->(b:Committee) "
        c+= "MATCH (u)-[:HAPPENED_ON]->(v:Day) "
        c+= "WHERE ID(t) IN $ids2 "
        c+= "AND v.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND v.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if cmte_pty_affiliation is not None:
        c+= "AND b.cmte_pty_affiliation = toUpper($cmte_pty_affiliation) "
    if cmte_dsgn is not None:
        c+= "AND b.cmte_dsgn = toUpper($cmte_dsgn) "
    if cmte_tp is not None:
        c+= "AND b.cmte_tp = toUpper($cmte_tp) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_payee_committee(tx, ids, ids2, cmte_pty_affiliation, cmte_dsgn, cmte_tp, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Payee)<-[:PAID]-(c:Expenditure)<-[:SPENT]-(b:Committee) "
    c+= "MATCH (c)-[:HAPPENED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if ids2 is not None:
        c+= "MATCH (t:Payee)<-[:PAID]-(u:Expenditure)<-[:SPENT]-(b:Committee) "
        c+= "MATCH (u)-[:HAPPENED_ON]->(v:Day) "
        c+= "WHERE ID(t) IN $ids2 "
        c+= "AND v.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND v.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if cmte_pty_affiliation is not None:
        c+= "AND b.cmte_pty_affiliation = toUpper($cmte_pty_affiliation) "
    if cmte_dsgn is not None:
        c+= "AND b.cmte_dsgn = toUpper($cmte_dsgn) "
    if cmte_tp is not None:
        c+= "AND b.cmte_tp = toUpper($cmte_tp) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_tweeter_candidate(tx, ids, ids2, cand_pty_affiliation, cand_office, cand_office_st, cand_office_district, cand_election_yr, cand_ici, skip, limit):
    c = "MATCH (a:Tweeter)<-[:ASSOCIATED_WITH]-(b:Candidate) "
    c+= "WHERE ID(a) IN $ids "
    if ids2 is not None:
        c+= "MATCH (t:Tweeter)<-[:ASSOCIATED_WITH]-(b:Candidate) "
        c+= "WHERE ID(t) IN $ids2 "
    if cand_pty_affiliation is not None:
        c+= "AND b.cand_pty_affiliation = toUpper($cand_pty_affiliation) "
    if cand_office is not None:
        c+= "AND b.cand_office = toUpper($cand_office) "
    if cand_office_st is not None:
        c+= "AND b.cand_office_st = toUpper($cand_office_st) "
    if cand_office_district is not None:
        c+= "AND b.cand_office_district = $cand_office_district "
    if cand_election_yr is not None:
        c+= "AND b.cand_election_yr = $cand_election_yr "
    if cand_ici is not None:
        c+= "AND b.cand_ici = toUpper($cand_ici) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, skip=skip, limit=limit).graph()

def graph_traverse_associations_tweeter_source(tx, ids, ids2, bias_score, factually_questionable_flag, conspiracy_flag, hate_group_flag, propaganda_flag, satire_flag, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Tweeter)<-[:PUBLISHED_BY]-(c:Tweet)-[:MENTIONS]->(:Link)-[:ASSOCIATED_WITH]-(:Domain)-[:ASSOCIATED_WITH]->(b:Source) "
    c+= "MATCH (c)-[:PUBLISHED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if ids2 is not None:
        c+= "MATCH (t:Tweeter)<-[:PUBLISHED_BY]-(u:Tweet)-[:MENTIONS]->(:Link)-[:ASSOCIATED_WITH]-(:Domain)-[:ASSOCIATED_WITH]->(b:Source) "
        c+= "MATCH (u)-[:PUBLISHED_ON]->(v:Day) "
        c+= "WHERE ID(t) IN $ids2 "
        c+= "AND v.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND v.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if bias_score is not None:
        c+= "AND b.bias_score IN $bias_score "
    if factually_questionable_flag == 1:
        c+= "AND b.factually_questionable_flag = 1 "
    if conspiracy_flag == 1:
        c+= "AND b.conspiracy_flag = 1 "
    if hate_group_flag == 1:
        c+= "AND b.hate_group_flag = 1 "
    if propaganda_flag == 1:
        c+= "AND b.propaganda_flag = 1 "
    if satire_flag == 1:
        c+= "AND b.satire_flag = 1 "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, bias_score=bias_score, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_source_tweeter(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Source)<-[:ASSOCIATED_WITH]-(:Domain)<-[:ASSOCIATED_WITH]-(:Link)<-[:MENTIONS]-(c:Tweet)-[:PUBLISHED_BY]->(b:Tweeter) "
    c+= "MATCH (c)-[:PUBLISHED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if ids2 is not None:
        c+= "MATCH (t:Source)<-[:ASSOCIATED_WITH]-(:Domain)<-[:ASSOCIATED_WITH]-(:Link)<-[:MENTIONS]-(u:Tweet)-[:PUBLISHED_BY]->(b:Tweeter) "
        c+= "MATCH (u)-[:PUBLISHED_ON]->(v:Day) "
        c+= "WHERE ID(t) IN $ids2 "
        c+= "AND v.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND v.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_buyer_page(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Buyer)<-[:PAID_BY]-(c:Ad)-[:PUBLISHED_BY]->(b:Page) "
    c+= "MATCH (c)-[:CREATED_ON|DELIVERED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if ids2 is not None:
        c+= "MATCH (t:Buyer)<-[:PAID_BY]-(u:Ad)-[:PUBLISHED_BY]->(b:Page) "
        c+= "MATCH (u)-[:CREATED_ON|DELIVERED_ON]->(v:Day) "
        c+= "WHERE ID(t) IN $ids2 "
        c+= "AND v.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND v.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_page_buyer(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Page)<-[:PUBLISHED_BY]-(c:Ad)-[:PAID_BY]->(b:Buyer) "
    c+= "MATCH (c)-[:CREATED_ON|DELIVERED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if ids2 is not None:
        c+= "MATCH (t:Page)<-[:PUBLISHED_BY]-(u:Ad)-[:PAID_BY]->(b:Buyer) "
        c+= "MATCH (u)-[:CREATED_ON|DELIVERED_ON]->(v:Day) "
        c+= "WHERE ID(t) IN $ids2 "
        c+= "AND v.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND v.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_candidate_committee(tx, ids, ids2, sup_opp, purpose, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Candidate)<-[:IDENTIFIES]-(d:Expenditure)<-[:SPENT]-(b:Committee) "
    c+= "MATCH (d)-[:HAPPENED_ON]->(e:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND ID(b) IN $ids2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if sup_opp is not None:
        c+= "AND d.sup_opp = toUpper($sup_opp) "
    if purpose is not None:
        c+= "AND d.purpose CONTAINS toUpper($purpose) "
    if amndt_ind is not None:
        c+= "AND d.amndt_ind = toUpper($amndt_ind) "
    if gt is not None:
        c+= "AND $gt < d.exp_amt "
    if lte is not None:
        c+= "AND < d.exp_amt <= $lte "
    c+= "CALL apoc.nodes.collapse([a, d], {properties: 'overwrite'}) "
    c+= "YIELD from, rel, to "
    c+= "WITH DISTINCT from AS x "
    c+= "MATCH (x) "
    c+= "WHERE labels(x) = [] "
    c+= "RETURN x "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_committee_candidate(tx, ids, ids2, sup_opp, purpose, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Committee)-[:SPENT]->(d:Expenditure)-[:IDENTIFIES]->(b:Candidate) "
    c+= "MATCH (d)-[:HAPPENED_ON]->(e:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND ID(b) IN $ids2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if sup_opp is not None:
        c+= "AND d.sup_opp = toUpper($sup_opp) "
    if purpose is not None:
        c+= "AND d.purpose CONTAINS toUpper($purpose) "
    if amndt_ind is not None:
        c+= "AND d.amndt_ind = toUpper($amndt_ind) "
    if gt is not None:
        c+= "AND $gt < d.exp_amt "
    if lte is not None:
        c+= "AND < d.exp_amt <= $lte "
    c+= "CALL apoc.nodes.collapse([b, d], {properties: 'overwrite'}) "
    c+= "YIELD from, rel, to "
    c+= "WITH DISTINCT from AS x "
    c+= "MATCH (x) "
    c+= "WHERE labels(x) = [] "
    c+= "RETURN x "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_committee_committee(tx, ids, ids2, transaction_tp, transaction_pgi, rpt_tp, amndt_ind, gt, lte, cand_pty_affiliation, cand_office, cand_office_st, cand_office_district, cand_election_yr, cand_ici, intermediaries, direction, sup_opp, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    if intermediaries == "contribution":
        if direction == "receipts":
            c = "MATCH (a:Committee)<-[:CONTRIBUTED_TO]-(d:Contribution)<-[:CONTRIBUTED_TO]-(b:Committee) "
        elif direction == "disbursements":
            c = "MATCH (a:Committee)-[:CONTRIBUTED_TO]->(d:Contribution)-[:CONTRIBUTED_TO]->(b:Committee) "
        else:
            c = "MATCH (a:Committee)-[:CONTRIBUTED_TO]-(d:Contribution)-[:CONTRIBUTED_TO]-(b:Committee) "
        c+= "MATCH (d)-[:HAPPENED_ON]->(e:Day) "
        c+= "WHERE ID(a) IN $ids "
        c+= "AND ID(b) IN $ids2 "
        c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
        if transaction_tp is not None:
            c+= "AND d.transaction_tp = toUpper($transaction_tp) "
        if transaction_pgi is not None:
            c+= "AND d.transaction_pgi = toUpper($transaction_pgi) "
        if rpt_tp is not None:
            c+= "AND d.rpt_tp = toUpper($rpt_tp) "
        if amndt_ind is not None:
            c+= "AND d.amndt_ind = toUpper($amndt_ind) "
        if gt is not None:
            c+= "AND $gt < d.transaction_amt "
        if lte is not None:
            c+= "AND < d.transaction_amt <= $lte "
        c+= "RETURN DISTINCT d "
    else:
        c = "MATCH (a:Committee)-[:SPENT]->(c:Expenditure)-[:IDENTIFIES]->(x:Candidate)<-[IDENTIFIES]-(d:Expenditure)<-[:SPENT]-(b:Committee) "
        c+= "MATCH (c)-[:HAPPENED_ON]->(e:Day)<-[:HAPPENED_ON]-(d) "
        c+= "WHERE ID(a) IN $ids "
        c+= "AND ID(b) IN $ids2 "
        c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
        c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
        if sup_opp is not None:
            c+= "AND c.sup_opp = toUpper($sup_opp) "
            c+= "AND d.sup_opp = toUpper($sup_opp) "
        if cand_pty_affiliation is not None:
            c+= "AND x.cand_pty_affiliation = toUpper($cand_pty_affiliation) "
        if cand_office is not None:
            c+= "AND x.cand_office = toUpper($cand_office) "
        if cand_office_st is not None:
            c+= "AND x.cand_office_st = toUpper($cand_office_st) "
        if cand_office_district is not None:
            c+= "AND x.cand_office_district = $cand_office_district "
        if cand_election_yr is not None:
            c+= "AND x.cand_election_yr = $cand_election_yr "
        if cand_ici is not None:
            c+= "AND x.cand_ici = toUpper($cand_ici) "
        c+= "RETURN x "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, transaction_tp=transaction_tp, transaction_pgi=transaction_pgi, rpt_tp=rpt_tp, amndt_ind=amndt_ind, gt=gt, lte=lte, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, sup_opp=sup_opp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_committee_donor(tx, ids, ids2, transaction_tp, transaction_pgi, rpt_tp, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Committee)<-[:CONTRIBUTED_TO]-(d:Contribution)<-[:CONTRIBUTED_TO]-(b:Donor) "
    c+= "MATCH (d)-[:HAPPENED_ON]->(e:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND ID(b) IN $ids2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if transaction_tp is not None:
        c+= "AND d.transaction_tp = toUpper($transaction_tp) "
    if transaction_pgi is not None:
        c+= "AND d.transaction_pgi = toUpper($transaction_pgi) "
    if rpt_tp is not None:
        c+= "AND d.rpt_tp = toUpper($rpt_tp) "
    if amndt_ind is not None:
        c+= "AND d.amndt_ind = toUpper($amndt_ind) "
    if gt is not None:
        c+= "AND $gt < d.transaction_amt "
    if lte is not None:
        c+= "AND < d.transaction_amt <= $lte "
    c+= "RETURN DISTINCT d "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, transaction_tp=transaction_tp, transaction_pgi=transaction_pgi, rpt_tp=rpt_tp, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_committee_payee(tx, ids, ids2, type, sup_opp, purpose, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Committee)-[:SPENT]->(d:Expenditure)-[:PAID]->(b:Payee) "
    c+= "MATCH (d)-[:HAPPENED_ON]->(e:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND ID(b) IN $ids2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if type is not None:
        c+= "AND d.type = toLower($type) "
    if sup_opp is not None:
        c+= "AND d.sup_opp = toUpper($sup_opp) "
    if purpose is not None:
        c+= "AND d.purpose CONTAINS toUpper($purpose) "
    if amndt_ind is not None:
        c+= "AND d.amndt_ind = toUpper($amndt_ind) "
    if gt is not None:
        c+= "AND $gt < d.exp_amt "
    if lte is not None:
        c+= "AND < d.exp_amt <= $lte "
    c+= "OPTIONAL MATCH (d)-[:IDENTIFIES]->(h:Candidate) "
    c+= "CALL apoc.nodes.collapse([h, d], {properties: 'overwrite'}) "
    c+= "YIELD from, rel, to "
    c+= "WITH DISTINCT from AS x "
    c+= "MATCH (x) "
    c+= "WHERE labels(x) = [] "
    c+= "RETURN x "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, type=type, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_donor_committee(tx, ids, ids2, transaction_tp, transaction_pgi, rpt_tp, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Donor)-[:CONTRIBUTED_TO]->(d:Contribution)-[:CONTRIBUTED_TO]->(b:Committee) "
    c+= "MATCH (d)-[:HAPPENED_ON]->(e:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND ID(b) IN $ids2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if transaction_tp is not None:
        c+= "AND d.transaction_tp = toUpper($transaction_tp) "
    if transaction_pgi is not None:
        c+= "AND d.transaction_pgi = toUpper($transaction_pgi) "
    if rpt_tp is not None:
        c+= "AND d.rpt_tp = toUpper($rpt_tp) "
    if amndt_ind is not None:
        c+= "AND d.amndt_ind = toUpper($amndt_ind) "
    if gt is not None:
        c+= "AND $gt < d.transaction_amt "
    if lte is not None:
        c+= "AND < d.transaction_amt <= $lte "
    c+= "RETURN DISTINCT d "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, transaction_tp=transaction_tp, transaction_pgi=transaction_pgi, rpt_tp=rpt_tp, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_payee_committee(tx, ids, ids2, type, sup_opp, purpose, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Payee)<-[:PAID]-(d:Expenditure)<-[:SPENT]-(b:Committee) "
    c+= "MATCH (d)-[:HAPPENED_ON]->(e:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND ID(b) IN $ids2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    if type is not None:
        c+= "AND d.type = toLower($type) "
    if sup_opp is not None:
        c+= "AND d.sup_opp = toUpper($sup_opp) "
    if purpose is not None:
        c+= "AND d.purpose CONTAINS toUpper($purpose) "
    if amndt_ind is not None:
        c+= "AND d.amndt_ind = toUpper($amndt_ind) "
    if gt is not None:
        c+= "AND $gt < d.exp_amt "
    if lte is not None:
        c+= "AND < d.exp_amt <= $lte "
    c+= "OPTIONAL MATCH (d)-[:IDENTIFIES]->(h:Candidate) "
    c+= "CALL apoc.nodes.collapse([h, d], {properties: 'overwrite'}) "
    c+= "YIELD from, rel, to "
    c+= "WITH DISTINCT from AS x "
    c+= "MATCH (x) "
    c+= "WHERE labels(x) = [] "
    c+= "RETURN x "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, type=type, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_tweeter_source(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Tweeter)<-[:PUBLISHED_BY]-(c:Tweet)-[:MENTIONS]->(:Link)-[:ASSOCIATED_WITH]-(:Domain)-[:ASSOCIATED_WITH]->(b:Source) "
    c+= "MATCH (c)-[:PUBLISHED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND ID(b) IN $ids2 "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN DISTINCT c "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_source_tweeter(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Source)<-[:ASSOCIATED_WITH]-(:Domain)<-[:ASSOCIATED_WITH]-(:Link)<-[:MENTIONS]-(c:Tweet)-[:PUBLISHED_BY]->(b:Tweeter) "
    c+= "MATCH (c)-[:PUBLISHED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND ID(b) IN $ids2 "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN DISTINCT c "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_buyer_page(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Buyer)<-[:PAID_BY]-(c:Ad)-[:PUBLISHED_BY]->(b:Page) "
    c+= "MATCH (c)-[:CREATED_ON|DELIVERED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND ID(b) IN $ids2 "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN DISTINCT c "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_page_buyer(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Page)<-[:PUBLISHED_BY]-(c:Ad)-[:PAID_BY]->(b:Buyer) "
    c+= "MATCH (c)-[:CREATED_ON|DELIVERED_ON]->(d:Day) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND ID(b) IN $ids2 "
    c+= "AND d.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND d.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN DISTINCT c "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_relationships_contribution_contributor(tx, ids, skip, limit):
    c = "MATCH (a:Contribution)<-[:CONTRIBUTED_TO]-(b) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND (b:Donor OR b:Committee) "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, skip=skip, limit=limit).graph()

def graph_traverse_relationships_contribution_recipient(tx, ids, skip, limit):
    c = "MATCH (a:Contribution)-[:CONTRIBUTED_TO]->(b:Committee) "
    c+= "WHERE ID(a) IN $ids "
    c+= "RETURN DISTINCT b "
    c+= "SKIP $skip "
    c+= "LIMIT $limit"
    return tx.run(c, ids=ids, skip=skip, limit=limit).graph()

#########################################################
# uncover graph insights
#########################################################

def graph_uncover_donors(tx, ids, labels, min_transaction_amt, limit):
    c  = f"MATCH (a) <-[c1:CONTRIBUTED_TO]-(t:Contribution)<-[c2:CONTRIBUTED_TO]- (d) "
    c += f"WHERE ID(a) IN {ids} AND t.transaction_amt > {min_transaction_amt} "
    if labels is not None:
        c+= f"AND ( {labels} )"
    c += f"RETURN a, t, d, c1, c2 "
    c += f"LIMIT {limit} "
    query_out = tx.run(c, ids=ids)
    return query_out.graph()

#########################################################
# analyze elements
#########################################################

def data_analyze_count_in(tx, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c) "
    c+= "WHERE (c:Donor OR c:Committee) "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_candidate(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c) "
    c+= "MATCH (b)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(d:Candidate) "
    c+= "WHERE (c:Donor OR c:Committee) "
    c+= "AND d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_committee(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c) "
    c+= "MATCH (b)-[:CONTRIBUTED_TO]->(d:Committee) "
    c+= "WHERE (c:Donor OR c:Committee) "
    c+= "AND d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_candidate_candidate(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c) "
    c+= "MATCH (b)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(d:Candidate) "
    c+= "WHERE (c:Donor OR c:Committee) "
    c+= "AND d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)<-[:CONTRIBUTED_TO]-(c) "
    c+= "MATCH (f)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(g:Candidate) "
    c+= "WHERE g.uuid = $uuid2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_candidate_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c) "
    c+= "MATCH (b)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(d:Candidate) "
    c+= "WHERE (c:Donor OR c:Committee) "
    c+= "AND d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)<-[:CONTRIBUTED_TO]-(c) "
    c+= "MATCH (f)-[:CONTRIBUTED_TO]->(g:Committee) "
    c+= "WHERE g.uuid = $uuid2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_committee_candidate(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c) "
    c+= "MATCH (b)-[:CONTRIBUTED_TO]->(d:Committee) "
    c+= "WHERE (c:Donor OR c:Committee) "
    c+= "AND d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)<-[:CONTRIBUTED_TO]-(c) "
    c+= "MATCH (f)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(g:Candidate) "
    c+= "WHERE g.uuid = $uuid2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_committee_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c) "
    c+= "MATCH (b)-[:CONTRIBUTED_TO]->(d:Committee) "
    c+= "WHERE (c:Donor OR c:Committee) "
    c+= "AND d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)<-[:CONTRIBUTED_TO]-(c) "
    c+= "MATCH (f)-[:CONTRIBUTED_TO]->(g:Committee) "
    c+= "WHERE g.uuid = $uuid2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out(tx, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "WHERE a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_donor(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (b)<-[:CONTRIBUTED_TO]-(d:Donor) "
    c+= "WHERE d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_committee(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (b)<-[:CONTRIBUTED_TO]-(d:Committee) "
    c+= "WHERE d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_committee_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (b)<-[:CONTRIBUTED_TO]-(d:Committee) "
    c+= "WHERE d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (f)<-[:CONTRIBUTED_TO]-(g:Committee) "
    c+= "WHERE g.uuid = $uuid2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_committee_donor(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (b)<-[:CONTRIBUTED_TO]-(d:Committee) "
    c+= "WHERE d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (f)<-[:CONTRIBUTED_TO]-(g:Donor) "
    c+= "WHERE g.uuid = $uuid2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_donor_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (b)<-[:CONTRIBUTED_TO]-(d:Donor) "
    c+= "WHERE d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (f)<-[:CONTRIBUTED_TO]-(g:Committee) "
    c+= "WHERE g.uuid = $uuid2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_donor_donor(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (b)<-[:CONTRIBUTED_TO]-(d:Donor) "
    c+= "WHERE d.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (f)<-[:CONTRIBUTED_TO]-(g:Donor) "
    c+= "WHERE g.uuid = $uuid2 "
    c+= "AND e.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND e.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "RETURN count(DISTINCT c) AS count "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_sum_revenue_candidate(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(c:Candidate) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_revenue_committee(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_revenue_candidate_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(c:Candidate) "
    c+= "MATCH (b)<-[:CONTRIBUTED_TO]-(d:Committee) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND d.uuid = $uuid2 "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_revenue_candidate_donor(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(c:Candidate) "
    c+= "MATCH (b)<-[:CONTRIBUTED_TO]-(d:Donor) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND d.uuid = $uuid2 "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_revenue_committee_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (b)<-[:CONTRIBUTED_TO]-(d:Committee) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND d.uuid = $uuid2 "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_revenue_committee_donor(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee) "
    c+= "MATCH (b)<-[:CONTRIBUTED_TO]-(d:Donor) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND d.uuid = $uuid2 "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_committee(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Committee) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_donor(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Donor) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_committee_candidate(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Committee) "
    c+= "MATCH (b)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(d:Candidate) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND d.uuid = $uuid2 "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_committee_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Committee) "
    c+= "MATCH (b)-[:CONTRIBUTED_TO]->(d:Committee) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND d.uuid = $uuid2 "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_donor_candidate(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Donor) "
    c+= "MATCH (b)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(d:Candidate) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND d.uuid = $uuid2 "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_donor_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = "MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Donor) "
    c+= "MATCH (b)-[:CONTRIBUTED_TO]->(d:Committee) "
    c+= "WHERE c.uuid = $uuid "
    c+= "AND d.uuid = $uuid2 "
    c+= "AND a.date >= date({year: $min_year, month: $min_month, day: $min_day}) "
    c+= "AND a.date <= date({year: $max_year, month: $max_month, day: $max_day}) "
    c+= "WITH DISTINCT b "
    c+= "RETURN sum(b.transaction_amt) AS sum "
    return tx.run(c, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]
