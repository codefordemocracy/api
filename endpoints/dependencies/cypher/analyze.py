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
