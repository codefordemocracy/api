#########################################################
# analyze elements
#########################################################

def data_analyze_count_in(tx, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c)")
    c.append("WHERE (c:Donor OR c:Committee)")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_candidate(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c)")
    c.append("MATCH (b)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(d:Candidate)")
    c.append("WHERE (c:Donor OR c:Committee)")
    c.append("AND d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_committee(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c)")
    c.append("MATCH (b)-[:CONTRIBUTED_TO]->(d:Committee)")
    c.append("WHERE (c:Donor OR c:Committee)")
    c.append("AND d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_candidate_candidate(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c)")
    c.append("MATCH (b)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(d:Candidate)")
    c.append("WHERE (c:Donor OR c:Committee)")
    c.append("AND d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)<-[:CONTRIBUTED_TO]-(c)")
    c.append("MATCH (f)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(g:Candidate)")
    c.append("WHERE g.uuid = $uuid2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_candidate_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c)")
    c.append("MATCH (b)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(d:Candidate)")
    c.append("WHERE (c:Donor OR c:Committee)")
    c.append("AND d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)<-[:CONTRIBUTED_TO]-(c)")
    c.append("MATCH (f)-[:CONTRIBUTED_TO]->(g:Committee)")
    c.append("WHERE g.uuid = $uuid2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_committee_candidate(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c)")
    c.append("MATCH (b)-[:CONTRIBUTED_TO]->(d:Committee)")
    c.append("WHERE (c:Donor OR c:Committee)")
    c.append("AND d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)<-[:CONTRIBUTED_TO]-(c)")
    c.append("MATCH (f)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(g:Candidate)")
    c.append("WHERE g.uuid = $uuid2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_in_committee_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c)")
    c.append("MATCH (b)-[:CONTRIBUTED_TO]->(d:Committee)")
    c.append("WHERE (c:Donor OR c:Committee)")
    c.append("AND d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)<-[:CONTRIBUTED_TO]-(c)")
    c.append("MATCH (f)-[:CONTRIBUTED_TO]->(g:Committee)")
    c.append("WHERE g.uuid = $uuid2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out(tx, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("WHERE a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_donor(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (b)<-[:CONTRIBUTED_TO]-(d:Donor)")
    c.append("WHERE d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_committee(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (b)<-[:CONTRIBUTED_TO]-(d:Committee)")
    c.append("WHERE d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_committee_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (b)<-[:CONTRIBUTED_TO]-(d:Committee)")
    c.append("WHERE d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (f)<-[:CONTRIBUTED_TO]-(g:Committee)")
    c.append("WHERE g.uuid = $uuid2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_committee_donor(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (b)<-[:CONTRIBUTED_TO]-(d:Committee)")
    c.append("WHERE d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (f)<-[:CONTRIBUTED_TO]-(g:Donor)")
    c.append("WHERE g.uuid = $uuid2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_donor_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (b)<-[:CONTRIBUTED_TO]-(d:Donor)")
    c.append("WHERE d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (f)<-[:CONTRIBUTED_TO]-(g:Committee)")
    c.append("WHERE g.uuid = $uuid2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_count_out_donor_donor(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (b)<-[:CONTRIBUTED_TO]-(d:Donor)")
    c.append("WHERE d.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("MATCH (e:Day)<-[:HAPPENED_ON]-(f:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (f)<-[:CONTRIBUTED_TO]-(g:Donor)")
    c.append("WHERE g.uuid = $uuid2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN count(DISTINCT c) AS count")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["count"]

def data_analyze_sum_revenue_candidate(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(c:Candidate)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_revenue_committee(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_revenue_candidate_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(c:Candidate)")
    c.append("MATCH (b)<-[:CONTRIBUTED_TO]-(d:Committee)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND d.uuid = $uuid2")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_revenue_candidate_donor(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(c:Candidate)")
    c.append("MATCH (b)<-[:CONTRIBUTED_TO]-(d:Donor)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND d.uuid = $uuid2")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_revenue_committee_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (b)<-[:CONTRIBUTED_TO]-(d:Committee)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND d.uuid = $uuid2")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_revenue_committee_donor(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)-[:CONTRIBUTED_TO]->(c:Committee)")
    c.append("MATCH (b)<-[:CONTRIBUTED_TO]-(d:Donor)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND d.uuid = $uuid2")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_committee(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Committee)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_donor(tx, uuid, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Donor)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_committee_candidate(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Committee)")
    c.append("MATCH (b)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(d:Candidate)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND d.uuid = $uuid2")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_committee_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Committee)")
    c.append("MATCH (b)-[:CONTRIBUTED_TO]->(d:Committee)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND d.uuid = $uuid2")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_donor_candidate(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Donor)")
    c.append("MATCH (b)-[:CONTRIBUTED_TO]->(:Committee {cmte_dsgn: 'P'})-[:ASSOCIATED_WITH]->(d:Candidate)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND d.uuid = $uuid2")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]

def data_analyze_sum_wallet_donor_committee(tx, uuid, uuid2, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Day)<-[:HAPPENED_ON]-(b:Contribution)<-[:CONTRIBUTED_TO]-(c:Donor)")
    c.append("MATCH (b)-[:CONTRIBUTED_TO]->(d:Committee)")
    c.append("WHERE c.uuid = $uuid")
    c.append("AND d.uuid = $uuid2")
    c.append("AND a.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND a.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("WITH DISTINCT b")
    c.append("RETURN sum(b.transaction_amt) AS sum")
    return tx.run(" ".join(c), uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).data()[0]["sum"]
