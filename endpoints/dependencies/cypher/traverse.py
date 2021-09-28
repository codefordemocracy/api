#########################################################
# traverse graph
#########################################################

def graph_traverse_neighbors(tx, ids, labels, skip, limit):
    c = []
    c.append("MATCH (a)-[x]-(b)")
    c.append("WHERE ID(a) IN $ids")
    if labels is not None:
        c.append("AND (" + labels + ")")
    c.append("RETURN a, x, b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, skip=skip, limit=limit).graph()

def graph_traverse_associations_candidate_committee(tx, ids, ids2, cmte_pty_affiliation, cmte_dsgn, cmte_tp, org_tp, intermediaries, sup_opp, purpose, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    if intermediaries == "linkage":
        c.append("MATCH (a:Candidate)<-[:ASSOCIATED_WITH]-(b:Committee)")
        c.append("WHERE ID(a) IN $ids")
    else:
        c.append("MATCH (a:Candidate)<-[:IDENTIFIES]-(d:Expenditure)<-[:SPENT]-(b:Committee)")
        c.append("MATCH (d)-[:HAPPENED_ON]->(e:Day)")
        c.append("WHERE ID(a) IN $ids")
        c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
        if sup_opp is not None:
            c.append("AND d.sup_opp = toUpper($sup_opp)")
        if purpose is not None:
            c.append("AND d.purpose CONTAINS toUpper($purpose)")
        if amndt_ind is not None:
            c.append("AND d.amndt_ind = toUpper($amndt_ind)")
        if gt is not None:
            c.append("AND $gt < d.transaction_amt")
        if lte is not None:
            c.append("AND d.transaction_amt <= $lte")
    if ids2 is not None:
        if intermediaries == "linkage":
            c.append("MATCH (t:Candidate)<-[:ASSOCIATED_WITH]-(b:Committee)")
            c.append("WHERE ID(t) IN $ids2")
        else:
            c.append("MATCH (t:Candidate)<-[:IDENTIFIES]-(u:Expenditure)<-[:SPENT]-(b:Committee)")
            c.append("MATCH (u)-[:HAPPENED_ON]->(v:Day)")
            c.append("WHERE ID(t) IN $ids2")
            c.append("AND v.date >= date({year: $min_year, month: $min_month, day: $min_day})")
            c.append("AND v.date <= date({year: $max_year, month: $max_month, day: $max_day})")
            if sup_opp is not None:
                c.append("AND u.sup_opp = toUpper($sup_opp)")
            if purpose is not None:
                c.append("AND u.purpose CONTAINS toUpper($purpose)")
            if amndt_ind is not None:
                c.append("AND u.amndt_ind = toUpper($amndt_ind)")
            if gt is not None:
                c.append("AND $gt < u.transaction_amt")
            if lte is not None:
                c.append("AND u.transaction_amt <= $lte")
    if cmte_pty_affiliation is not None:
        c.append("AND b.cmte_pty_affiliation = toUpper($cmte_pty_affiliation)")
    if cmte_dsgn is not None:
        c.append("AND b.cmte_dsgn = toUpper($cmte_dsgn)")
    if cmte_tp is not None:
        c.append("AND b.cmte_tp = toUpper($cmte_tp)")
    if org_tp is not None:
        c.append("AND b.org_tp = toUpper($org_tp)")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, org_tp=org_tp, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_candidate_tweeter(tx, ids, ids2, skip, limit):
    c = []
    c.append("MATCH (a:Candidate)-[:ASSOCIATED_WITH]->(b:Tweeter)")
    c.append("WHERE ID(a) IN $ids")
    if ids2 is not None:
        c.append("MATCH (t:Candidate)-[:ASSOCIATED_WITH]->(b:Tweeter)")
        c.append("WHERE ID(t) IN $ids2")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, skip=skip, limit=limit).graph()

def graph_traverse_associations_committee_candidate(tx, ids, ids2, cand_pty_affiliation, cand_office, cand_office_st, cand_office_district, cand_election_yr, cand_ici, intermediaries, sup_opp, purpose, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    if intermediaries == "linkage":
        c.append("MATCH (a:Committee)-[:ASSOCIATED_WITH]->(b:Candidate)")
        c.append("WHERE ID(a) IN $ids")
    else:
        c.append("MATCH (a:Committee)-[:SPENT]->(d:Expenditure)-[:IDENTIFIES]->(b:Candidate)")
        c.append("MATCH (d)-[:HAPPENED_ON]->(e:Day)")
        c.append("WHERE ID(a) IN $ids")
        c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
        if sup_opp is not None:
            c.append("AND d.sup_opp = toUpper($sup_opp)")
        if purpose is not None:
            c.append("AND d.purpose CONTAINS toUpper($purpose)")
        if amndt_ind is not None:
            c.append("AND d.amndt_ind = toUpper($amndt_ind)")
        if gt is not None:
            c.append("AND $gt < d.transaction_amt")
        if lte is not None:
            c.append("AND d.transaction_amt <= $lte")
    if ids2 is not None:
        if intermediaries == "linkage":
            c.append("MATCH (t:Committee)-[:ASSOCIATED_WITH]->(b:Candidate)")
            c.append("WHERE ID(t) IN $ids2")
        else:
            c.append("MATCH (t:Committee)-[:SPENT]->(u:Expenditure)-[:IDENTIFIES]->(b:Candidate)")
            c.append("MATCH (u)-[:HAPPENED_ON]->(v:Day)")
            c.append("WHERE ID(t) IN $ids2")
            c.append("AND v.date >= date({year: $min_year, month: $min_month, day: $min_day})")
            c.append("AND v.date <= date({year: $max_year, month: $max_month, day: $max_day})")
            if sup_opp is not None:
                c.append("AND u.sup_opp = toUpper($sup_opp)")
            if purpose is not None:
                c.append("AND u.purpose CONTAINS toUpper($purpose)")
            if amndt_ind is not None:
                c.append("AND u.amndt_ind = toUpper($amndt_ind)")
            if gt is not None:
                c.append("AND $gt < u.transaction_amt")
            if lte is not None:
                c.append("AND u.transaction_amt <= $lte")
    if cand_pty_affiliation is not None:
        c.append("AND b.cand_pty_affiliation = toUpper($cand_pty_affiliation)")
    if cand_office is not None:
        c.append("AND b.cand_office = toUpper($cand_office)")
    if cand_office_st is not None:
        c.append("AND b.cand_office_st = toUpper($cand_office_st)")
    if cand_office_district is not None:
        c.append("AND b.cand_office_district = $cand_office_district")
    if cand_election_yr is not None:
        c.append("AND b.cand_election_yr = $cand_election_yr")
    if cand_ici is not None:
        c.append("AND b.cand_ici = toUpper($cand_ici)")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_committee_committee(tx, ids, ids2, cmte_pty_affiliation, cmte_dsgn, cmte_tp, org_tp, intermediaries, direction, transaction_tp, transaction_pgi, rpt_tp, contribution_amndt_ind, contribution_gt, contribution_lte, sup_opp, purpose, expenditure_amndt_ind, expenditure_gt, expenditure_lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    if intermediaries == "contribution":
        if direction == "receipts":
            c.append("MATCH (a:Committee)<-[:CONTRIBUTED_TO]-(c:Contribution)<-[:CONTRIBUTED_TO]-(b:Committee)")
        elif direction == "disbursements":
            c.append("MATCH (a:Committee)-[:CONTRIBUTED_TO]->(c:Contribution)-[:CONTRIBUTED_TO]->(b:Committee)")
        else:
            c.append("MATCH (a:Committee)-[:CONTRIBUTED_TO]-(c:Contribution)-[:CONTRIBUTED_TO]-(b:Committee)")
        c.append("MATCH (c)-[:HAPPENED_ON]->(d:Day)")
        c.append("WHERE ID(a) IN $ids")
        c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
        if transaction_tp is not None:
            c.append("AND d.transaction_tp = toUpper($transaction_tp)")
        if transaction_pgi is not None:
            c.append("AND d.transaction_pgi = toUpper($transaction_pgi)")
        if rpt_tp is not None:
            c.append("AND d.rpt_tp = toUpper($rpt_tp)")
        if amndt_ind is not None:
            c.append("AND d.amndt_ind = toUpper($contribution_amndt_ind)")
        if gt is not None:
            c.append("AND $contribution_gt < d.transaction_amt")
        if lte is not None:
            c.append("AND d.transaction_amt <= $contribution_lte")
    else:
        c.append("MATCH (a:Committee)-[:SPENT]->(c:Expenditure)-[:IDENTIFIES]->(:Candidate)<-[IDENTIFIES]-(d:Expenditure)<-[:SPENT]-(b:Committee)")
        c.append("MATCH (c)-[:HAPPENED_ON]->(e:Day)<-[:HAPPENED_ON]-(d)")
        c.append("WHERE ID(a) IN $ids")
        c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
        if sup_opp is not None:
            c.append("AND c.sup_opp = toUpper($sup_opp)")
            c.append("AND d.sup_opp = toUpper($sup_opp)")
        if purpose is not None:
            c.append("AND c.purpose CONTAINS toUpper($purpose)")
            c.append("AND d.purpose CONTAINS toUpper($purpose)")
        if amndt_ind is not None:
            c.append("AND c.amndt_ind = toUpper($expenditure_amndt_ind)")
            c.append("AND d.amndt_ind = toUpper($expenditure_amndt_ind)")
        if gt is not None:
            c.append("AND $expenditure_gt < c.transaction_amt")
            c.append("AND $expenditure_gt < d.transaction_amt")
        if lte is not None:
            c.append("AND c.transaction_amt <= $expenditure_lte")
            c.append("AND d.transaction_amt <= $expenditure_lte")
    if ids2 is not None:
        if intermediaries == "contribution":
            if direction == "receipts":
                c.append("MATCH (t:Committee)<-[:CONTRIBUTED_TO]-(u:Contribution)<-[:CONTRIBUTED_TO]-(b:Committee)")
            elif direction == "disbursements":
                c.append("MATCH (t:Committee)-[:CONTRIBUTED_TO]->(u:Contribution)-[:CONTRIBUTED_TO]->(b:Committee)")
            else:
                c.append("MATCH (t:Committee)-[:CONTRIBUTED_TO]-(u:Contribution)-[:CONTRIBUTED_TO]-(b:Committee)")
            c.append("MATCH (u)-[:HAPPENED_ON]->(v:Day)")
            c.append("WHERE ID(t) IN $ids2")
            c.append("AND v.date >= date({year: $min_year, month: $min_month, day: $min_day})")
            c.append("AND v.date <= date({year: $max_year, month: $max_month, day: $max_day})")
            if transaction_tp is not None:
                c.append("AND u.transaction_tp = toUpper($transaction_tp)")
            if transaction_pgi is not None:
                c.append("AND u.transaction_pgi = toUpper($transaction_pgi)")
            if rpt_tp is not None:
                c.append("AND u.rpt_tp = toUpper($rpt_tp)")
            if amndt_ind is not None:
                c.append("AND u.amndt_ind = toUpper($contribution_amndt_ind)")
            if gt is not None:
                c.append("AND $contribution_gt < u.transaction_amt")
            if lte is not None:
                c.append("AND u.transaction_amt <= $contribution_lte")
        else:
            c.append("MATCH (t:Committee)-[:SPENT]->(u:Expenditure)-[:IDENTIFIES]->(:Candidate)<-[IDENTIFIES]-(v:Expenditure)<-[:SPENT]-(b:Committee)")
            c.append("MATCH (u)-[:HAPPENED_ON]->(w:Day)<-[:HAPPENED_ON]-(v)")
            c.append("WHERE ID(t) IN $ids2")
            c.append("AND w.date >= date({year: $min_year, month: $min_month, day: $min_day})")
            c.append("AND w.date <= date({year: $max_year, month: $max_month, day: $max_day})")
            if sup_opp is not None:
                c.append("AND u.sup_opp = toUpper($sup_opp)")
                c.append("AND v.sup_opp = toUpper($sup_opp)")
            if purpose is not None:
                c.append("AND u.purpose CONTAINS toUpper($purpose)")
                c.append("AND v.purpose CONTAINS toUpper($purpose)")
            if amndt_ind is not None:
                c.append("AND u.amndt_ind = toUpper($expenditure_amndt_ind)")
                c.append("AND v.amndt_ind = toUpper($expenditure_amndt_ind)")
            if gt is not None:
                c.append("AND $expenditure_gt < u.transaction_amt")
                c.append("AND $expenditure_gt < v.transaction_amt")
            if lte is not None:
                c.append("AND u.transaction_amt <= $expenditure_lte")
                c.append("AND v.transaction_amt <= $expenditure_lte")
    if cmte_pty_affiliation is not None:
        c.append("AND b.cmte_pty_affiliation = toUpper($cmte_pty_affiliation)")
    if cmte_dsgn is not None:
        c.append("AND b.cmte_dsgn = toUpper($cmte_dsgn)")
    if cmte_tp is not None:
        c.append("AND b.cmte_tp = toUpper($cmte_tp)")
    if org_tp is not None:
        c.append("AND b.org_tp = toUpper($org_tp)")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, org_tp=org_tp, transaction_tp=transaction_tp, transaction_pgi=transaction_pgi, rpt_tp=rpt_tp, contribution_amndt_ind=contribution_amndt_ind, contribution_gt=contribution_gt, contribution_lte=contribution_lte, sup_opp=sup_opp, purpose=purpose, expenditure_amndt_ind=expenditure_amndt_ind, expenditure_gt=expenditure_gt, expenditure_lte=expenditure_lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_committee_donor(tx, ids, ids2, employer, occupation, state, zip_code, entity_tp, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Committee)<-[:CONTRIBUTED_TO]-(c:Contribution)<-[:CONTRIBUTED_TO]-(b:Donor)")
    c.append("MATCH (c)-[:HAPPENED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if ids2 is not None:
        c.append("MATCH (t:Committee)<-[:CONTRIBUTED_TO]-(u:Contribution)<-[:CONTRIBUTED_TO]-(b:Donor)")
        c.append("MATCH (u)-[:HAPPENED_ON]->(v:Day)")
        c.append("WHERE ID(t) IN $ids2")
        c.append("AND v.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND v.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if employer is not None:
        c.append("AND b.employer CONTAINS toUpper($employer)")
    if occupation is not None:
        c.append("AND b.occupation CONTAINS toUpper($occupation)")
    if state is not None:
        c.append("AND b.state = toUpper($state)")
    if zip_code is not None:
        c.append("AND b.zip_code = toString($zip_code)")
    if entity_tp is not None:
        c.append("AND b.entity_tp = toUpper($entity_tp)")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, employer=employer, occupation=occupation, state=state, zip_code=zip_code, entity_tp=entity_tp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_committee_payee(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Committee)-[:SPENT]->(c:Expenditure)-[:PAID]->(b:Payee)")
    c.append("MATCH (c)-[:HAPPENED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if ids2 is not None:
        c.append("MATCH (t:Committee)-[:SPENT]->(u:Expenditure)-[:PAID]->(b:Payee)")
        c.append("MATCH (u)-[:HAPPENED_ON]->(v:Day)")
        c.append("WHERE ID(t) IN $ids2")
        c.append("AND v.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND v.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_donor_committee(tx, ids, ids2, cmte_pty_affiliation, cmte_dsgn, cmte_tp, org_tp, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Donor)-[:CONTRIBUTED_TO]->(c:Contribution)-[:CONTRIBUTED_TO]->(b:Committee)")
    c.append("MATCH (c)-[:HAPPENED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if ids2 is not None:
        c.append("MATCH (t:Donor)-[:CONTRIBUTED_TO]->(u:Contribution)-[:CONTRIBUTED_TO]->(b:Committee)")
        c.append("MATCH (u)-[:HAPPENED_ON]->(v:Day)")
        c.append("WHERE ID(t) IN $ids2")
        c.append("AND v.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND v.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if cmte_pty_affiliation is not None:
        c.append("AND b.cmte_pty_affiliation = toUpper($cmte_pty_affiliation)")
    if cmte_dsgn is not None:
        c.append("AND b.cmte_dsgn = toUpper($cmte_dsgn)")
    if cmte_tp is not None:
        c.append("AND b.cmte_tp = toUpper($cmte_tp)")
    if org_tp is not None:
        c.append("AND b.org_tp = toUpper($org_tp)")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, org_tp=org_tp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_payee_committee(tx, ids, ids2, cmte_pty_affiliation, cmte_dsgn, cmte_tp, org_tp, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Payee)<-[:PAID]-(c:Expenditure)<-[:SPENT]-(b:Committee)")
    c.append("MATCH (c)-[:HAPPENED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if ids2 is not None:
        c.append("MATCH (t:Payee)<-[:PAID]-(u:Expenditure)<-[:SPENT]-(b:Committee)")
        c.append("MATCH (u)-[:HAPPENED_ON]->(v:Day)")
        c.append("WHERE ID(t) IN $ids2")
        c.append("AND v.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND v.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if cmte_pty_affiliation is not None:
        c.append("AND b.cmte_pty_affiliation = toUpper($cmte_pty_affiliation)")
    if cmte_dsgn is not None:
        c.append("AND b.cmte_dsgn = toUpper($cmte_dsgn)")
    if cmte_tp is not None:
        c.append("AND b.cmte_tp = toUpper($cmte_tp)")
    if org_tp is not None:
        c.append("AND b.org_tp = toUpper($org_tp)")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, org_tp=org_tp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_tweeter_candidate(tx, ids, ids2, cand_pty_affiliation, cand_office, cand_office_st, cand_office_district, cand_election_yr, cand_ici, skip, limit):
    c = []
    c.append("MATCH (a:Tweeter)<-[:ASSOCIATED_WITH]-(b:Candidate)")
    c.append("WHERE ID(a) IN $ids")
    if ids2 is not None:
        c.append("MATCH (t:Tweeter)<-[:ASSOCIATED_WITH]-(b:Candidate)")
        c.append("WHERE ID(t) IN $ids2")
    if cand_pty_affiliation is not None:
        c.append("AND b.cand_pty_affiliation = toUpper($cand_pty_affiliation)")
    if cand_office is not None:
        c.append("AND b.cand_office = toUpper($cand_office)")
    if cand_office_st is not None:
        c.append("AND b.cand_office_st = toUpper($cand_office_st)")
    if cand_office_district is not None:
        c.append("AND b.cand_office_district = $cand_office_district")
    if cand_election_yr is not None:
        c.append("AND b.cand_election_yr = $cand_election_yr")
    if cand_ici is not None:
        c.append("AND b.cand_ici = toUpper($cand_ici)")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, skip=skip, limit=limit).graph()

def graph_traverse_associations_tweeter_source(tx, ids, ids2, bias_score, factually_questionable_flag, conspiracy_flag, hate_group_flag, propaganda_flag, satire_flag, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Tweeter)<-[:PUBLISHED_BY]-(c:Tweet)-[:MENTIONS]->(:Link)-[:ASSOCIATED_WITH]-(:Domain)-[:ASSOCIATED_WITH]->(b:Source)")
    c.append("MATCH (c)-[:PUBLISHED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if ids2 is not None:
        c.append("MATCH (t:Tweeter)<-[:PUBLISHED_BY]-(u:Tweet)-[:MENTIONS]->(:Link)-[:ASSOCIATED_WITH]-(:Domain)-[:ASSOCIATED_WITH]->(b:Source)")
        c.append("MATCH (u)-[:PUBLISHED_ON]->(v:Day)")
        c.append("WHERE ID(t) IN $ids2")
        c.append("AND v.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND v.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if bias_score is not None:
        c.append("AND b.bias_score IN $bias_score")
    if factually_questionable_flag == 1:
        c.append("AND b.factually_questionable_flag = 1")
    if conspiracy_flag == 1:
        c.append("AND b.conspiracy_flag = 1")
    if hate_group_flag == 1:
        c.append("AND b.hate_group_flag = 1")
    if propaganda_flag == 1:
        c.append("AND b.propaganda_flag = 1")
    if satire_flag == 1:
        c.append("AND b.satire_flag = 1")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, bias_score=bias_score, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_source_tweeter(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Source)<-[:ASSOCIATED_WITH]-(:Domain)<-[:ASSOCIATED_WITH]-(:Link)<-[:MENTIONS]-(c:Tweet)-[:PUBLISHED_BY]->(b:Tweeter)")
    c.append("MATCH (c)-[:PUBLISHED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if ids2 is not None:
        c.append("MATCH (t:Source)<-[:ASSOCIATED_WITH]-(:Domain)<-[:ASSOCIATED_WITH]-(:Link)<-[:MENTIONS]-(u:Tweet)-[:PUBLISHED_BY]->(b:Tweeter)")
        c.append("MATCH (u)-[:PUBLISHED_ON]->(v:Day)")
        c.append("WHERE ID(t) IN $ids2")
        c.append("AND v.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND v.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_buyer_page(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Buyer)<-[:PAID_BY]-(c:Ad)-[:PUBLISHED_BY]->(b:Page)")
    c.append("MATCH (c)-[:CREATED_ON|DELIVERED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if ids2 is not None:
        c.append("MATCH (t:Buyer)<-[:PAID_BY]-(u:Ad)-[:PUBLISHED_BY]->(b:Page)")
        c.append("MATCH (u)-[:CREATED_ON|DELIVERED_ON]->(v:Day)")
        c.append("WHERE ID(t) IN $ids2")
        c.append("AND v.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND v.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_associations_page_buyer(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Page)<-[:PUBLISHED_BY]-(c:Ad)-[:PAID_BY]->(b:Buyer)")
    c.append("MATCH (c)-[:CREATED_ON|DELIVERED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if ids2 is not None:
        c.append("MATCH (t:Page)<-[:PUBLISHED_BY]-(u:Ad)-[:PAID_BY]->(b:Buyer)")
        c.append("MATCH (u)-[:CREATED_ON|DELIVERED_ON]->(v:Day)")
        c.append("WHERE ID(t) IN $ids2")
        c.append("AND v.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND v.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_candidate_committee(tx, ids, ids2, sup_opp, purpose, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Candidate)<-[:IDENTIFIES]-(d:Expenditure)<-[:SPENT]-(b:Committee)")
    c.append("MATCH (d)-[:HAPPENED_ON]->(e:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND ID(b) IN $ids2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if sup_opp is not None:
        c.append("AND d.sup_opp = toUpper($sup_opp)")
    if purpose is not None:
        c.append("AND d.purpose CONTAINS toUpper($purpose)")
    if amndt_ind is not None:
        c.append("AND d.amndt_ind = toUpper($amndt_ind)")
    if gt is not None:
        c.append("AND $gt < d.transaction_amt")
    if lte is not None:
        c.append("AND d.transaction_amt <= $lte")
    c.append("CALL apoc.nodes.collapse([a, d], {properties: 'overwrite'})")
    c.append("YIELD from, rel, to")
    c.append("WITH DISTINCT from AS x")
    c.append("MATCH (x)")
    c.append("WHERE labels(x) = []")
    c.append("RETURN x")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_committee_candidate(tx, ids, ids2, sup_opp, purpose, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Committee)-[:SPENT]->(d:Expenditure)-[:IDENTIFIES]->(b:Candidate)")
    c.append("MATCH (d)-[:HAPPENED_ON]->(e:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND ID(b) IN $ids2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if sup_opp is not None:
        c.append("AND d.sup_opp = toUpper($sup_opp)")
    if purpose is not None:
        c.append("AND d.purpose CONTAINS toUpper($purpose)")
    if amndt_ind is not None:
        c.append("AND d.amndt_ind = toUpper($amndt_ind)")
    if gt is not None:
        c.append("AND $gt < d.transaction_amt")
    if lte is not None:
        c.append("AND d.transaction_amt <= $lte")
    c.append("CALL apoc.nodes.collapse([b, d], {properties: 'overwrite'})")
    c.append("YIELD from, rel, to")
    c.append("WITH DISTINCT from AS x")
    c.append("MATCH (x)")
    c.append("WHERE labels(x) = []")
    c.append("RETURN x")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_committee_committee(tx, ids, ids2, cand_pty_affiliation, cand_office, cand_office_st, cand_office_district, cand_election_yr, cand_ici, intermediaries, direction, transaction_tp, transaction_pgi, rpt_tp, contribution_amndt_ind, contribution_gt, contribution_lte, sup_opp, purpose, expenditure_amndt_ind, expenditure_gt, expenditure_lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    if intermediaries == "contribution":
        if direction == "receipts":
            c.append("MATCH (a:Committee)<-[:CONTRIBUTED_TO]-(d:Contribution)<-[:CONTRIBUTED_TO]-(b:Committee)")
        elif direction == "disbursements":
            c.append("MATCH (a:Committee)-[:CONTRIBUTED_TO]->(d:Contribution)-[:CONTRIBUTED_TO]->(b:Committee)")
        else:
            c.append("MATCH (a:Committee)-[:CONTRIBUTED_TO]-(d:Contribution)-[:CONTRIBUTED_TO]-(b:Committee)")
        c.append("MATCH (d)-[:HAPPENED_ON]->(e:Day)")
        c.append("WHERE ID(a) IN $ids")
        c.append("AND ID(b) IN $ids2")
        c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
        if transaction_tp is not None:
            c.append("AND d.transaction_tp = toUpper($transaction_tp)")
        if transaction_pgi is not None:
            c.append("AND d.transaction_pgi = toUpper($transaction_pgi)")
        if rpt_tp is not None:
            c.append("AND d.rpt_tp = toUpper($rpt_tp)")
        if amndt_ind is not None:
            c.append("AND d.amndt_ind = toUpper($contribution_amndt_ind)")
        if gt is not None:
            c.append("AND $contribution_gt < d.transaction_amt")
        if lte is not None:
            c.append("AND d.transaction_amt <= $contribution_lte")
        c.append("RETURN DISTINCT d")
    else:
        c.append("MATCH (a:Committee)-[:SPENT]->(c:Expenditure)-[:IDENTIFIES]->(x:Candidate)<-[IDENTIFIES]-(d:Expenditure)<-[:SPENT]-(b:Committee)")
        c.append("MATCH (c)-[:HAPPENED_ON]->(e:Day)<-[:HAPPENED_ON]-(d)")
        c.append("WHERE ID(a) IN $ids")
        c.append("AND ID(b) IN $ids2")
        c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
        c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
        if sup_opp is not None:
            c.append("AND c.sup_opp = toUpper($sup_opp)")
            c.append("AND d.sup_opp = toUpper($sup_opp)")
        if purpose is not None:
            c.append("AND c.purpose CONTAINS toUpper($purpose)")
            c.append("AND d.purpose CONTAINS toUpper($purpose)")
        if amndt_ind is not None:
            c.append("AND c.amndt_ind = toUpper($expenditure_amndt_ind)")
            c.append("AND d.amndt_ind = toUpper($expenditure_amndt_ind)")
        if gt is not None:
            c.append("AND $expenditure_gt < c.transaction_amt")
            c.append("AND $expenditure_gt < d.transaction_amt")
        if lte is not None:
            c.append("AND c.transaction_amt <= $expenditure_lte")
            c.append("AND d.transaction_amt <= $expenditure_lte")
        if cand_pty_affiliation is not None:
            c.append("AND x.cand_pty_affiliation = toUpper($cand_pty_affiliation)")
        if cand_office is not None:
            c.append("AND x.cand_office = toUpper($cand_office)")
        if cand_office_st is not None:
            c.append("AND x.cand_office_st = toUpper($cand_office_st)")
        if cand_office_district is not None:
            c.append("AND x.cand_office_district = $cand_office_district")
        if cand_election_yr is not None:
            c.append("AND x.cand_election_yr = $cand_election_yr")
        if cand_ici is not None:
            c.append("AND x.cand_ici = toUpper($cand_ici)")
        c.append("RETURN x")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, transaction_tp=transaction_tp, transaction_pgi=transaction_pgi, rpt_tp=rpt_tp, contribution_amndt_ind=contribution_amndt_ind, contribution_gt=contribution_gt, contribution_lte=contribution_lte, sup_opp=sup_opp, purpose=purpose, expenditure_amndt_ind=expenditure_amndt_ind, expenditure_gt=expenditure_gt, expenditure_lte=expenditure_lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_committee_donor(tx, ids, ids2, transaction_tp, transaction_pgi, rpt_tp, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Committee)<-[:CONTRIBUTED_TO]-(d:Contribution)<-[:CONTRIBUTED_TO]-(b:Donor)")
    c.append("MATCH (d)-[:HAPPENED_ON]->(e:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND ID(b) IN $ids2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if transaction_tp is not None:
        c.append("AND d.transaction_tp = toUpper($transaction_tp)")
    if transaction_pgi is not None:
        c.append("AND d.transaction_pgi = toUpper($transaction_pgi)")
    if rpt_tp is not None:
        c.append("AND d.rpt_tp = toUpper($rpt_tp)")
    if amndt_ind is not None:
        c.append("AND d.amndt_ind = toUpper($amndt_ind)")
    if gt is not None:
        c.append("AND $gt < d.transaction_amt")
    if lte is not None:
        c.append("AND d.transaction_amt <= $lte")
    c.append("RETURN DISTINCT d")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, transaction_tp=transaction_tp, transaction_pgi=transaction_pgi, rpt_tp=rpt_tp, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_committee_payee(tx, ids, ids2, sup_opp, purpose, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Committee)-[:SPENT]->(d:Expenditure)-[:PAID]->(b:Payee)")
    c.append("MATCH (d)-[:HAPPENED_ON]->(e:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND ID(b) IN $ids2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if sup_opp is not None:
        c.append("AND d.sup_opp = toUpper($sup_opp)")
    if purpose is not None:
        c.append("AND d.purpose CONTAINS toUpper($purpose)")
    if amndt_ind is not None:
        c.append("AND d.amndt_ind = toUpper($amndt_ind)")
    if gt is not None:
        c.append("AND $gt < d.transaction_amt")
    if lte is not None:
        c.append("AND d.transaction_amt <= $lte")
    c.append("OPTIONAL MATCH (d)-[:IDENTIFIES]->(h:Candidate)")
    c.append("CALL apoc.nodes.collapse([h, d], {properties: 'overwrite'})")
    c.append("YIELD from, rel, to")
    c.append("WITH DISTINCT from AS x")
    c.append("MATCH (x)")
    c.append("WHERE labels(x) = []")
    c.append("RETURN x")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_donor_committee(tx, ids, ids2, transaction_tp, transaction_pgi, rpt_tp, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Donor)-[:CONTRIBUTED_TO]->(d:Contribution)-[:CONTRIBUTED_TO]->(b:Committee)")
    c.append("MATCH (d)-[:HAPPENED_ON]->(e:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND ID(b) IN $ids2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if transaction_tp is not None:
        c.append("AND d.transaction_tp = toUpper($transaction_tp)")
    if transaction_pgi is not None:
        c.append("AND d.transaction_pgi = toUpper($transaction_pgi)")
    if rpt_tp is not None:
        c.append("AND d.rpt_tp = toUpper($rpt_tp)")
    if amndt_ind is not None:
        c.append("AND d.amndt_ind = toUpper($amndt_ind)")
    if gt is not None:
        c.append("AND $gt < d.transaction_amt")
    if lte is not None:
        c.append("AND d.transaction_amt <= $lte")
    c.append("RETURN DISTINCT d")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, transaction_tp=transaction_tp, transaction_pgi=transaction_pgi, rpt_tp=rpt_tp, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_payee_committee(tx, ids, ids2, sup_opp, purpose, amndt_ind, gt, lte, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Payee)<-[:PAID]-(d:Expenditure)<-[:SPENT]-(b:Committee)")
    c.append("MATCH (d)-[:HAPPENED_ON]->(e:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND ID(b) IN $ids2")
    c.append("AND e.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND e.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    if sup_opp is not None:
        c.append("AND d.sup_opp = toUpper($sup_opp)")
    if purpose is not None:
        c.append("AND d.purpose CONTAINS toUpper($purpose)")
    if amndt_ind is not None:
        c.append("AND d.amndt_ind = toUpper($amndt_ind)")
    if gt is not None:
        c.append("AND $gt < d.transaction_amt")
    if lte is not None:
        c.append("AND d.transaction_amt <= $lte")
    c.append("OPTIONAL MATCH (d)-[:IDENTIFIES]->(h:Candidate)")
    c.append("CALL apoc.nodes.collapse([h, d], {properties: 'overwrite'})")
    c.append("YIELD from, rel, to")
    c.append("WITH DISTINCT from AS x")
    c.append("MATCH (x)")
    c.append("WHERE labels(x) = []")
    c.append("RETURN x")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_tweeter_source(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Tweeter)<-[:PUBLISHED_BY]-(c:Tweet)-[:MENTIONS]->(:Link)-[:ASSOCIATED_WITH]-(:Domain)-[:ASSOCIATED_WITH]->(b:Source)")
    c.append("MATCH (c)-[:PUBLISHED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND ID(b) IN $ids2")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN DISTINCT c")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_source_tweeter(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Source)<-[:ASSOCIATED_WITH]-(:Domain)<-[:ASSOCIATED_WITH]-(:Link)<-[:MENTIONS]-(c:Tweet)-[:PUBLISHED_BY]->(b:Tweeter)")
    c.append("MATCH (c)-[:PUBLISHED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND ID(b) IN $ids2")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN DISTINCT c")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_buyer_page(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Buyer)<-[:PAID_BY]-(c:Ad)-[:PUBLISHED_BY]->(b:Page)")
    c.append("MATCH (c)-[:CREATED_ON|DELIVERED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND ID(b) IN $ids2")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN DISTINCT c")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_intermediaries_page_buyer(tx, ids, ids2, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH (a:Page)<-[:PUBLISHED_BY]-(c:Ad)-[:PAID_BY]->(b:Buyer)")
    c.append("MATCH (c)-[:CREATED_ON|DELIVERED_ON]->(d:Day)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND ID(b) IN $ids2")
    c.append("AND d.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND d.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN DISTINCT c")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()

def graph_traverse_relationships_contribution_contributor(tx, ids, skip, limit):
    c = []
    c.append("MATCH (a:Contribution)<-[:CONTRIBUTED_TO]-(b)")
    c.append("WHERE ID(a) IN $ids")
    c.append("AND (b:Donor OR b:Committee)")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, skip=skip, limit=limit).graph()

def graph_traverse_relationships_contribution_recipient(tx, ids, skip, limit):
    c = []
    c.append("MATCH (a:Contribution)-[:CONTRIBUTED_TO]->(b:Committee)")
    c.append("WHERE ID(a) IN $ids")
    c.append("RETURN DISTINCT b")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, skip=skip, limit=limit).graph()
