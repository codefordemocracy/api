#########################################################
# uncover graph insights
#########################################################

def graph_uncover_contributors(tx, ids, labels, min_transaction_amt, skip, limit, min_year, max_year, min_month, max_month, min_day, max_day):
    c = []
    c.append("MATCH p=(a)<-[:CONTRIBUTED_TO]-(t:Contribution)<-[:CONTRIBUTED_TO]-(d)")
    c.append("MATCH (t)-[:HAPPENED_ON]->(b:Day)")
    c.append("WHERE ID(a) IN $ids")
    if labels is not None:
        c.append("AND (" + labels + ")")
    c.append("AND t.transaction_amt >= $min_transaction_amt")
    c.append("AND b.date >= date({year: $min_year, month: $min_month, day: $min_day})")
    c.append("AND b.date <= date({year: $max_year, month: $max_month, day: $max_day})")
    c.append("RETURN p")
    c.append("SKIP $skip")
    c.append("LIMIT $limit")
    return tx.run(" ".join(c), ids=ids, min_transaction_amt=min_transaction_amt, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day).graph()
