#########################################################
# uncover graph insights
#########################################################

def graph_uncover_contributors(tx, ids, labels, min_transaction_amt, skip, limit):
    c  = "MATCH p=(a)<-[:CONTRIBUTED_TO]-(t:Contribution)<-[:CONTRIBUTED_TO]-(d) "
    c+= "WHERE ID(a) IN $ids "
    c+= "AND t.transaction_amt >= $min_transaction_amt "
    if labels is not None:
        c+= "AND (" + labels + ") "
    c+= "RETURN p "
    c+= "SKIP $skip "
    c+= "LIMIT $limit "
    return tx.run(c, ids=ids, min_transaction_amt=min_transaction_amt, skip=skip, limit=limit).graph()
