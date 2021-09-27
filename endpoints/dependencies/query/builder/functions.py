# main query body

def make_query():
    return {
        "query": {
            "bool": {
                "must": [],
                "must_not": []
            }
        }
    }

def set_query_dates(q, range):
    q["query"]["bool"]["must"].append({
        "range": range
    })
    return q

def add_must_clause(q, clause):
    q["query"]["bool"]["must"].append(clause)
    return q

# should subquery

def make_should_subquery():
    return {
        "bool": {
            "should": [],
            "minimum_should_match": 1
        }
    }

def add_should_clause(subquery, clause):
    subquery["bool"]["should"].append(clause)
    return subquery

# must_not subquery

def add_not_clause(q, clause):
    q["bool"]["must_not"].append(clause)
    return subquery

# iterate on terms and ids
def set_terms_ids(q, template, template_positions, include_terms, include_ids, exclude_terms, exclude_ids, term_settings, id_settings):
    subquery = make_should_subquery()
    for i in template_positions:
        if template == i[0]:
            if term_settings is not None and include_terms is not None:
                for term in include_terms[i[1]] or []:
                    subquery = add_should_clause(subquery, {
                        term_settings[0]: {
                            term_settings[1]: term
                        }
                    })
            if id_settings is not None and include_ids is not None:
                for id in include_ids[i[1]] or []:
                    subquery = add_should_clause(subquery, {
                        id_settings[0]: {
                            id_settings[1]: id
                        }
                    })
            if term_settings is not None and exclude_terms is not None:
                for term in exclude_terms[i[1]] or []:
                    q = add_not_clause(q, {
                        term_settings[0]: {
                            term_settings[1]: term
                        }
                    })
            if id_settings is not None and exclude_ids is not None:
                for id in exclude_ids[i[1]] or []:
                    q = add_not_clause(q, {
                        id_settings[0]: {
                            id_settings[1]: id
                        }
                    })
    q = add_must_clause(q, subquery)
    return q
