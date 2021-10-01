from ...helpers import map_keys

# main query body

def make_query():
    return {
        "query": {
            "bool": {
                "must": [],
                "must_not": [],
                "filter": []
            }
        }
    }

def add_must_clause(q, clause):
    q["query"]["bool"]["must"].append(clause)
    return q

def add_not_clause(q, clause):
    q["query"]["bool"]["must_not"].append(clause)
    return q

def add_filter_clause(q, clause):
    q["query"]["bool"]["filter"].append(clause)
    return q

def set_query_dates(q, field, mindate, maxdate):
    return add_filter_clause(q, {
        "range": {
            field: {
                "gte": mindate,
                "lte": maxdate
            }
        }
    })

# should subqueries

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

# iterate on terms and ids
def set_query_clauses(q, template, list_settings, include, exclude):
    for setting in list_settings:
        subquery = make_should_subquery()
        if template in setting["templates"]:
            if "terms" in setting:
                for criteria in setting["terms"]:
                    for term in include["terms"][setting["position"]] or []:
                        query = term.lower() if criteria["action"] == "term" else term
                        if "slop" in criteria:
                            query = {
                                "query": query,
                                "slop": criteria["slop"]
                            }
                        if criteria["action"] == "multi_match":
                            clause = {
                                criteria["action"]: query
                            }
                        else:
                            clause = {
                                criteria["action"]: {
                                    criteria["field"]: query
                                }
                            }
                        subquery = add_should_clause(subquery, clause)
                    if exclude is not None:
                        for term in exclude["terms"][setting["position"]] or []:
                            query = term.lower() if criteria["action"] == "term" else term
                            if "slop" in criteria:
                                query = {
                                    "query": query,
                                    "slop": criteria["slop"]
                                }
                            if criteria["action"] == "multi_match":
                                clause = {
                                    criteria["action"]: query
                                }
                            else:
                                clause = {
                                    criteria["action"]: {
                                        criteria["field"]: query
                                    }
                                }
                            q = add_not_clause(q, clause)
            if "ids" in setting:
                for criteria in setting["ids"]:
                    for id in include["ids"][setting["position"]] or []:
                        query = id.lower() if criteria["action"] == "term" else id
                        clause = {
                            criteria["action"]: {
                                criteria["field"]: query
                            }
                        }
                        subquery = add_should_clause(subquery, clause)
                    if exclude is not None:
                        for id in exclude["ids"][setting["position"]] or []:
                            query = id.lower() if criteria["action"] == "term" else id
                            clause = {
                                criteria["action"]: {
                                    criteria["field"]: query
                                }
                            }
                            q = add_not_clause(q, clause)
            if "filters" in setting:
                for criteria in setting["filters"]:
                    for key, values in include["filters"][setting["position"]].items() or []:
                        subquery = make_should_subquery()
                        for value in values or []:
                            subquery = add_should_clause(subquery, {
                                "term": {
                                    map_keys(criteria, key, value): value.lower() if isinstance(value, str) else value
                                }
                            })
                        q = add_must_clause(q, subquery)
                    if exclude is not None:
                        for key, values in exclude["filters"][setting["position"]].items() or []:
                            for value in values or []:
                                clause = {
                                    "term": {
                                        map_keys(criteria, key, value): value.lower() if isinstance(value, str) else value
                                    }
                                }
                                q = add_not_clause(q, clause)
        if len(subquery["bool"]["should"]) > 0:
            q = add_must_clause(q, subquery)
    return q
