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
                    # set lowercase keywords
                    if criteria["action"] == "term" or criteria["action"] == "terms":
                        include["terms"][setting["position"]] = [term.lower() if isinstance(term, str) else term for term in include["terms"][setting["position"]]]
                        exclude["terms"][setting["position"]] = [term.lower() if isinstance(term, str) else term for term in exclude["terms"][setting["position"]]]
                    # process query for includes
                    if criteria["action"] == "terms":
                        if include["terms"][setting["position"]] != []:
                            subquery = add_should_clause(subquery, {
                                criteria["action"]: {
                                    criteria["field"]: include["terms"][setting["position"]]
                                }
                            })
                    else:
                        for term in include["terms"][setting["position"]] or []:
                            if "slop" in criteria:
                                query = {
                                    "query": term,
                                    "slop": criteria["slop"]
                                }
                            else:
                                query = term
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
                    # process query for excludes
                    if exclude is not None:
                        if criteria["action"] == "terms":
                            if exclude["terms"][setting["position"]] != []:
                                clause = {
                                    criteria["action"]: {
                                        criteria["field"]: exclude["terms"][setting["position"]]
                                    }
                                }
                                if setting.get("type") == "nested":
                                    q = add_not_clause(q, {
                                        "nested": {
                                            "path": setting["path"],
                                            "query": clause
                                        }
                                    })
                                else:
                                    q = add_not_clause(q, clause)
                        else:
                            for term in exclude["terms"][setting["position"]] or []:
                                if "slop" in criteria:
                                    query = {
                                        "query": term,
                                        "slop": criteria["slop"]
                                    }
                                else:
                                    query = term
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
                                if setting.get("type") == "nested":
                                    q = add_not_clause(q, {
                                        "nested": {
                                            "path": setting["path"],
                                            "query": clause
                                        }
                                    })
                                else:
                                    q = add_not_clause(q, clause)
            if "ids" in setting:
                for field in setting["ids"]:
                    # process query for includes
                    if include["ids"][setting["position"]] != []:
                        subquery = add_should_clause(subquery, {
                            "terms": {
                                field: [id.lower() if isinstance(id, str) else id for id in include["ids"][setting["position"]]]
                            }
                        })
                    # process query for excludes
                    if exclude is not None:
                        if exclude["ids"][setting["position"]] != []:
                            clause = {
                                "terms": {
                                    field: [id.lower() if isinstance(id, str) else id for id in exclude["ids"][setting["position"]]]
                                }
                            }
                            if setting.get("type") == "nested":
                                q = add_not_clause(q, {
                                    "nested": {
                                        "path": setting["path"],
                                        "query": clause
                                    }
                                })
                            else:
                                q = add_not_clause(q, clause)
            if "filters" in setting:
                for criteria in setting["filters"]:
                    # process query for includes
                    for key, values in include["filters"][setting["position"]].items() or []:
                        clause = {
                            "terms": {
                                map_keys(criteria, key): [value.lower() if isinstance(value, str) else value for value in values]
                            }
                        }
                        if setting.get("type") == "nested":
                            q = add_must_clause(q, {
                                "nested": {
                                    "path": setting["path"],
                                    "query": clause
                                }
                            })
                        else:
                            q = add_must_clause(q, clause)
                    # process query for excludes
                    if exclude is not None:
                        for key, values in exclude["filters"][setting["position"]].items() or []:
                            clause = {
                                "terms": {
                                    map_keys(criteria, key): [value.lower() if isinstance(value, str) else value for value in values]
                                }
                            }
                            if setting.get("type") == "nested":
                                q = add_not_clause(q, {
                                    "nested": {
                                        "path": setting["path"],
                                        "query": clause
                                    }
                                })
                            else:
                                q = add_not_clause(q, clause)
        if len(subquery["bool"]["should"]) > 0:
            if setting.get("type") == "nested":
                q = add_must_clause(q, {
                    "nested": {
                        "path": setting["path"],
                        "query": subquery
                    }
                })
            else:
                q = add_must_clause(q, subquery)
    return q
