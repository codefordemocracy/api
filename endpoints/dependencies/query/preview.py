from .builder.functions import make_query, add_must_clause, add_not_clause, add_filter_clause, make_should_subquery, add_should_clause
from .builder.responses import get_response
from ..helpers import map_keys

def data_preview_organization_committee(es, include_terms, include_ids, include_filters, exclude_terms, exclude_ids, exclude_filters, skip, limit, count):
    q = make_query()
    subquery = make_should_subquery()
    if include_terms is not None:
        for term in include_terms:
            subquery = add_should_clause(subquery, {
                "match_phrase": {
                    "row.cmte_nm": {
                        "query": term,
                        "slop": 5
                    }
                }
            })
        q = add_must_clause(q, subquery)
    if include_ids is not None:
        for id in include_ids:
            subquery = add_should_clause(subquery, {
                "term": {
                    "row.cmte_id": id.lower()
                }
            })
    q = add_must_clause(q, subquery)
    if include_filters is not None:
        for key, values in include_filters.items():
            subquery = make_should_subquery()
            for value in values or []:
                if value is not None:
                    subquery = add_should_clause(subquery, {
                        "term": {
                            map_keys("committee", key): value.lower() if isinstance(value, str) else value
                        }
                    })
            q = add_must_clause(q, subquery)
    if exclude_terms is not None:
        for term in exclude_terms:
            q = add_not_clause(q, {
                "match_phrase": {
                    "row.cmte_nm": {
                        "query": term,
                        "slop": 5
                    }
                }
            })
    if exclude_ids is not None:
        for id in exclude_ids:
            q = add_not_clause(q, {
                "term": {
                    "row.cmte_id": id.lower()
                }
            })
    if exclude_filters is not None:
        for key, values in exclude_filters.items():
            for value in values or []:
                if value is not None:
                    q = add_not_clause(q, {
                        "term": {
                            map_keys("committee", key): value.lower() if isinstance(value, str) else value
                        }
                    })
    response = get_response(es, "federal_fec_committees", q, skip, limit, count, False,
        filter_path=["hits.hits._source.row.cmte_id", "hits.hits._source.row.cmte_nm"]
    )
    if count is not True:
        elements = []
        for hit in response:
            elements.append({
                "cmte_id": hit["_source"]["row"]["cmte_id"],
                "cmte_nm": hit["_source"]["row"]["cmte_nm"]
            })
        return elements
    return response

def data_preview_organization_employer(es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, count):
    q = make_query()
    q["collapse"] = {
        "field": "row.source.donor.employer.keyword"
    }
    subquery = make_should_subquery()
    if include_terms is not None:
        for term in include_terms:
            subquery = add_should_clause(subquery, {
                "match_phrase": {
                    "row.source.donor.employer": {
                        "query": term,
                        "slop": 5
                    }
                }
            })
    q = add_must_clause(q, subquery)
    if exclude_terms is not None:
        for term in exclude_terms:
            q = add_not_clause(q, {
                "match_phrase": {
                    "row.source.donor.employer": {
                        "query": term,
                        "slop": 5
                    }
                }
            })
    response = get_response(es, "federal_fec_contributions", q, skip, limit, count, False,
        filter_path=["hits.hits._source.row.source.donor.employer"]
    )
    if count is not True:
        elements = []
        for hit in response:
            elements.append({
                "name": hit["_source"]["row"]["source"]["donor"]["employer"]
            })
        return elements
    return response

def data_preview_organization_source(es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, count):
    q = make_query()
    q["collapse"] = {
        "field": "extracted.source.url.keyword"
    }
    subquery = make_should_subquery()
    if include_terms is not None:
        for term in include_terms:
            subquery = add_should_clause(subquery, {
                "match_phrase": {
                    "extracted.source.sitename": {
                        "query": term,
                        "slop": 5
                    }
                }
            })
    if include_ids is not None:
        for id in include_ids:
            subquery = add_should_clause(subquery, {
                "term": {
                    "extracted.source.url": id.lower()
                }
            })
    q = add_must_clause(q, subquery)
    if exclude_terms is not None:
        for term in exclude_terms:
            q = add_not_clause(q, {
                "match_phrase": {
                    "extracted.source.sitename": {
                        "query": term,
                        "slop": 5
                    }
                }
            })
    if exclude_ids is not None:
        for id in exclude_ids:
            q = add_not_clause(q, {
                "match": {
                    "extracted.source.url": id.lower()
                }
            })
    response = get_response(es, "news_articles", q, skip, limit, count, False,
        filter_path=["hits.hits._source.extracted.source"]
    )
    if count is not True:
        elements = []
        for hit in response:
            elements.append({
                "sitename": hit["_source"]["extracted"]["source"]["sitename"],
                "url": hit["_source"]["extracted"]["source"]["url"]
            })
        return elements
    return response

def data_preview_person_candidate(es, include_terms, include_ids, include_filters, exclude_terms, exclude_ids, exclude_filters, skip, limit, count):
    q = make_query()
    subquery = make_should_subquery()
    if include_terms is not None:
        for term in include_terms:
            subquery = add_should_clause(subquery, {
                "match_phrase": {
                    "processed.row.cand_name": {
                        "query": term,
                        "slop": 5
                    }
                }
            })
    if include_ids is not None:
        subquery = make_should_subquery()
        for id in include_ids:
            subquery = add_should_clause(subquery, {
                "term": {
                    "row.cand_id": id.lower()
                }
            })
    q = add_must_clause(q, subquery)
    if include_filters is not None:
        for key, values in include_filters.items():
            subquery = make_should_subquery()
            for value in values or []:
                if value is not None:
                    subquery = add_should_clause(subquery, {
                        "term": {
                            map_keys("candidate", key): value.lower() if isinstance(value, str) else value
                        }
                    })
            q = add_must_clause(q, subquery)
    if exclude_terms is not None:
        for term in exclude_terms:
            q = add_not_clause(q, {
                "match_phrase": {
                    "processed.row.cand_name": {
                        "query": term,
                        "slop": 5
                    }
                }
            })
    if exclude_ids is not None:
        for id in exclude_ids:
            q = add_not_clause(q, {
                "term": {
                    "row.cand_id": id.lower()
                }
            })
    if exclude_filters is not None:
        for key, values in exclude_filters.items():
            for value in values or []:
                if value is not None:
                    q = add_not_clause(q, {
                        "term": {
                            map_keys("candidate", key): value.lower() if isinstance(value, str) else value
                        }
                    })
    response = get_response(es, "federal_fec_candidates", q, skip, limit, count, False,
        filter_path=["hits.hits._source.row.cand_id", "hits.hits._source.row.cand_name", "hits.hits._source.processed.row.cand_name"]
    )
    if count is not True:
        elements = []
        for hit in response:
            elements.append({
                "cand_id": hit["_source"]["row"]["cand_id"],
                "cand_name": hit["_source"].get("processed", {}).get("row", {}).get("cand_name") if hit["_source"].get("processed", {}).get("row", {}).get("cand_name") is not None else hit["_source"]["row"]["cand_id"]
            })
        return elements
    return response

def data_preview_person_donor_fec(es, include_terms, include_ids, include_filters, exclude_terms, exclude_ids, exclude_filters, skip, limit, count):
    q = make_query()
    q["collapse"] = {
        "field": "processed.source.donor.name.keyword"
    }
    subquery = make_should_subquery()
    if include_terms is not None:
        for term in include_terms:
            subquery = add_should_clause(subquery, {
                "match_phrase": {
                    "processed.source.donor.name": {
                        "query": term,
                        "slop": 2
                    }
                }
            })
    q = add_must_clause(q, subquery)
    if include_filters is not None:
        for key, values in include_filters.items():
            subquery = make_should_subquery()
            for value in values or []:
                if value is not None:
                    subquery = add_should_clause(subquery, {
                        "term": {
                            map_keys("donor", key): value.lower() if isinstance(value, str) else value
                        }
                    })
            q = add_must_clause(q, subquery)
    if exclude_terms is not None:
        for term in exclude_terms:
            q = add_not_clause(q, {
                "match_phrase": {
                    "processed.source.donor.name": {
                        "query": term,
                        "slop": 2
                    }
                }
            })
    if exclude_filters is not None:
        for key, values in exclude_filters.items():
            for value in values or []:
                if value is not None:
                    q = add_not_clause(q, {
                        "term": {
                            map_keys("donor", key): value.lower() if isinstance(value, str) else value
                        }
                    })
    response = get_response(es, "federal_fec_contributions", q, skip, limit, count, False,
        filter_path=["hits.hits._source.processed.source.donor.name"]
    )
    if count is not True:
        elements = []
        for hit in response:
            elements.append({
                "name": hit["_source"]["processed"]["source"]["donor"]["name"]
            })
        return elements
    return response

def data_preview_person_donor_lobbying(es, include_terms, include_ids, include_filters, exclude_terms, exclude_ids, exclude_filters, skip, limit, count):
    q = make_query()
    q["collapse"] = {
        "field": "child.lobbyist.name.keyword"
    }
    subquery = make_should_subquery()
    if include_terms is not None:
        for term in include_terms:
            subquery = add_should_clause(subquery, {
                "match_phrase": {
                    "child.lobbyist.name": {
                        "query": term,
                        "slop": 2
                    }
                }
            })
    q = add_must_clause(q, subquery)
    if exclude_terms is not None:
        for term in exclude_terms:
            q = add_not_clause(q, {
                "match_phrase": {
                    "child.lobbyist.name": {
                        "query": term,
                        "slop": 2
                    }
                }
            })
    response = get_response(es, "federal_senate_lobbying_contributions_nested,federal_house_lobbying_contributions_nested", q, skip, limit, count, False,
        filter_path=["hits.hits._source.child.lobbyist.name"]
    )
    if count is not True:
        elements = []
        for hit in response:
            elements.append({
                "name": hit["_source"]["child"]["lobbyist"]["name"]
            })
        return elements
    return response

def data_preview_job(es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, count):
    q = make_query()
    q["collapse"] = {
        "field": "row.source.donor.occupation.keyword"
    }
    subquery = make_should_subquery()
    if include_terms is not None:
        for term in include_terms:
            subquery = add_should_clause(subquery, {
                "match_phrase": {
                    "row.source.donor.occupation": {
                        "query": term,
                        "slop": 5
                    }
                }
            })
    q = add_must_clause(q, subquery)
    if exclude_terms is not None:
        for term in exclude_terms:
            q = add_not_clause(q, {
                "match_phrase": {
                    "row.source.donor.occupation": {
                        "query": term,
                        "slop": 5
                    }
                }
            })
    response = get_response(es, "federal_fec_contributions", q, skip, limit, count, False,
        filter_path=["hits.hits._source.row.source.donor.occupation"]
    )
    if count is not True:
        elements = []
        for hit in response:
            elements.append({
                "term": hit["_source"]["row"]["source"]["donor"]["occupation"]
            })
        return elements
    return response
