def data_preview_organization_committee(es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, count):
    q = {
        "query": {
            "bool": {
                "should": [],
                "minimum_should_match": 1,
                "must_not": []
            }
        }
    }
    if include_terms is not None:
        for term in include_terms:
            q["query"]["bool"]["should"].append({
                "match": {
                    "row.cmte_nm": term
                }
            })
    if include_ids is not None:
        for id in include_ids:
            q["query"]["bool"]["should"].append({
                "match": {
                    "row.cmte_id": id
                }
            })
    if exclude_terms is not None:
        for term in exclude_terms:
            q["query"]["bool"]["must_not"].append({
                "match": {
                    "row.cmte_nm": term
                }
            })
    if exclude_ids is not None:
        for id in exclude_ids:
            q["query"]["bool"]["must_not"].append({
                "match": {
                    "row.cmte_id": id
                }
            })
    if count is True:
        response = es.count(index="federal_fec_committees", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(index="federal_fec_committees", body=q, filter_path=["hits.hits._source.row.cmte_id", "hits.hits._source.row.cmte_nm"])
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                elements.append({
                    "cmte_id": hit["_source"]["row"]["cmte_id"],
                    "cmte_nm": hit["_source"]["row"]["cmte_nm"]
                })
            return elements
        except:
            return []

def data_preview_organization_employer(es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, count):
    q = {
        "query": {
            "bool": {
                "should": [],
                "minimum_should_match": 1,
                "must_not": []
            }
        },
        "collapse": {
            "field": "row.source.donor.employer.keyword"
        }
    }
    if include_terms is not None:
        for term in include_terms:
            q["query"]["bool"]["should"].append({
                "match": {
                    "row.source.donor.employer": term
                }
            })
    if exclude_terms is not None:
        for term in exclude_terms:
            q["query"]["bool"]["must_not"].append({
                "match": {
                    "row.source.donor.employer": term
                }
            })
    if count is True:
        response = es.count(index="federal_fec_contributions", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(index="federal_fec_contributions", body=q, filter_path=["hits.hits._source.row.source.donor.employer"])
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                elements.append({
                    "name": hit["_source"]["row"]["source"]["donor"]["employer"]
                })
            return elements
        except:
            return []

def data_preview_person_candidate(es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, count):
    q = {
        "query": {
            "bool": {
                "should": [],
                "minimum_should_match": 1,
                "must_not": []
            }
        }
    }
    if include_terms is not None:
        for term in include_terms:
            q["query"]["bool"]["should"].append({
                "match": {
                    "row.cand_name": term
                }
            })
    if include_ids is not None:
        for id in include_ids:
            q["query"]["bool"]["should"].append({
                "match": {
                    "row.cand_id": id
                }
            })
    if exclude_terms is not None:
        for term in exclude_terms:
            q["query"]["bool"]["must_not"].append({
                "match": {
                    "row.cand_name": term
                }
            })
    if exclude_ids is not None:
        for id in exclude_ids:
            q["query"]["bool"]["must_not"].append({
                "match": {
                    "row.cand_id": id
                }
            })
    if count is True:
        response = es.count(index="federal_fec_candidates", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(index="federal_fec_candidates", body=q, filter_path=["hits.hits._source.row.cand_id", "hits.hits._source.row.cand_name"])
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                elements.append({
                    "cand_id": hit["_source"]["row"]["cand_id"],
                    "cand_name": hit["_source"]["row"]["cand_name"]
                })
            return elements
        except:
            return []

def data_preview_person_donor(es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, count):
    q = {
        "query": {
            "bool": {
                "should": [],
                "minimum_should_match": 1,
                "must_not": []
            }
        },
        "collapse": {
            "field": "processed.source.donor.name.keyword"
        }
    }
    if include_terms is not None:
        for term in include_terms:
            q["query"]["bool"]["should"].append({
                "match_phrase": {
                    "processed.source.donor.name": {
                        "query": term,
                        "slop": 2
                    }
                }
            })
    if exclude_terms is not None:
        for term in exclude_terms:
            q["query"]["bool"]["must_not"].append({
                "match_phrase": {
                    "processed.source.donor.name": {
                        "query": term,
                        "slop": 2
                    }
                }
            })
    if count is True:
        response = es.count(index="federal_fec_contributions", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(index="federal_fec_contributions", body=q, filter_path=["hits.hits._source.processed.source.donor.name"])
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                elements.append({
                    "name": hit["_source"]["processed"]["source"]["donor"]["name"]
                })
            return elements
        except:
            return []

def data_preview_job(es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, count):
    q = {
        "query": {
            "bool": {
                "should": [],
                "minimum_should_match": 1,
                "must_not": []
            }
        },
        "collapse": {
            "field": "row.source.donor.occupation.keyword"
        }
    }
    if include_terms is not None:
        for term in include_terms:
            q["query"]["bool"]["should"].append({
                "match": {
                    "row.source.donor.occupation": term
                }
            })
    if exclude_terms is not None:
        for term in exclude_terms:
            q["query"]["bool"]["must_not"].append({
                "match": {
                    "row.source.donor.occupation": term
                }
            })
    if count is True:
        response = es.count(index="federal_fec_contributions", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(index="federal_fec_contributions", body=q, filter_path=["hits.hits._source.row.source.donor.occupation"])
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                elements.append({
                    "term": hit["_source"]["row"]["source"]["donor"]["occupation"]
                })
            return elements
        except:
            return []
