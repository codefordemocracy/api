from ..helpers import clean_committees_names
from .preview import data_preview_organization_committee
from .builder.functions import make_query, set_terms_ids

def data_calculate_recipe_article(template, es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram):
    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "extracted.date": {
                                "gte": mindate,
                                "lte": maxdate
                            }
                        }
                    }
                ]
            }
        }
    }
    if len(include_terms) > 0 or len(include_ids) > 0:
        # List A
        subquery = {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
        if len(include_terms) > 0:
            if include_terms[0] is not None:
                for term in include_terms[0]:
                    if template in ["PMYZ", "WdMv", "RasK"]:
                        subquery["bool"]["should"].append({
                            "match_phrase": {
                                "extracted.text": {
                                    "query": term,
                                    "slop": 5
                                }
                            }
                        })
                    elif template in ["GSmB"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "extracted.text": term
                            }
                        })
        if len(include_ids) > 0:
            if include_ids[0] is not None:
                if template in ["PMYZ"]:
                    committees = data_preview_organization_committee(es, None, include_ids[0], None, None, 0, 10000, False)
                    for committee in committees:
                        name = clean_committees_names(committee["cmte_nm"])
                        subquery["bool"]["should"].append({
                            "match_phrase": {
                                "extracted.text": {
                                    "query": name,
                                    "slop": 5
                                }
                            }
                        })
        q["query"]["bool"]["must"].append(subquery)
    if len(exclude_terms) > 0 or len(exclude_ids) > 0:
        # List A
        subquery = {
            "bool": {
                "must_not": []
            }
        }
        if len(exclude_terms) > 0:
            if exclude_terms[0] is not None:
                for term in exclude_terms[0]:
                    if template in ["PMYZ", "WdMv", "RasK"]:
                        subquery["bool"]["must_not"].append({
                            "match_phrase": {
                                "extracted.text": {
                                    "query": term,
                                    "slop": 5
                                }
                            }
                        })
                    elif template in ["GSmB"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "extracted.text": term
                            }
                        })
        if len(exclude_ids) > 0:
            if exclude_ids[0] is not None:
                if template in ["PMYZ"]:
                    committees = data_preview_organization_committee(es, None, exclude_ids[0], None, None, 0, 10000, False)
                    for committee in committees:
                        name = clean_committees_names(committee["cmte_nm"])
                        subquery["bool"]["must_not"].append({
                            "match_phrase": {
                                "extracted.text": {
                                    "query": name,
                                    "slop": 5
                                }
                            }
                        })
        q["query"]["bool"]["must"].append(subquery)
    if orderby == "date":
        q["sort"] = {
            "extracted.date": {"order": orderdir},
        }
    if count is True:
        response = es.count(index="news_articles", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    elif histogram is True:
        q["aggs"] = {
            "dates": {
                "date_histogram": {
                    "field": "extracted.date",
                    "calendar_interval": "day",
                    "time_zone": "America/New_York"
                }
            }
        }
        response = es.search(index="news_articles", body=q, filter_path=["aggregations"])
        try:
            return response["aggregations"]["dates"]["buckets"]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(
            index="news_articles",
            body=q,
            filter_path=["hits.hits._source.extracted.title", "hits.hits._source.extracted.date", "hits.hits._source.extracted.url"]
        )
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                row = {
                    "date": hit["_source"]["extracted"]["date"][:10],
                    "title": hit["_source"]["extracted"]["title"],
                    "url": hit["_source"]["extracted"]["url"]
                }
                elements.append(row)
            return elements
        except:
            return []

def data_calculate_recipe_ad(template, es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram):
    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "obj.ad_creation_time": {
                                "gte": mindate,
                                "lte": maxdate
                            }
                        }
                    }
                ]
            }
        }
    }
    if len(include_terms) > 0 or len(include_ids) > 0:
        # List A
        subquery = {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
        if len(include_terms) > 0:
            if include_terms[0] is not None:
                for term in include_terms[0]:
                    if template in ["D3WE"]:
                        subquery["bool"]["should"].append({
                            "match_phrase": {
                                "obj.page_name": {
                                    "query": term,
                                    "slop": 5
                                }
                            }
                        })
                        subquery["bool"]["should"].append({
                            "match_phrase": {
                                "obj.funding_entity": {
                                    "query": term,
                                    "slop": 5
                                }
                            }
                        })
                    elif template in ["BuW8", "P2HG", "N7Jk"]:
                        subquery["bool"]["should"].append({
                            "match_phrase": {
                                "obj.ad_creative_body": {
                                    "query": term,
                                    "slop": 5
                                }
                            }
                        })
                    elif template in ["8HcR"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "obj.ad_creative_body": term
                            }
                        })
        if len(include_ids) > 0:
            if include_ids[0] is not None:
                if template in ["D3WE", "BuW8"]:
                    committees = data_preview_organization_committee(es, None, include_ids[0], None, None, 0, 10000, False)
                    for committee in committees:
                        name = clean_committees_names(committee["cmte_nm"])
                        if template in ["D3WE"]:
                            subquery["bool"]["should"].append({
                                "match_phrase": {
                                    "obj.page_name": {
                                        "query": name,
                                        "slop": 5
                                    }
                                }
                            })
                            subquery["bool"]["should"].append({
                                "match_phrase": {
                                    "obj.funding_entity": {
                                        "query": name,
                                        "slop": 5
                                    }
                                }
                            })
                        elif template in ["BuW8"]:
                            subquery["bool"]["should"].append({
                                "match_phrase": {
                                    "obj.ad_creative_body": {
                                        "query": name,
                                        "slop": 5
                                    }
                                }
                            })
        q["query"]["bool"]["must"].append(subquery)
    if len(exclude_terms) > 0 or len(exclude_ids) > 0:
        # List A
        subquery = {
            "bool": {
                "must_not": []
            }
        }
        if len(exclude_terms) > 0:
            if exclude_terms[0] is not None:
                for term in exclude_terms[0]:
                    if template in ["D3WE"]:
                        subquery["bool"]["must_not"].append({
                            "match_phrase": {
                                "obj.page_name": {
                                    "query": term,
                                    "slop": 5
                                }
                            }
                        })
                        subquery["bool"]["must_not"].append({
                            "match_phrase": {
                                "obj.funding_entity": {
                                    "query": term,
                                    "slop": 5
                                }
                            }
                        })
                    elif template in ["BuW8", "P2HG", "N7Jk"]:
                        subquery["bool"]["must_not"].append({
                            "match_phrase": {
                                "obj.ad_creative_body": {
                                    "query": term,
                                    "slop": 5
                                }
                            }
                        })
                    elif template in ["8HcR"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "obj.ad_creative_body": term
                            }
                        })
        if len(exclude_ids) > 0:
            if exclude_ids[0] is not None:
                if template in ["D3WE", "BuW8"]:
                    committees = data_preview_organization_committee(es, None, exclude_ids[0], None, None, 0, 10000, False)
                    for committee in committees:
                        name = clean_committees_names(committee["cmte_nm"])
                        if template in ["D3WE"]:
                            subquery["bool"]["must_not"].append({
                                "match_phrase": {
                                    "obj.page_name": {
                                        "query": name,
                                        "slop": 5
                                    }
                                }
                            })
                            subquery["bool"]["must_not"].append({
                                "match_phrase": {
                                    "obj.funding_entity": {
                                        "query": name,
                                        "slop": 5
                                    }
                                }
                            })
                        elif template in ["BuW8"]:
                            subquery["bool"]["must_not"].append({
                                "match_phrase": {
                                    "obj.ad_creative_body": {
                                        "query": name,
                                        "slop": 5
                                    }
                                }
                            })
        q["query"]["bool"]["must"].append(subquery)
    if orderby == "date":
        q["sort"] = {
            "obj.ad_creation_time": {"order": orderdir},
        }
    if count is True:
        response = es.count(index="facebook_ads", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    elif histogram is True:
        q["aggs"] = {
            "dates": {
                "date_histogram": {
                    "field": "obj.ad_creation_time",
                    "calendar_interval": "day",
                    "time_zone": "America/New_York"
                }
            }
        }
        response = es.search(index="facebook_ads", body=q, filter_path=["aggregations"])
        try:
            return response["aggregations"]["dates"]["buckets"]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(
            index="facebook_ads",
            body=q,
            filter_path=["hits.hits._source.obj.ad_creation_time", "hits.hits._source.obj.page_name", "hits.hits._source.obj.funding_entity", "hits.hits._source.obj.id"]
        )
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                row = {
                    "created_at": hit["_source"]["obj"]["ad_creation_time"][:10],
                    "page_name": hit["_source"]["obj"]["page_name"],
                    "funding_entity": hit["_source"]["obj"].get("funding_entity"),
                    "archive_url": "https://facebook.com/ads/library/?id=" + hit["_source"]["obj"]["id"]
                }
                elements.append(row)
            return elements
        except:
            return []

def data_calculate_recipe_contribution(template, es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram):
    # find committees for contributions to candidates
    if template in ["DXhw", "WK3K", "KR64", "F7Xn", "gXjA"]:
        template_positions = [("DXhw", 0), ("WK3K", 1), ("KR64", 1), ("F7Xn", 1), ("gXjA", 2)]
        # get included committee ids
        q = set_terms_ids(
            q=make_query(),
            template=template,
            template_positions=template_positions,
            include_terms=include_terms,
            include_ids=include_ids,
            exclude_terms=None,
            exclude_ids=None,
            term_settings=("match", "row.cand_name"),
            id_settings=("match", "row.cand_id"),
        )
        q["size"] = 10000
        response = es.search(index="federal_fec_candidates", body=q, filter_path=["hits.hits._source.linkages.committees"])
        committee_ids = []
        for hit in response["hits"]["hits"]:
            for committee in hit["_source"]["linkages"]["committees"]:
                committee_ids.append(committee["cmte_id"])
        for i in template_positions:
            if template == i[0]:
                include_terms[i[1]] = None
                include_ids[i[1]] = committee_ids
        # get excluded committee ids
        for i in template_positions:
            if template == i[0]:
                if exclude_terms[i[1]] is not None or exclude_ids[i[1]] is not None:
                    q = set_terms_ids(
                        q=make_query(),
                        template=template,
                        template_positions=template_positions,
                        include_terms=exclude_terms or [],
                        include_ids=exclude_ids or [],
                        exclude_terms=None,
                        exclude_ids=None,
                        term_settings=("match", "row.cand_name"),
                        id_settings=("match", "row.cand_id"),
                    )
                    q["size"] = 10000
                    response = es.search(index="federal_fec_candidates", body=q, filter_path=["hits.hits._source.linkages.committees"])
                    committee_ids = []
                    for hit in response["hits"]["hits"]:
                        for committee in hit["_source"]["linkages"]["committees"]:
                            committee_ids.append(committee["cmte_id"])
                    exclude_terms[i[1]] = None
                    exclude_ids[i[1]] = committee_ids
        # find the equivalent committee recipe
        for i in [("DXhw", "VqHR"), ("KWYZ", "dFMy"), ("WK3K", "IQL2"), ("KR64", "Bs5W"), ("F7Xn", "6peF"), ("gXjA", "F2mS")]:
            if template == i[0]:
                template = i[1]
    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "processed.date": {
                                "gte": mindate,
                                "lte": maxdate
                            }
                        }
                    }
                ],
                "filter": {
                    "bool": {
                        "must_not": [
                            {
                                "match": {
                                    "row.target.committee.cmte_id": "C00401224" # actblue
                                }
                            },
                            {
                                "match": {
                                    "row.target.committee.cmte_id": "C00694323" # winred
                                }
                            }
                        ]
                    }
                }
            }
        }
    }
    # add filters for type of contributor
    if template == "VqHR":
        q["query"]["bool"]["must"].append({
            "match": {
                "row.source.classification": "committee"
            }
        })
    elif template == "dFMy":
        q["query"]["bool"]["must"].append({
            "match": {
                "row.source.classification": "individual"
            }
        })
    if len(include_terms) > 0 or len(include_ids) > 0:
        # List A
        subquery = {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
        if len(include_terms) > 0:
            if include_terms[0] is not None:
                for term in include_terms[0]:
                    if template in ["ReqQ", "IQL2"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.source.committee.cmte_nm": term
                            }
                        })
                    elif template in ["NcFz"]:
                        subquery["bool"]["should"].append({
                            "match_phrase": {
                                "processed.source.donor.name": {
                                    "query": term,
                                    "slop": 2
                                }
                            }
                        })
                    elif template in ["m4YC", "Bs5W"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.source.donor.employer": term
                            }
                        })
                    elif template in ["7v4P", "6peF", "F2mS", "T5xv"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.source.donor.occupation": term
                            }
                        })
                    elif template in ["VqHR", "dFMy"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.target.committee.cmte_nm": term
                            }
                        })
        if len(include_ids) > 0:
            if include_ids[0] is not None:
                for id in include_ids[0]:
                    if template in ["ReqQ", "IQL2"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.source.committee.cmte_id": id
                            }
                        })
                    elif template in ["VqHR", "dFMy"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.target.committee.cmte_id": id
                            }
                        })
        q["query"]["bool"]["must"].append(subquery)
        # List B
        subquery = {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
        if len(include_terms) > 1:
            if include_terms[1] is not None:
                for term in include_terms[1]:
                    if template in ["T5xv", "F2mS"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.source.donor.employer": term
                            }
                        })
                    elif template in ["IQL2", "Bs5W", "6peF"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.target.committee.cmte_nm": term
                            }
                        })
        if len(include_ids) > 1:
            if include_ids[1] is not None:
                for id in include_ids[1]:
                    if template in ["ReqQ"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.source.committee.cmte_id": id
                            }
                        })
                    elif template in ["IQL2", "Bs5W", "6peF"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.target.committee.cmte_id": id
                            }
                        })
        q["query"]["bool"]["must"].append(subquery)
        # List C
        subquery = {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
        if len(include_terms) > 2:
            if include_terms[2] is not None:
                for term in include_terms[2]:
                    if template in ["F2mS"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.target.committee.cmte_nm": term
                            }
                        })
        if len(include_ids) > 2:
            if include_ids[2] is not None:
                for id in include_ids[2]:
                    if template in ["F2mS"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.target.committee.cmte_id": id
                            }
                        })
        q["query"]["bool"]["must"].append(subquery)
    if len(exclude_terms) > 0 or len(exclude_ids) > 0:
        # List A
        subquery = {
            "bool": {
                "must_not": []
            }
        }
        if len(exclude_terms) > 0:
            if exclude_terms[0] is not None:
                for term in exclude_terms[0]:
                    if template in ["ReqQ", "IQL2"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.source.committee.cmte_nm": term
                            }
                        })
                    elif template in ["NcFz"]:
                        subquery["bool"]["must_not"].append({
                            "match_phrase": {
                                "processed.source.donor.name": {
                                    "query": term,
                                    "slop": 2
                                }
                            }
                        })
                    elif template in ["m4YC", "Bs5W"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.source.donor.employer": term
                            }
                        })
                    elif template in ["7v4P", "6peF", "F2mS", "T5xv"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.source.donor.occupation": term
                            }
                        })
                    elif template in ["VqHR", "dFMy"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.target.committee.cmte_nm": term
                            }
                        })
        if len(exclude_ids) > 0:
            if exclude_ids[0] is not None:
                for id in exclude_ids[0]:
                    if template in ["ReqQ", "IQL2"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.source.committee.cmte_id": id
                            }
                        })
                    elif template in ["VqHR", "dFMy"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.target.committee.cmte_id": id
                            }
                        })
        q["query"]["bool"]["must"].append(subquery)
        # List B
        subquery = {
            "bool": {
                "must_not": []
            }
        }
        if len(exclude_terms) > 1:
            if exclude_terms[1] is not None:
                for term in exclude_terms[1]:
                    if template in ["T5xv", "F2mS"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.source.donor.employer": term
                            }
                        })
                    elif template in ["IQL2", "Bs5W", "6peF"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.target.committee.cmte_nm": term
                            }
                        })
        if len(exclude_ids) > 1:
            if exclude_ids[1] is not None:
                for id in exclude_ids[1]:
                    if template in ["ReqQ"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.source.committee.cmte_id": id
                            }
                        })
                    elif template in ["IQL2", "Bs5W", "6peF"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.target.committee.cmte_id": id
                            }
                        })
        q["query"]["bool"]["must"].append(subquery)
        # List C
        subquery = {
            "bool": {
                "must_not": []
            }
        }
        if len(exclude_terms) > 2:
            if exclude_terms[2] is not None:
                for term in exclude_terms[2]:
                    if template in ["F2mS"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.target.committee.cmte_nm": term
                            }
                        })
        if len(exclude_ids) > 2:
            if exclude_ids[2] is not None:
                for id in exclude_ids[2]:
                    if template in ["F2mS"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.target.committee.cmte_id": id
                            }
                        })
        q["query"]["bool"]["must"].append(subquery)
    if orderby == "date":
        q["sort"] = {
            "processed.date": {"order": orderdir},
        }
    elif orderby == "amount":
        q["sort"] = {
            "row.transaction_amt": {"order": orderdir},
        }
    if count is True:
        response = es.count(index="federal_fec_contributions", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    elif histogram is True:
        q["aggs"] = {
            "dates": {
                "date_histogram": {
                    "field": "processed.date",
                    "calendar_interval": "day",
                    "time_zone": "America/New_York"
                }
            }
        }
        response = es.search(index="federal_fec_contributions", body=q, filter_path=["aggregations"])
        try:
            return response["aggregations"]["dates"]["buckets"]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(
            index="federal_fec_contributions",
            body=q,
            filter_path=["hits.hits._source.processed", "hits.hits._source.row"]
        )
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                row = {
                    "recipient_cmte_id": hit["_source"]["row"]["target"]["committee"]["cmte_id"],
                    "recipient_cmte_nm": hit["_source"]["row"]["target"]["committee"]["cmte_nm"],
                    "date": hit["_source"]["processed"]["date"][:10],
                    "transaction_amt": hit["_source"]["row"]["transaction_amt"],
                    "transaction_tp": hit["_source"]["row"]["transaction_tp"],
                    "reported_by": "contributor" if hit["_source"]["row"]["transaction_tp"].startswith("2") or hit["_source"]["row"]["transaction_tp"].startswith("4") and hit["_source"]["row"]["transaction_tp"] != "24I" and hit["_source"]["row"]["transaction_tp"] != "24T" else "recipient",
                    "url": "https://docquery.fec.gov/cgi-bin/fecimg/?" + hit["_source"]["row"]["image_num"]
                }
                if "committee" in hit["_source"]["row"]["source"]:
                    row["contributor_cmte_id"] = hit["_source"]["row"]["source"]["committee"]["cmte_id"]
                    row["contributor_cmte_nm"] = hit["_source"]["row"]["source"]["committee"]["cmte_nm"]
                elif "donor" in hit["_source"]["row"]["source"]:
                    row["donor_name"] = hit["_source"]["processed"]["source"]["donor"]["name"]
                    row["donor_zip_code"] = hit["_source"]["row"]["source"]["donor"]["zip_code"]
                    row["donor_employer"] = hit["_source"]["row"]["source"]["donor"]["employer"]
                    row["donor_occupation"] = hit["_source"]["row"]["source"]["donor"]["occupation"]
                elements.append(row)
            return elements
        except:
            return []

def data_calculate_recipe_lobbying_disclosures(template, es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram, concise=False):
    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "processed.date_submitted": {
                                "gte": mindate,
                                "lte": maxdate
                            }
                        }
                    }
                ]
            }
        }
    }
    if count is False and concise is True:
        q["collapse"] = {
            "field": "processed.registrant.senate_id.keyword"
        }
    if len(include_terms) > 0 or len(include_ids) > 0:
        subquery = {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
        if include_terms[0] is not None:
            for term in include_terms[0]:
                if template == "kMER":
                    subquery["bool"]["should"].append({
                        "match": {
                            "processed.client.name": term
                        }
                    })
                elif template == "wLvp":
                    subquery["bool"]["should"].append({
                        "match": {
                            "processed.registrant.name": term
                        }
                    })
                elif template == "MJdb":
                    subquery["bool"]["should"].append({
                        "match": {
                            "processed.activities": term
                        }
                    })
                    subquery["bool"]["should"].append({
                        "match": {
                            "processed.issues.display": term
                        }
                    })
        if include_ids[0] is not None:
            for id in include_ids[0]:
                if template == "MJdb":
                    subquery["bool"]["should"].append({
                        "match": {
                            "processed.issues.code": id
                        }
                    })
        q["query"]["bool"]["must"].append(subquery)
    if len(exclude_terms) > 0 or len(exclude_ids) > 0:
        subquery = {
            "bool": {
                "must_not": []
            }
        }
        if exclude_terms[0] is not None:
            for term in exclude_terms[0]:
                if template == "kMER":
                    subquery["bool"]["must_not"].append({
                        "match": {
                            "processed.client.name": term
                        }
                    })
                elif template == "wLvp":
                    subquery["bool"]["must_not"].append({
                        "match": {
                            "processed.registrant.name": term
                        }
                    })
                elif template == "MJdb":
                    subquery["bool"]["must_not"].append({
                        "match": {
                            "processed.activities": term
                        }
                    })
                    subquery["bool"]["must_not"].append({
                        "match": {
                            "processed.issues.display": term
                        }
                    })
        if exclude_ids[0] is not None:
            for id in exclude_ids[0]:
                if template == "MJdb":
                    subquery["bool"]["must_not"].append({
                        "match": {
                            "processed.issues.code": id
                        }
                    })
        q["query"]["bool"]["must"].append(subquery)
    if orderby == "date":
        q["sort"] = {
            "processed.date_submitted": {"order": orderdir},
        }
    if count is True:
        response = es.count(index="federal_senate_lobbying_disclosures,federal_house_lobbying_disclosures", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    elif histogram is True:
        q["aggs"] = {
            "dates": {
                "date_histogram": {
                    "field": "processed.date_submitted",
                    "calendar_interval": "day",
                    "time_zone": "America/New_York"
                }
            }
        }
        response = es.search(index="federal_senate_lobbying_disclosures,federal_house_lobbying_disclosures", body=q, filter_path=["aggregations"])
        try:
            return response["aggregations"]["dates"]["buckets"]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(
            index="federal_senate_lobbying_disclosures,federal_house_lobbying_disclosures",
            body=q,
            filter_path=["hits.hits._source.processed"]
        )
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                if concise is True:
                    elements.append({
                        "registrant_senate_id": hit["_source"]["processed"]["registrant"].get("senate_id"),
                    })
                else:
                    elements.append({
                        "date_submitted": hit["_source"]["processed"].get("date_submitted")[:10],
                        "filing_year": hit["_source"]["processed"].get("filing_year"),
                        "filing_type": hit["_source"]["processed"].get("filing_type"),
                        "client_name": hit["_source"]["processed"]["client"].get("name"),
                        "registrant_name": hit["_source"]["processed"]["registrant"].get("name"),
                        "registrant_house_id": hit["_source"]["processed"]["registrant"].get("house_id"),
                        "registrant_senate_id": hit["_source"]["processed"]["registrant"].get("senate_id"),
                        "lobbyists": ", ".join([i["name"] for i in hit["_source"]["processed"].get("lobbyists", [])]),
                        "lobbying_activities": "; ".join(hit["_source"]["processed"].get("activities", [])),
                        "lobbying_issues": ", ".join([i["code"] for i in hit["_source"]["processed"].get("issues", [])]),
                        "lobbying_coverage": "; ".join(hit["_source"]["processed"].get("coverage", [])),
                        "url": hit["_source"]["processed"].get("url"),
                    })
            return elements
        except:
            return []

def data_calculate_recipe_lobbying_contributions(template, es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram):
    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "processed.date_submitted": {
                                "gte": mindate,
                                "lte": maxdate
                            }
                        }
                    }, {
                        "match": {
                            "processed.no_contributions": False
                        }
                    }
                ]
            }
        }
    }
    if len(include_terms) > 0 or len(include_ids) > 0:
        subquery = {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
        if include_ids[0] is not None:
            for id in include_ids[0]:
                if template in ["PjyR", "WGb3", "MK93"]:
                    subquery["bool"]["should"].append({
                        "match": {
                            "processed.registrant.senate_id": id
                        }
                    })
        q["query"]["bool"]["must"].append(subquery)
    if len(exclude_terms) > 0 or len(exclude_ids) > 0:
        subquery = {
            "bool": {
                "must_not": []
            }
        }
        if exclude_ids[0] is not None:
            for id in exclude_ids[0]:
                if template in ["PjyR", "WGb3", "MK93"]:
                    subquery["bool"]["must_not"].append({
                        "match": {
                            "processed.registrant.senate_id": id
                        }
                    })
        q["query"]["bool"]["must"].append(subquery)
    if orderby == "date":
        q["sort"] = {
            "processed.date_submitted": {"order": orderdir},
        }
    if count is True:
        response = es.count(index="federal_senate_lobbying_contributions,federal_house_lobbying_contributions", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    elif histogram is True:
        q["aggs"] = {
            "dates": {
                "date_histogram": {
                    "field": "processed.date_submitted",
                    "calendar_interval": "day",
                    "time_zone": "America/New_York"
                }
            }
        }
        response = es.search(index="federal_senate_lobbying_contributions,federal_house_lobbying_contributions", body=q, filter_path=["aggregations"])
        try:
            return response["aggregations"]["dates"]["buckets"]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(
            index="federal_senate_lobbying_contributions,federal_house_lobbying_contributions",
            body=q,
            filter_path=["hits.hits._source.processed"]
        )
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                contributions = hit["_source"]["processed"].get("contributions")
                if template in ["V5Gh", "3Nrt", "Q23x"]:
                    contributions = [c for c in contributions if c["contribution_type"] == "Honorary Expenses"]
                for contribution in contributions:
                    contribution["date_contribution"] = contribution.pop("date")[:10]
                    contribution["date_submitted"] = hit["_source"]["processed"].get("date_submitted")[:10]
                    contribution["filing_year"] = hit["_source"]["processed"].get("filing_year")
                    contribution["filing_type"] = hit["_source"]["processed"].get("filing_type")
                    contribution["registrant_name"] = hit["_source"]["processed"]["registrant"].get("name")
                    contribution["registrant_house_id"] = hit["_source"]["processed"]["registrant"].get("house_id")
                    contribution["registrant_senate_id"] = hit["_source"]["processed"]["registrant"].get("senate_id")
                    contribution["url"] = hit["_source"]["processed"].get("url")
                    elements.append(contribution)
            return elements
        except:
            return []

def data_calculate_recipe_990(template, es, include_terms, include_ids, exclude_terms, exclude_ids, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram):
    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "row.sub_date": {
                                "gte": mindate,
                                "lte": maxdate
                            }
                        }
                    }
                ]
            }
        }
    }
    if len(include_terms) > 0 or len(include_ids) > 0:
        # List A
        subquery = {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
        if len(include_terms) > 0:
            if include_terms[0] is not None:
                for term in include_terms[0]:
                    if template in ["GCv2", "P34n", "K23r"]:
                        subquery["bool"]["should"].append({
                            "multi_match": {
                                "query": term,
                                "slop": 5
                            }
                        })
        if len(include_ids) > 0:
            if include_ids[0] is not None:
                if template in ["D3WE", "BuW8"]:
                    committees = data_preview_organization_committee(es, None, include_ids[0], None, None, 0, 10000, False)
                    for committee in committees:
                        name = clean_committees_names(committee["cmte_nm"])
                        if template in ["GCv2"]:
                            subquery["bool"]["should"].append({
                                "multi_match": {
                                    "query": term,
                                    "slop": 5
                                }
                            })
        q["query"]["bool"]["must"].append(subquery)
    if len(exclude_terms) > 0 or len(exclude_ids) > 0:
        # List A
        subquery = {
            "bool": {
                "must_not": []
            }
        }
        if len(exclude_terms) > 0:
            if exclude_terms[0] is not None:
                for term in exclude_terms[0]:
                    if template in ["GCv2", "P34n", "K23r"]:
                        subquery["bool"]["must_not"].append({
                            "multi_match": {
                                "query": term,
                                "slop": 5
                            }
                        })
        if len(exclude_ids) > 0:
            if exclude_ids[0] is not None:
                if template in ["D3WE", "BuW8"]:
                    committees = data_preview_organization_committee(es, None, exclude_ids[0], None, None, 0, 10000, False)
                    for committee in committees:
                        name = clean_committees_names(committee["cmte_nm"])
                        if template in ["GCv2"]:
                            subquery["bool"]["must_not"].append({
                                "multi_match": {
                                    "query": term,
                                    "slop": 5
                                }
                            })
        q["query"]["bool"]["must"].append(subquery)
    if orderby == "date":
        q["sort"] = {
            "row.sub_date": {"order": orderdir},
        }
    if count is True:
        response = es.count(index="federal_irs_990,federal_irs_990ez,federal_irs_990pf", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    elif histogram is True:
        q["aggs"] = {
            "dates": {
                "date_histogram": {
                    "field": "row.sub_date",
                    "calendar_interval": "day",
                    "time_zone": "America/New_York"
                }
            }
        }
        response = es.search(index="federal_irs_990,federal_irs_990ez,federal_irs_990pf", body=q, filter_path=["aggregations"])
        try:
            return response["aggregations"]["dates"]["buckets"]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(
            index="federal_irs_990,federal_irs_990ez,federal_irs_990pf",
            body=q,
            filter_path=["hits.hits._source.row"]
        )
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                row = {
                    "submission_date": hit["_source"]["row"]["sub_date"][:10],
                    "ein": hit["_source"]["row"]["ein"],
                    "taxpayer_name": hit["_source"]["row"]["taxpayer_name"],
                    "return_type": hit["_source"]["row"]["return_type"],
                    "tax_period": hit["_source"]["row"]["tax_period"],
                    "xml_url": "https://s3.amazonaws.com/irs-form-990/" + hit["_source"]["row"]["object_id"] + "_public.xml",
                    "pdf_url": "https://apps.irs.gov/pub/epostcard/cor/" + hit["_source"]["row"]["ein"] + "_" + hit["_source"]["row"]["tax_period"] + "_" + hit["_source"]["row"]["return_type"] + "_" + hit["_source"]["row"]["sub_date"][:10].replace("-", "") + hit["_source"]["row"]["return_id"] + ".pdf"
                }
                elements.append(row)
            return elements
        except:
            return []
