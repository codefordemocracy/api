from ..helpers import clean_committees_names
from .preview import data_preview_organization_committee

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
                    elif template in ["BuW8", "N7Jk", "P2HG"]:
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
                    elif template in ["BuW8", "N7Jk", "P2HG"]:
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
    if len(include_terms) > 0 or len(include_ids) > 0:
        # Contributions
        if template in ["P3JF"]:
            q["query"]["bool"]["must"].append({
                "range": {
                    "row.transaction_amt": {
                        "lt": 0
                    }
                }
            })
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
                    if template in ["ReqQ", "IQL2", "P3JF"]:
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
                    elif template in ["7v4P", "T5xv", "6peF", "F2mS"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.source.donor.occupation": term
                            }
                        })
                    elif template in ["VqHR"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.target.committee.cmte_nm": term
                            }
                        })
        if len(include_ids) > 0:
            if include_ids[0] is not None:
                for id in include_ids[0]:
                    if template in ["ReqQ", "IQL2", "P3JF"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "row.source.committee.cmte_id": id
                            }
                        })
                    elif template in ["VqHR"]:
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
                    elif template in ["Bs5W", "6peF", "IQL2"]:
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
                    elif template in ["Bs5W", "6peF", "IQL2"]:
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
                    if template in ["ReqQ", "IQL2", "P3JF"]:
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
                    elif template in ["7v4P", "T5xv", "6peF", "F2mS"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.source.donor.occupation": term
                            }
                        })
                    elif template in ["VqHR"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.target.committee.cmte_nm": term
                            }
                        })
        if len(exclude_ids) > 0:
            if exclude_ids[0] is not None:
                for id in exclude_ids[0]:
                    if template in ["ReqQ", "IQL2", "P3JF"]:
                        subquery["bool"]["must_not"].append({
                            "match": {
                                "row.source.committee.cmte_id": id
                            }
                        })
                    elif template in ["VqHR"]:
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
                    elif template in ["Bs5W", "6peF", "IQL2"]:
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
                    elif template in ["Bs5W", "6peF", "IQL2"]:
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
                if template in ["VqHR", "ReqQ", "NcFz", "m4YC", "7v4P", "T5xv", "Bs5W", "6peF", "F2mS", "IQL2"]:
                    row = {
                        "recipient_cmte_id": hit["_source"]["row"]["target"]["committee"]["cmte_id"],
                        "recipient_cmte_nm": hit["_source"]["row"]["target"]["committee"]["cmte_nm"],
                        "date": hit["_source"]["processed"]["date"][:10],
                        "transaction_amt": hit["_source"]["row"]["transaction_amt"],
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
                elif template in ["P3JF"]:
                    row = {
                        "contributor_cmte_id": hit["_source"]["row"]["source"]["committee"]["cmte_id"],
                        "contributor_cmte_nm": hit["_source"]["row"]["source"]["committee"]["cmte_nm"],
                        "refunding_cmte_id": hit["_source"]["row"]["target"]["committee"]["cmte_id"],
                        "refunding_cmte_nm": hit["_source"]["row"]["target"]["committee"]["cmte_nm"],
                        "date": hit["_source"]["processed"]["date"][:10],
                        "transaction_amt": hit["_source"]["row"]["transaction_amt"],
                        "url": "https://docquery.fec.gov/cgi-bin/fecimg/?" + hit["_source"]["row"]["image_num"]
                    }
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
                if template in ["WGb3", "PjyR", "MK93"]:
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
                if template in ["WGb3", "PjyR", "MK93"]:
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
                if template in ["3Nrt", "V5Gh", "Q23x"]:
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
                    if template in ["K23r", "GCv2", "P34n"]:
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
                    if template in ["K23r", "GCv2", "P34n"]:
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
