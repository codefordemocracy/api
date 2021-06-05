from collections import defaultdict

def documents_browse_news_articles_source(es, domains, text, histogram, skip, limit, mindate, maxdate):
    q = {
        "from": skip,
        "size": limit,
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
    if text is not None:
        q["query"]["bool"]["must"].append({
            "match": {
                "extracted.text": text
            }
        })
    if len(domains) > 0:
        q["query"]["bool"]["must"].append({
            "terms": {
                "source.url": domains
            }
        })
    if histogram is True:
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
        response = es.search(index="news_articles", body=q, filter_path=["hits.hits._id", "hits.hits._source.extracted.title", "hits.hits._source.extracted.text", "hits.hits._source.extracted.date", "hits.hits._source.url"])
        try:
            return response["hits"]["hits"]
        except:
            return []

def documents_browse_twitter_tweets_user(es, user_ids, text, histogram, skip, limit, mindate, maxdate):
    q = {
        "from": skip,
        "size": limit,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "obj.created_at": {
                                "gte": mindate,
                                "lte": maxdate
                            }
                        }
                    }
                ]
            }
        }
    }
    if text is not None:
        q["query"]["bool"]["must"].append({
            "match": {
                "obj.text": text
            }
        })
    if len(user_ids) > 0:
        q["query"]["bool"]["must"].append({
            "terms": {
                "user_id": user_ids
            }
        })
    if histogram is True:
        q["aggs"] = {
            "dates": {
                "date_histogram": {
                    "field": "obj.created_at",
                    "calendar_interval": "day",
                    "time_zone": "America/New_York"
                }
            }
        }
        response = es.search(index="twitter_tweets,tweets_old", body=q, filter_path=["aggregations"])
        try:
            return response["aggregations"]["dates"]["buckets"]
        except:
            return []
    else:
        response = es.search(index="twitter_tweets,tweets_old", body=q, filter_path=["hits.hits._source.user_id", "hits.hits._source.user_screen_name", "hits.hits._source.obj.id", "hits.hits._source.obj.created_at", "hits.hits._source.obj.entities.hashtags"])
        try:
            return response["hits"]["hits"]
        except:
            return []

def documents_browse_facebook_ads(es, text, histogram, skip, limit, mindate, maxdate):
    q = {
        "from": skip,
        "size": limit,
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {
                                    "range": {
                                        "obj.ad_creation_time": {
                                            "gte": mindate,
                                            "lte": maxdate
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "obj.created": {
                                            "gte": mindate,
                                            "lte": maxdate
                                        }
                                    }
                                }
                            ],
                            "minimum_should_match": 1
                        }
                    }
                ]
            }
        }
    }
    if text is not None:
        q["query"]["bool"]["must"].append({
            "bool": {
                "should": [
                    {
                        "match": {
                            "obj.ad_creative_body": text
                        }
                    },
                    {
                        "match": {
                            "obj.ad_creative_link_description": text
                        }
                    },
                    {
                        "match": {
                            "obj.title": text
                        }
                    },
                    {
                        "match": {
                            "obj.selftext": text
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        })
    if histogram is True:
        q["aggs"] = {
            "facebook": {
                "date_histogram": {
                    "field": "obj.ad_creation_time",
                    "calendar_interval": "day",
                    "time_zone": "America/New_York"
                }
            }
        }
        response = es.search(index="facebook_ads", body=q, filter_path=["aggregations"])
        try:
            buckets = defaultdict(lambda : {"key_as_string": "", "key": 0, "doc_count": 0})
            for d in response["aggregations"]["facebook"]["buckets"]:
                buckets[d["key_as_string"]]["key_as_string"] = d["key_as_string"]
                buckets[d["key_as_string"]]["key"] = d["key"]
                buckets[d["key_as_string"]]["doc_count"] += d["doc_count"]
            return [v for k,v in buckets.items()]
        except:
            return []
    else:
        response = es.search(index="facebook_ads", body=q, filter_path=["hits.hits._source.type", "hits.hits._source.obj.id", "hits.hits._source.obj.page_id", "hits.hits._source.obj.page_name", "hits.hits._source.obj.funding_entity", "hits.hits._source.obj.permalink", "hits.hits._source.obj.ad_creation_time", "hits.hits._source.obj.created"])
        try:
            return response["hits"]["hits"]
        except:
            return []

def data_preview_committee(es, terms, ids, skip, limit, count):
    q = {
        "query": {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
    }
    if terms is not None:
        for term in terms:
            q["query"]["bool"]["should"].append({
                "match": {
                    "obj.cmte_nm": term
                }
            })
    if ids is not None:
        for id in ids:
            q["query"]["bool"]["should"].append({
                "match": {
                    "obj.cmte_id": id
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
        response = es.search(index="federal_fec_committees", body=q, filter_path=["hits.hits._source.obj.cmte_id", "hits.hits._source.obj.cmte_nm"])
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                elements.append({
                    "cmte_id": hit["_source"]["obj"]["cmte_id"],
                    "cmte_nm": hit["_source"]["obj"]["cmte_nm"]
                })
            return elements
        except:
            return []

def data_preview_employer(es, terms, ids, skip, limit, count):
    q = {
        "query": {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        },
        "collapse": {
            "field": "processed.source.donor.employer.keyword"
        }
    }
    if terms is not None:
        for term in terms:
            q["query"]["bool"]["should"].append({
                "match": {
                    "processed.source.donor.employer": term
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
        response = es.search(index="federal_fec_contributions", body=q, filter_path=["hits.hits._source.processed.source.donor.employer"])
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                elements.append({
                    "name": hit["_source"]["processed"]["source"]["donor"]["employer"]
                })
            return elements
        except:
            return []

def data_preview_job(es, terms, ids, skip, limit, count):
    q = {
        "query": {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        },
        "collapse": {
            "field": "processed.source.donor.occupation.keyword"
        }
    }
    if terms is not None:
        for term in terms:
            q["query"]["bool"]["should"].append({
                "match": {
                    "processed.source.donor.occupation": term
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
        response = es.search(index="federal_fec_contributions", body=q, filter_path=["hits.hits._source.processed.source.donor.occupation"])
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                elements.append({
                    "name": hit["_source"]["processed"]["source"]["donor"]["occupation"]
                })
            return elements
        except:
            return []

def data_calculate_recipe_contribution(template, es, terms, ids, skip, limit, mindate, maxdate, orderby, orderdir, count):
    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "processed.transaction_dt": {
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
                                    "processed.target.committee.cmte_id": "C00401224" # actblue
                                }
                            },
                            {
                                "match": {
                                    "processed.target.committee.cmte_id": "C00694323" # winred
                                }
                            }
                        ]
                    }
                }
            }
        }
    }
    if len(terms) > 0 or len(ids) > 0:
        # Contributions
        if template in ["P3JF"]:
            q["query"]["bool"]["must"].append({
                "range": {
                    "processed.transaction_amt": {
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
        if len(terms) > 0:
            if terms[0] is not None:
                for term in terms[0]:
                    if template in ["ReqQ", "IQL2", "P3JF"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "processed.source.committee.cmte_nm": term
                            }
                        })
                    elif template in ["m4YC", "Bs5W"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "processed.source.donor.employer": term
                            }
                        })
                    elif template in ["7v4P", "T5xv", "6peF", "F2mS"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "processed.source.donor.occupation": term
                            }
                        })
        if len(ids) > 0:
            if ids[0] is not None:
                for id in ids[0]:
                    if template in ["ReqQ", "IQL2", "P3JF"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "processed.source.committee.cmte_id": id
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
        if len(terms) > 1:
            if terms[1] is not None:
                for term in terms[1]:
                    if template in ["T5xv", "F2mS"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "processed.source.donor.employer": term
                            }
                        })
                    elif template in ["Bs5W", "6peF", "IQL2"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "processed.target.committee.cmte_nm": term
                            }
                        })
        if len(ids) > 1:
            if ids[1] is not None:
                for id in ids[1]:
                    if template in ["ReqQ"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "processed.source.committee.cmte_id": id
                            }
                        })
                    elif template in ["Bs5W", "6peF", "IQL2"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "processed.target.committee.cmte_id": id
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
        if len(terms) > 2:
            if terms[2] is not None:
                for term in terms[2]:
                    if template in ["F2mS"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "processed.target.committee.cmte_nm": term
                            }
                        })
        if len(ids) > 2:
            if ids[2] is not None:
                for id in ids[2]:
                    if template in ["F2mS"]:
                        subquery["bool"]["should"].append({
                            "match": {
                                "processed.target.committee.cmte_id": id
                            }
                        })
        q["query"]["bool"]["must"].append(subquery)
    if orderby == "date":
        q["sort"] = {
            "processed.transaction_dt": {"order": orderdir},
        }
    elif orderby == "date":
        q["sort"] = {
            "processed.transaction_amt": {"order": orderdir},
        }
    if count is True:
        response = es.count(index="federal_fec_contributions", body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(
            index="federal_fec_contributions",
            body=q,
            filter_path=["hits.hits._source.processed"]
        )
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                if template in ["ReqQ", "IQL2"]:
                    elements.append({
                        "contributor_cmte_id": hit["_source"]["processed"]["source"]["committee"]["cmte_id"],
                        "contributor_cmte_nm": hit["_source"]["processed"]["source"]["committee"]["cmte_nm"],
                        "recipient_cmte_id": hit["_source"]["processed"]["target"]["committee"]["cmte_id"],
                        "recipient_cmte_nm": hit["_source"]["processed"]["target"]["committee"]["cmte_nm"],
                        "date": hit["_source"]["processed"]["transaction_dt"][:10],
                        "transaction_amt": hit["_source"]["processed"]["transaction_amt"]
                    })
                elif template in ["m4YC", "7v4P", "T5xv", "Bs5W", "6peF", "F2mS"]:
                    elements.append({
                        "donor_name": hit["_source"]["processed"]["source"]["donor"]["name"],
                        "donor_zip_code": hit["_source"]["processed"]["source"]["donor"]["zip_code"],
                        "donor_employer": hit["_source"]["processed"]["source"]["donor"]["employer"],
                        "donor_occupation": hit["_source"]["processed"]["source"]["donor"]["occupation"],
                        "recipient_cmte_id": hit["_source"]["processed"]["target"]["committee"]["cmte_id"],
                        "recipient_cmte_nm": hit["_source"]["processed"]["target"]["committee"]["cmte_nm"],
                        "date": hit["_source"]["processed"]["transaction_dt"][:10],
                        "transaction_amt": hit["_source"]["processed"]["transaction_amt"]
                    })
                elif template in ["P3JF"]:
                    elements.append({
                        "contributor_cmte_id": hit["_source"]["processed"]["source"]["committee"]["cmte_id"],
                        "contributor_cmte_nm": hit["_source"]["processed"]["source"]["committee"]["cmte_nm"],
                        "refunding_cmte_id": hit["_source"]["processed"]["target"]["committee"]["cmte_id"],
                        "refunding_cmte_nm": hit["_source"]["processed"]["target"]["committee"]["cmte_nm"],
                        "date": hit["_source"]["processed"]["transaction_dt"][:10],
                        "transaction_amt": hit["_source"]["processed"]["transaction_amt"]
                    })
            return elements
        except:
            return []

def data_calculate_recipe_lobbying(template, es, terms, ids, skip, limit, mindate, maxdate, orderby, orderdir, count):
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
    if len(terms) > 0 or len(ids) > 0:
        subquery = {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
        if terms[0] is not None:
            for term in terms[0]:
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
        if ids[0] is not None:
            for id in ids[0]:
                if template == "MJdb":
                    subquery["bool"]["should"].append({
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
                elements.append({
                    "date_submitted": hit["_source"]["processed"].get("date_submitted")[:10],
                    "filing_year": hit["_source"]["processed"].get("filing_year"),
                    "filing_type": hit["_source"]["processed"].get("filing_type"),
                    "client_name": hit["_source"]["processed"]["client"].get("name"),
                    "registrant_name": hit["_source"]["processed"]["registrant"].get("name"),
                    "lobbyists": ", ".join([i["name"] for i in hit["_source"]["processed"].get("lobbyists", [])]),
                    "lobbying_activities": "; ".join(hit["_source"]["processed"].get("activities", [])),
                    "lobbying_issues": ", ".join([i["code"] for i in hit["_source"]["processed"].get("issues", [])]),
                    "lobbying_coverage": "; ".join(hit["_source"]["processed"].get("coverage", [])),
                    "url": hit["_source"]["processed"].get("url"),
                })
            return elements
        except:
            return []
