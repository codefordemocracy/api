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

def data_calculate_recipe_lobbying(template, es, terms, skip, limit, mindate, maxdate, orderby, orderdir, count):
    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {
                                    "range": { # senate
                                        "filing.dt_posted": {
                                            "gte": mindate,
                                            "lte": maxdate
                                        }
                                    },
                                    # "range": { # senate
                                    #     "filing.filing_year": {
                                    #         "gte": mindate.year,
                                    #         "lte": maxdate.year
                                    #     }
                                    # },
                                    "range": { # house
                                        "filing.signedDate": {
                                            "gte": mindate,
                                            "lte": maxdate
                                        }
                                    },
                                    # "range": { # house
                                    #     "filing.reportYear": {
                                    #         "gte": mindate.year,
                                    #         "lte": maxdate.year
                                    #     }
                                    # }
                                }
                            ],
                            "minimum_should_match": 1
                        }
                    }
                ]
            }
        }
    }
    if terms[0] is not None:
        subquery = {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
        for term in terms[0]:
            if template == "kMER":
                subquery["bool"]["should"].append({
                    "match": {
                        "filing.client.name": term # senate
                    }
                })
                subquery["bool"]["should"].append({
                    "match": {
                        "filing.clientName": term # house
                    }
                })
            elif template == "wLvp":
                subquery["bool"]["should"].append({
                    "match": {
                        "filing.registrant.name": term # senate
                    }
                })
                subquery["bool"]["should"].append({
                    "match": {
                        "filing.filing.organizationName": term # house
                    }
                })
            elif template == "MJdb":
                subquery["bool"]["should"].append({
                    "match": {
                        "filing.lobbying_activities": term # senate
                    }
                })
                subquery["bool"]["should"].append({
                    "match": {
                        "filing.alis.ali_info.specific_issues.description": term # house
                    }
                })
        q["query"]["bool"]["must"].append(subquery)
    # if orderby == "date":
    #     q["sort"] = {
    #         "filing.dt_posted": {"order": orderdir}, # senate
    #         "filing.signedDate": {"order": orderdir} # house
    #     }
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
            filter_path=[
                "hits.hits._index",
                "hits.hits._id",
                "hits.hits._source.filing.income",
                # senate
                "hits.hits._source.filing.dt_posted",
                "hits.hits._source.filing.filing_year",
                "hits.hits._source.filing.filing_type",
                "hits.hits._source.filing.client.name",
                "hits.hits._source.filing.registrant.name",
                "hits.hits._source.filing.lobbying_activities",
                # house
                "hits.hits._source.filing.signedDate",
                "hits.hits._source.filing.reportYear",
                "hits.hits._source.filing.reportType",
                "hits.hits._source.filing.clientName",
                "hits.hits._source.filing.organizationName",
                "hits.hits._source.filing.alis.ali_info.issueAreaCode",
                "hits.hits._source.filing.alis.ali_info.specific_issues.description",
            ]
        )
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                if hit["_index"] == "federal_senate_lobbying_disclosures":
                    elements.append({
                        "date_submitted": str(hit["_source"]["filing"].get("dt_posted"))[:10],
                        "filing_year": hit["_source"]["filing"].get("filing_year"),
                        "filing_type": hit["_source"]["filing"].get("filing_type"),
                        "client_name": hit["_source"]["filing"].get("client", {}).get("name"),
                        "registrant_name": hit["_source"]["filing"].get("registrant", {}).get("name"),
                        "lobbying_activities": hit["_source"]["filing"].get("lobbying_activities", [{}])[0].get("description"),
                        "income": hit["_source"]["filing"].get("income"),
                        "url": "https://lda.senate.gov/filings/public/filing/" + hit["_id"] + "/print/"
                    })
                elif hit["_index"] == "federal_house_lobbying_disclosures":
                    elements.append({
                        "date_submitted": str(hit["_source"]["filing"].get("signedDate"))[:10],
                        "filing_year": hit["_source"]["filing"].get("reportYear"),
                        "filing_type": hit["_source"]["filing"].get("reportType"),
                        "client_name": hit["_source"]["filing"].get("clientName"),
                        "registrant_name": hit["_source"]["filing"].get("organizationName"),
                        "lobbying_activities": hit["_source"]["filing"].get("alis", {}).get("ali_info", {})[0].get("specific_issues", {}).get("description") if isinstance(hit["_source"]["filing"].get("alis", {}).get("ali_info", {}), list) else hit["_source"]["filing"].get("alis", {}).get("ali_info", {}).get("specific_issues", {}).get("description"),
                        "income": hit["_source"]["filing"].get("income"),
                        "url": "https://disclosurespreview.house.gov/ld/ldxmlrelease/" + hit["_source"]["filing"].get("reportYear") + "/" + hit["_source"]["filing"].get("reportType") + "/" + hit["_id"] + ".xml"
                    })
            return elements
        except:
            return []
