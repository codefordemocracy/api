from .builder.functions import make_query, set_query_dates, set_query_clauses, add_must_clause, add_not_clause, add_filter_clause
from .builder.responses import get_response

def documents_browse_news_articles_source(es, domains, text, histogram, skip, limit, mindate, maxdate):
    q = make_query()
    q = set_query_dates(q, "extracted.date", mindate, maxdate)
    q["from"] = skip
    q["size"] = limit
    if text is not None:
        q = add_must_clause(q, {
            "match_phrase": {
                "extracted.text": {
                    "query": text,
                    "slop": 5
                }
            }
        })
    if len(domains) > 0:
        q = add_must_clause(q, {
            "terms": {
                "extracted.source.url": domains
            }
        })
    response = get_response(es, "news_articles", q, skip, limit, False, histogram,
        date_field="extracted.date", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._id", "hits.hits._source.extracted.title", "hits.hits._source.extracted.text", "hits.hits._source.extracted.date", "hits.hits._source.extracted.url"]
    )
    if histogram is not True:
        elements = []
        for hit in response:
            elements.append(hit["_source"])
        return elements
    return response

def documents_browse_twitter_tweets_user(es, user_ids, text, histogram, skip, limit, mindate, maxdate):
    q = make_query()
    q = set_query_dates(q, "obj.tweet.created_at", mindate, maxdate)
    q["from"] = skip
    q["size"] = limit
    if text is not None:
        q = add_must_clause(q, {
            "match_phrase": {
                "obj.tweet.text": {
                    "query": text,
                    "slop": 5
                }
            }
        })
    if len(user_ids) > 0:
        q = add_must_clause(q, {
            "terms": {
                "obj.author.id": user_ids
            }
        })
    response = get_response(es, "twitter_tweets_new", q, skip, limit, False, histogram,
        date_field="obj.tweet.created_at", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.obj.author.id", "hits.hits._source.obj.author.username", "hits.hits._source.obj.tweet.id", "hits.hits._source.obj.tweet.created_at", "hits.hits._source.obj.tweet.entities.hashtags"]
    )
    if histogram is not True:
        elements = []
        for hit in response:
            elements.append(hit["_source"])
        return elements
    return response

def documents_browse_facebook_ads(es, text, histogram, skip, limit, mindate, maxdate):
    q = make_query()
    q = set_query_dates(q, "obj.ad_creation_time", mindate, maxdate)
    q["from"] = skip
    q["size"] = limit
    if text is not None:
        q = add_must_clause(q, {
            "bool": {
                "should": [
                    {
                        "match_phrase": {
                            "obj.ad_creative_body": {
                                "query": text,
                                "slop": 5
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            "obj.ad_creative_link_description": {
                                "query": text,
                                "slop": 5
                            }
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        })
    response = get_response(es, "facebook_ads", q, skip, limit, False, histogram,
        date_field="obj.ad_creation_time", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.obj.id", "hits.hits._source.obj.page_id", "hits.hits._source.obj.page_name", "hits.hits._source.obj.funding_entity", "hits.hits._source.obj.ad_creation_time"]
    )
    if histogram is not True:
        elements = []
        for hit in response:
            elements.append(hit["_source"])
        return elements
    return response
