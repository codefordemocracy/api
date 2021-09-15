from fastapi import APIRouter, Depends, Query

from .dependencies.authentication import get_auth
from .dependencies.connections import driver, es
from .dependencies.defaults import get_years
from .dependencies import helpers
from .dependencies.cypher import search as cypher
from .dependencies.query import browse as query

import datetime
import pytz

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/documents/browse",
    tags=["browse"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# explore elasticsearch
#########################################################

@router.get("/news/articles/source/questionable/", summary="Explore Articles from Factually Questionable Sources")
def documents_browse_news_articles_source_questionable(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=None, factually_questionable_flag=1, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/news/articles/source/conspiracy/", summary="Explore Articles from Conspiracy Sources")
def documents_browse_news_articles_source_conspiracy(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=1, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/news/articles/source/hate/", summary="Explore Articles from Hate Group Sources")
def documents_browse_news_articles_source_hate(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=1, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/news/articles/source/propaganda/", summary="Explore Articles from Propaganda Sources")
def documents_browse_news_articles_source_propaganda(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=1, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/news/articles/source/satire/", summary="Explore Articles from Satire Sources")
def documents_browse_news_articles_source_satire(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=1, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/news/articles/source/liberal/", summary="Explore Articles from Liberal Sources")
def documents_browse_news_articles_source_liberal(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=[-2], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/news/articles/source/left/", summary="Explore Articles from Left Leaning Sources")
def documents_browse_news_articles_source_left(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=[-1], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/news/articles/source/moderate/", summary="Explore Articles from Moderate Sources")
def documents_browse_news_articles_source_moderate(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=[0], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/news/articles/source/right/", summary="Explore Articles from Right Leaning Sources")
def documents_browse_news_articles_source_right(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=[1], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/news/articles/source/conservative/", summary="Explore Articles from Conservative Sources")
def documents_browse_news_articles_source_conservative(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=[2], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/twitter/tweets/candidate/dem/", summary="Explore Tweets from Democratic Candidates")
def documents_browse_twitter_tweets_candidate_dem(text: str = None, cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        tweeters = neo4j.read_transaction(cypher.graph_search_tweeters, name=None, username=None, candidate=True, cand_pty_affiliation="DEM", cand_election_yr=cand_election_yr, context=False, skip=0, limit=50000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    user_ids = [i["user_id"] for i in tweeters]
    return query.documents_browse_twitter_tweets_user(es, user_ids=user_ids, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/twitter/tweets/candidate/rep/", summary="Explore Tweets from Republican Candidates")
def documents_browse_twitter_tweets_candidate_rep(text: str = None, cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        tweeters = neo4j.read_transaction(cypher.graph_search_tweeters, name=None, username=None, candidate=True, cand_pty_affiliation="REP", cand_election_yr=cand_election_yr, context=False, skip=0, limit=50000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    user_ids = [i["user_id"] for i in tweeters]
    return query.documents_browse_twitter_tweets_user(es, user_ids=user_ids, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@router.get("/facebook/ads/", summary="Explore Ads")
def documents_browse_facebook_ads(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    return query.documents_browse_facebook_ads(es, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)
