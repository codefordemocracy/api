from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from .dependencies.authentication import get_auth
from .dependencies.connections import driver, es
from .dependencies.cypher import search as cypher
from .dependencies.query import browse as query
from .dependencies.models import PaginationConfig, DatesConfig

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
# define models
#########################################################

class DocumentsBrowseBaseBody(BaseModel):
    text: str = Field(None)
    histogram: bool = Field(False)
    pagination: PaginationConfig = PaginationConfig()
    dates: DatesConfig = DatesConfig()

#########################################################
# explore elasticsearch
#########################################################

@router.post("/news/articles/source/questionable/", summary="Explore Articles from Factually Questionable Sources")
def documents_browse_news_articles_source_questionable(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources,
            domain=None, bias_score=None, factually_questionable_flag=1, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None,
            context=False,
            skip=0, limit=5000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es,
        domains=domains,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/news/articles/source/conspiracy/", summary="Explore Articles from Conspiracy Sources")
def documents_browse_news_articles_source_conspiracy(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources,
            domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=1, hate_group_flag=None, propaganda_flag=None, satire_flag=None,
            context=False,
            skip=0, limit=5000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es,
        domains=domains,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/news/articles/source/hate/", summary="Explore Articles from Hate Group Sources")
def documents_browse_news_articles_source_hate(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources,
            domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=1, propaganda_flag=None, satire_flag=None,
            context=False,
            skip=0, limit=5000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es,
        domains=domains,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/news/articles/source/propaganda/", summary="Explore Articles from Propaganda Sources")
def documents_browse_news_articles_source_propaganda(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources,
            domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=1, satire_flag=None,
            context=False,
            skip=0, limit=5000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es,
        domains=domains,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/news/articles/source/satire/", summary="Explore Articles from Satire Sources")
def documents_browse_news_articles_source_satire(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources,
            domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=1,
            context=False,
            skip=0, limit=5000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es,
        domains=domains,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/news/articles/source/liberal/", summary="Explore Articles from Liberal Sources")
def documents_browse_news_articles_source_liberal(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources,
            domain=None, bias_score=[-2], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None,
            context=False,
            skip=0, limit=5000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es,
        domains=domains,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/news/articles/source/left/", summary="Explore Articles from Left Leaning Sources")
def documents_browse_news_articles_source_left(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources,
            domain=None, bias_score=[-1], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None,
            context=False,
            skip=0, limit=5000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es,
        domains=domains,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/news/articles/source/moderate/", summary="Explore Articles from Moderate Sources")
def documents_browse_news_articles_source_moderate(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources,
            domain=None, bias_score=[0], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None,
            context=False,
            skip=0, limit=5000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es,
        domains=domains,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/news/articles/source/right/", summary="Explore Articles from Right Leaning Sources")
def documents_browse_news_articles_source_right(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources,
            domain=None, bias_score=[1], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None,
            context=False,
            skip=0, limit=5000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es,
        domains=domains,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/news/articles/source/conservative/", summary="Explore Articles from Conservative Sources")
def documents_browse_news_articles_source_conservative(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources,
            domain=None, bias_score=[2], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None,
            context=False,
            skip=0, limit=5000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es,
        domains=domains,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/twitter/tweets/candidate/dem/", summary="Explore Tweets from Democratic Candidates")
def documents_browse_twitter_tweets_candidate_dem(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        tweeters = neo4j.read_transaction(cypher.graph_search_tweeters,
            name=None, username=None,
            candidate=True,
                cand_pty_affiliation="DEM", cand_election_yr=None,
            context=False,
            skip=0, limit=50000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    user_ids = [i["user_id"] for i in tweeters]
    return query.documents_browse_twitter_tweets_user(es,
        user_ids=user_ids,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/twitter/tweets/candidate/rep/", summary="Explore Tweets from Republican Candidates")
def documents_browse_twitter_tweets_candidate_rep(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        tweeters = neo4j.read_transaction(cypher.graph_search_tweeters,
            name=None, username=None,
            candidate=True,
                cand_pty_affiliation="REP", cand_election_yr=None,
            context=False,
            skip=0, limit=50000,
            min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0,
            concise=True
        )
    user_ids = [i["user_id"] for i in tweeters]
    return query.documents_browse_twitter_tweets_user(es,
        user_ids=user_ids,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )

@router.post("/facebook/ads/", summary="Explore Ads")
def documents_browse_facebook_ads(body: DocumentsBrowseBaseBody):
    mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    return query.documents_browse_facebook_ads(es,
        text=body.text,
        histogram=body.histogram,
        skip=body.pagination.skip, limit=body.pagination.limit,
        mindate=mindate, maxdate=maxdate
    )
