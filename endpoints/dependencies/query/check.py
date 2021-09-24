import datetime
import pytz

def status_check_data_ads(es):
    count = es.count(index="facebook_ads")
    coverage = es.search(index="facebook_ads",
        body={
            "size": 0,
            "aggs": {
                "dates": {
                    "date_histogram": {
                        "field": "obj.ad_creation_time",
                        "calendar_interval": "quarter",
                        "time_zone": "America/New_York"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    last_indexed = es.search(index="facebook_ads",
        body={
            "size": 0,
            "aggs": {
                "last_indexed": {
                    "max": {
                        "field": "context.last_indexed"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    return {
        "count": count["count"],
        "coverage": coverage["aggregations"]["dates"]["buckets"],
        "last_indexed": pytz.utc.localize(datetime.datetime.fromtimestamp(last_indexed["aggregations"]["last_indexed"]["value"]/1000)).astimezone(tz=pytz.timezone('US/Eastern'))
    }

def status_check_data_contributions(es):
    total_count = es.count(index="federal_fec_contributions")
    bulk_count = es.count(index="federal_fec_contributions",
        body={
            "query": {
                "exists": {
                    "field": "row.sub_id"
                }
            }
        }
    )
    bulk_coverage = es.search(index="federal_fec_contributions",
        body={
            "size": 0,
            "query": {
                "exists": {
                    "field": "row.sub_id"
                }
            },
            "aggs": {
                "dates": {
                    "date_histogram": {
                        "field": "processed.date",
                        "calendar_interval": "quarter",
                        "time_zone": "America/New_York"
                    }
                }
            }
        },
        filter_path=["aggregations"],
        request_timeout=30
    )
    bulk_last_indexed = es.search(index="federal_fec_contributions",
        body={
            "size": 0,
            "query": {
                "exists": {
                    "field": "row.sub_id"
                }
            },
            "aggs": {
                "last_indexed": {
                    "max": {
                        "field": "context.last_bulked"
                    }
                }
            }
        },
        filter_path=["aggregations"],
        request_timeout=30
    )
    api_count = es.count(index="federal_fec_contributions",
        body={
            "query": {
                "exists": {
                    "field": "obj.sub_id"
                }
            }
        }
    )
    api_coverage = es.search(index="federal_fec_contributions",
        body={
            "size": 0,
            "query": {
                "exists": {
                    "field": "obj.sub_id"
                }
            },
            "aggs": {
                "dates": {
                    "date_histogram": {
                        "field": "processed.date",
                        "calendar_interval": "quarter",
                        "time_zone": "America/New_York"
                    }
                }
            }
        },
        filter_path=["aggregations"],
        request_timeout=30
    )
    api_last_indexed = es.search(index="federal_fec_contributions",
        body={
            "size": 0,
            "query": {
                "exists": {
                    "field": "obj.sub_id"
                }
            },
            "aggs": {
                "last_indexed": {
                    "max": {
                        "field": "context.last_indexed"
                    }
                }
            }
        },
        filter_path=["aggregations"],
        request_timeout=30
    )
    return {
        "count": total_count["count"],
        "bulk": {
            "count": bulk_count["count"],
            "coverage": bulk_coverage["aggregations"]["dates"]["buckets"],
            "last_indexed": pytz.utc.localize(datetime.datetime.fromtimestamp(bulk_last_indexed["aggregations"]["last_indexed"]["value"]/1000)).astimezone(tz=pytz.timezone('US/Eastern'))
        },
        "api": {
            "count": api_count["count"],
            "coverage": api_coverage["aggregations"]["dates"]["buckets"],
            "last_indexed": pytz.utc.localize(datetime.datetime.fromtimestamp(api_last_indexed["aggregations"]["last_indexed"]["value"]/1000)).astimezone(tz=pytz.timezone('US/Eastern'))
        }
    }

def status_check_data_lobbying(es):
    senate_disclosures_count = es.count(index="federal_senate_lobbying_disclosures")
    senate_disclosures_coverage = es.search(index="federal_senate_lobbying_disclosures",
        body={
            "size": 0,
            "aggs": {
                "dates": {
                    "date_histogram": {
                        "field": "processed.date_submitted",
                        "calendar_interval": "quarter",
                        "time_zone": "America/New_York"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    senate_disclosures_last_indexed = es.search(index="federal_senate_lobbying_disclosures",
        body={
            "size": 0,
            "aggs": {
                "last_indexed": {
                    "max": {
                        "field": "context.last_indexed"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    house_disclosures_count = es.count(index="federal_house_lobbying_disclosures")
    house_disclosures_coverage = es.search(index="federal_house_lobbying_disclosures",
        body={
            "size": 0,
            "aggs": {
                "dates": {
                    "date_histogram": {
                        "field": "processed.date_submitted",
                        "calendar_interval": "quarter",
                        "time_zone": "America/New_York"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    house_disclosures_last_indexed = es.search(index="federal_house_lobbying_disclosures",
        body={
            "size": 0,
            "aggs": {
                "last_indexed": {
                    "max": {
                        "field": "context.last_indexed"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    senate_contributions_count = es.count(index="federal_senate_lobbying_contributions")
    senate_contributions_coverage = es.search(index="federal_senate_lobbying_contributions",
        body={
            "size": 0,
            "aggs": {
                "dates": {
                    "date_histogram": {
                        "field": "processed.date_submitted",
                        "calendar_interval": "quarter",
                        "time_zone": "America/New_York"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    senate_contributions_last_indexed = es.search(index="federal_senate_lobbying_contributions",
        body={
            "size": 0,
            "aggs": {
                "last_indexed": {
                    "max": {
                        "field": "context.last_indexed"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    house_contributions_count = es.count(index="federal_house_lobbying_contributions")
    house_contributions_coverage = es.search(index="federal_house_lobbying_contributions",
        body={
            "size": 0,
            "aggs": {
                "dates": {
                    "date_histogram": {
                        "field": "processed.date_submitted",
                        "calendar_interval": "quarter",
                        "time_zone": "America/New_York"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    house_contributions_last_indexed = es.search(index="federal_house_lobbying_contributions",
        body={
            "size": 0,
            "aggs": {
                "last_indexed": {
                    "max": {
                        "field": "context.last_indexed"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    return {
        "senate": {
            "disclosures": {
                "count": senate_disclosures_count["count"],
                "coverage": senate_disclosures_coverage["aggregations"]["dates"]["buckets"],
                "last_indexed": pytz.utc.localize(datetime.datetime.fromtimestamp(senate_disclosures_last_indexed["aggregations"]["last_indexed"]["value"]/1000)).astimezone(tz=pytz.timezone('US/Eastern'))
            },
            "contributions": {
                "count": senate_contributions_count["count"],
                "coverage": senate_contributions_coverage["aggregations"]["dates"]["buckets"],
                "last_indexed": pytz.utc.localize(datetime.datetime.fromtimestamp(senate_contributions_last_indexed["aggregations"]["last_indexed"]["value"]/1000)).astimezone(tz=pytz.timezone('US/Eastern'))
            }
        },
        "house": {
            "disclosures": {
                "count": house_disclosures_count["count"],
                "coverage": house_disclosures_coverage["aggregations"]["dates"]["buckets"],
                "last_indexed": pytz.utc.localize(datetime.datetime.fromtimestamp(house_disclosures_last_indexed["aggregations"]["last_indexed"]["value"]/1000)).astimezone(tz=pytz.timezone('US/Eastern'))
            },
            "contributions": {
                "count": house_contributions_count["count"],
                "coverage": house_contributions_coverage["aggregations"]["dates"]["buckets"],
                "last_indexed": pytz.utc.localize(datetime.datetime.fromtimestamp(house_contributions_last_indexed["aggregations"]["last_indexed"]["value"]/1000)).astimezone(tz=pytz.timezone('US/Eastern'))
            }
        }
    }

def status_check_data_990(es):
    count = es.count(index="federal_irs_990,federal_irs_990ez,federal_irs_990pf")
    coverage = es.search(index="federal_irs_990,federal_irs_990ez,federal_irs_990pf",
        body={
            "size": 0,
            "aggs": {
                "dates": {
                    "date_histogram": {
                        "field": "row.sub_date",
                        "calendar_interval": "quarter",
                        "time_zone": "America/New_York"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    last_indexed = es.search(index="federal_irs_990,federal_irs_990ez,federal_irs_990pf",
        body={
            "size": 0,
            "aggs": {
                "last_indexed": {
                    "max": {
                        "field": "context.last_indexed"
                    }
                }
            }
        },
        filter_path=["aggregations"]
    )
    return {
        "count": count["count"],
        "coverage": coverage["aggregations"]["dates"]["buckets"],
        "last_indexed": pytz.utc.localize(datetime.datetime.fromtimestamp(last_indexed["aggregations"]["last_indexed"]["value"]/1000)).astimezone(tz=pytz.timezone('US/Eastern'))
    }