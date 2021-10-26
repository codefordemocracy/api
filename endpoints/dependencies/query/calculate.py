from ..helpers import clean_committees_names, flatten
from .preview import data_preview_organization_committee, data_preview_person_candidate
from .builder.functions import make_query, set_query_dates, set_freshness, set_query_clauses, add_must_clause, add_not_clause, add_filter_clause
from .builder.responses import get_response

def data_calculate_recipe_article(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram, freshness=None):
    # preprocess some recipes
    if template in ["PMYZ"]:
        if len(include["ids"][0]) > 0 or len(include["filters"][0]):
            committees = data_preview_organization_committee(es, None, include["ids"][0], include["filters"][0], None, None, None, 0, 10000, False)
            for committee in committees:
                include["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
            include["terms"][0] = list(set(include["terms"][0]))
        if len(exclude["ids"][0]) > 0 or len(exclude["filters"][0]) > 0:
            committees = data_preview_organization_committee(es, None, exclude["ids"][0], exclude["filters"][0], None, None, None, 0, 10000, False)
            for committee in committees:
                exclude["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
            exclude["terms"][0] = list(set(exclude["terms"][0]))
    if template in ["EBli"]:
        if len(include["ids"][0]) > 0 or len(include["filters"][0]):
            candidates = data_preview_person_candidate(es, None, include["ids"][0], include["filters"][0], None, None, None, 0, 10000, False)
            for candidate in candidates:
                include["terms"][0].append(candidate["cand_name"])
            include["terms"][0] = list(set(include["terms"][0]))
        if len(exclude["ids"][0]) > 0 or len(exclude["filters"][0]) > 0:
            candidates = data_preview_person_candidate(es, None, exclude["ids"][0], exclude["filters"][0], None, None, None, 0, 10000, False)
            for candidate in candidates:
                exclude["terms"][0].append(candidate["cand_name"])
            exclude["terms"][0] = list(set(exclude["terms"][0]))
    # build query
    q = make_query()
    q = set_query_dates(q, "extracted.date", mindate, maxdate)
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["PMYZ", "WdMv", "RasK", "GSmB"],
            "terms": [{
                "action": "match_phrase",
                "field": "extracted.text",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["EBli"],
            "terms": [{
                "action": "match_phrase",
                "field": "extracted.text",
                "slop": 2
            }]
        }
    ], include=include, exclude=exclude)
    # set sort
    if orderby == "date":
        q["sort"] = {
            "extracted.date": {"order": orderdir},
        }
    # filter on freshness
    if freshness is not None:
        q = set_freshness(q, "context.last_indexed", freshness)
    # get response
    response = get_response(es, "news_articles", q, skip, limit, count, histogram,
        date_field="extracted.date", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.extracted.title", "hits.hits._source.extracted.date", "hits.hits._source.extracted.url"],
        highlight=True
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for hit in response:
            row = {
                "date": hit["_source"]["extracted"]["date"][:10],
                "title": hit["_source"]["extracted"]["title"],
                "url": hit["_source"]["extracted"]["url"],
                "values_matched": flatten(list(hit["highlight"].values()))
            }
            elements.append(row)
        return elements
    return response

def data_calculate_recipe_ad(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram, freshness=None):
    # preprocess some recipes
    if template in ["D3WE", "BuW8"]:
        if len(include["ids"][0]) > 0 or len(include["filters"][0]):
            committees = data_preview_organization_committee(es, None, include["ids"][0], include["filters"][0], None, None, None, 0, 10000, False)
            for committee in committees:
                include["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
            include["terms"][0] = list(set(include["terms"][0]))
        if len(exclude["ids"][0]) > 0 or len(exclude["filters"][0]) > 0:
            committees = data_preview_organization_committee(es, None, exclude["ids"][0], exclude["filters"][0], None, None, None, 0, 10000, False)
            for committee in committees:
                exclude["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
            exclude["terms"][0] = list(set(exclude["terms"][0]))
    if template in ["Jphg"]:
        if len(include["ids"][0]) > 0 or len(include["filters"][0]):
            candidates = data_preview_person_candidate(es, None, include["ids"][0], include["filters"][0], None, None, None, 0, 10000, False)
            for candidate in candidates:
                include["terms"][0].append(candidate["cand_name"])
            include["terms"][0] = list(set(include["terms"][0]))
        if len(exclude["ids"][0]) > 0 or len(exclude["filters"][0]) > 0:
            candidates = data_preview_person_candidate(es, None, exclude["ids"][0], exclude["filters"][0], None, None, None, 0, 10000, False)
            for candidate in candidates:
                exclude["terms"][0].append(candidate["cand_name"])
            exclude["terms"][0] = list(set(exclude["terms"][0]))
    # build query
    q = make_query()
    q = set_query_dates(q, "obj.ad_creation_time", mindate, maxdate)
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["D3WE"],
            "terms": [{
                "action": "multi_match",
                "type": "phrase",
                "fields": ["obj.page_name", "obj.funding_entity"],
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["BuW8", "P2HG", "N7Jk", "8HcR"],
            "terms": [{
                "action": "match_phrase",
                "field": "obj.ad_creative_body",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["Jphg"],
            "terms": [{
                "action": "match_phrase",
                "field": "obj.ad_creative_body",
                "slop": 2
            }]
        }
    ], include=include, exclude=exclude)
    # set sort
    if orderby == "date":
        q["sort"] = {
            "obj.ad_creation_time": {"order": orderdir},
        }
    # filter on freshness
    if freshness is not None:
        q = set_freshness(q, "context.last_indexed", freshness)
    # get response
    response = get_response(es, "facebook_ads", q, skip, limit, count, histogram,
        date_field="obj.ad_creation_time", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.obj.ad_creation_time", "hits.hits._source.obj.page_name", "hits.hits._source.obj.funding_entity", "hits.hits._source.obj.id"],
        highlight=True
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for hit in response:
            row = {
                "created_at": hit["_source"]["obj"]["ad_creation_time"][:10],
                "page_name": hit["_source"]["obj"]["page_name"].upper(),
                "funding_entity": hit["_source"]["obj"].get("funding_entity").upper() if hit["_source"]["obj"].get("funding_entity") is not None else None,
                "archive_url": "https://facebook.com/ads/library/?id=" + str(hit["_source"]["obj"]["id"]),
                "values_matched": flatten(list(hit["highlight"].values()))
            }
            elements.append(row)
        return elements
    return response

def data_calculate_recipe_contribution(template, es, include, exclude, skip, limit, mindate, maxdate, filters, orderby, orderdir, count, histogram, freshness=None):
    # preprocess some recipes
    if template in ["DXhw", "KWYZ", "WK3K", "KR64", "F7Xn", "gXjA"]:
        # get committee ids for candidates
        list_settings = [
            {
                "position": 0,
                "templates": ["DXhw", "KWYZ"],
                "terms": [{
                    "action": "match_phrase",
                    "field": "processed.row.cand_name",
                    "slop": 5
                }],
                "ids": ["row.cand_id"],
                "filters": ["candidate"]
            }, {
                "position": 1,
                "templates": ["WK3K", "KR64", "F7Xn"],
                "terms": [{
                    "action": "match_phrase",
                    "field": "processed.row.cand_name",
                    "slop": 5
                }],
                "ids": ["row.cand_id"],
                "filters": ["candidate"]
            }, {
                "position": 2,
                "templates": ["gXjA"],
                "terms": [{
                    "action": "match_phrase",
                    "field": "processed.row.cand_name",
                    "slop": 5
                }],
                "ids": ["row.cand_id"],
                "filters": ["candidate"]
            }
        ]
        q = set_query_clauses(make_query(), template, list_settings=list_settings, include=include, exclude=None)
        q["size"] = 10000
        response = es.search(index="federal_fec_candidates", body=q, filter_path=["hits.hits._source.linkages.committees"])
        committee_ids = []
        for hit in response["hits"]["hits"]:
            for committee in hit["_source"]["linkages"]["committees"]:
                committee_ids.append(committee["cmte_id"])
        for i in list_settings:
            if template in i["templates"]:
                include["terms"][i["position"]] = None
                include["ids"][i["position"]] = committee_ids
                if len(exclude["terms"][i["position"]]) > 0 or len(exclude["ids"][i["position"]]) > 0:
                    q = set_query_clauses(make_query(), template, list_settings=list_settings, include=exclude, exclude=None)
                    q["size"] = 10000
                    response = es.search(index="federal_fec_candidates", body=q, filter_path=["hits.hits._source.linkages.committees"])
                    committee_ids = []
                    for hit in response["hits"]["hits"]:
                        for committee in hit["_source"]["linkages"]["committees"]:
                            committee_ids.append(committee["cmte_id"])
                    exclude["terms"][i["position"]] = None
                    exclude["ids"][i["position"]] = committee_ids
    # build query
    q = make_query()
    q = set_query_dates(q, "processed.date", mindate, maxdate)
    q = add_not_clause(q, {
        "term": {
            "row.target.committee.cmte_id": "c00401224" # actblue
        }
    })
    q = add_not_clause(q, {
        "term": {
            "row.target.committee.cmte_id": "c00694323" # winred
        }
    })
    if template in ["VqHR", "DXhw"]:
        q = add_filter_clause(q, {
            "term": {
                "row.source.classification": "committee"
            }
        })
    elif template in ["dFMy", "KWYZ"]:
        q = add_filter_clause(q, {
            "term": {
                "row.source.classification": "individual"
            }
        })
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["ReqQ", "IQL2", "WK3K"],
            "terms": [{
                "action": "match_phrase",
                "field": "row.source.committee.cmte_nm",
                "slop": 5
            }],
            "ids": ["row.source.committee.cmte_id"],
            "filters": ["source.committee"]
        }, {
            "position": 0,
            "templates": ["NcFz"],
            "terms": [{
                "action": "match_phrase",
                "field": "processed.source.donor.name",
                "slop": 2
            }],
            "filters": ["donor"]
        }, {
            "position": 0,
            "templates": ["m4YC", "Bs5W", "KR64"],
            "terms": [{
                "action": "match_phrase",
                "field": "row.source.donor.employer",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["7v4P", "6peF", "F7Xn", "T5xv", "F2mS", "gXjA"],
            "terms": [{
                "action": "match_phrase",
                "field": "row.source.donor.occupation",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["VqHR", "dFMy"],
            "terms": [{
                "action": "match_phrase",
                "field": "row.target.committee.cmte_nm",
                "slop": 5
            }],
            "ids": ["row.target.committee.cmte_id"],
            "filters": ["target.committee"]
        }, {
            "position": 0,
            "templates": ["DXhw", "KWYZ"],
            "ids": ["row.target.committee.cmte_id"]
        }, {
            "position": 1,
            "templates": ["T5xv", "F2mS", "gXjA"],
            "terms": [{
                "action": "match_phrase",
                "field": "row.source.donor.employer",
                "slop": 5
            }]
        }, {
            "position": 1,
            "templates": ["IQL2", "Bs5W", "6peF"],
            "terms": [{
                "action": "match_phrase",
                "field": "row.target.committee.cmte_nm",
                "slop": 5
            }],
            "ids": ["row.target.committee.cmte_id"],
            "filters": ["target.committee"]
        }, {
            "position": 1,
            "templates": ["WK3K", "KR64", "F7Xn"],
            "ids": ["row.target.committee.cmte_id"]
        }, {
            "position": 2,
            "templates": ["F2mS"],
            "terms": [{
                "action": "match_phrase",
                "field": "row.target.committee.cmte_nm",
                "slop": 5
            }],
            "ids": ["row.target.committee.cmte_id"],
            "filters": ["target.committee"]
        }, {
            "position": 2,
            "templates": ["gXjA"],
            "ids": ["row.target.committee.cmte_id"]
        }
    ], include=include, exclude=exclude)
    # add filters
    if filters["amount"]["min"] is not None or filters["amount"]["max"] is not None:
        range = dict()
        if filters["amount"]["min"] is not None:
            range["gte"] = filters["amount"]["min"]
        if filters["amount"]["max"] is not None:
            range["lte"] = filters["amount"]["max"]
        q = add_filter_clause(q, {
            "range": {
                "row.transaction_amt": range
            }
        })
    # set sort
    if orderby == "date":
        q["sort"] = {
            "processed.date": {"order": orderdir},
        }
    elif orderby == "amount":
        q["sort"] = {
            "row.transaction_amt": {"order": orderdir},
        }
    # filter on freshness
    if freshness is not None:
        q = set_freshness(q, "context.last_bulked", freshness)
    # get response
    response = get_response(es, "federal_fec_contributions", q, skip, limit, count, histogram,
        date_field="processed.date", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.processed", "hits.hits._source.row"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for hit in response:
            row = {
                "recipient_cmte_id": hit["_source"]["row"]["target"]["committee"]["cmte_id"],
                "recipient_cmte_nm": hit["_source"]["row"]["target"]["committee"]["cmte_nm"],
                "date": hit["_source"]["processed"]["date"][:10],
                "transaction_amt": hit["_source"]["row"]["transaction_amt"],
                "transaction_tp": hit["_source"]["row"]["transaction_tp"],
                "reported_by": "contributor" if str(hit["_source"]["row"]["transaction_tp"]).startswith("2") or str(hit["_source"]["row"]["transaction_tp"]).startswith("4") and hit["_source"]["row"]["transaction_tp"] != "24I" and hit["_source"]["row"]["transaction_tp"] != "24T" else "recipient",
                "url": "https://docquery.fec.gov/cgi-bin/fecimg/?" + str(hit["_source"]["row"]["image_num"]),
                "sub_id": str(hit["_source"]["row"]["sub_id"])
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
    return response

def data_calculate_recipe_lobbying_disclosures(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram, freshness=None, collapse=None):
    # build query
    q = make_query()
    q = set_query_dates(q, "processed.date_submitted", mindate, maxdate)
    if count is False and collapse is not None:
        q["collapse"] = {
            "field": collapse
        }
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["wLvp"],
            "terms": [{
                "action": "match_phrase",
                "field": "processed.registrant.name",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["kMER"],
            "terms": [{
                "action": "match_phrase",
                "field": "processed.client.name",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["MJdb"],
            "terms": [{
                "action": "match_phrase",
                "field": "processed.activities.specific_issues",
                "slop": 5
            }],
            "ids": ["processed.activities.issue_area_code"]
        }
    ], include=include, exclude=exclude)
    # set sort
    if orderby == "date":
        q["sort"] = {
            "processed.date_submitted": {"order": orderdir},
        }
    # filter on freshness
    if freshness is not None:
        q = set_freshness(q, "context.last_indexed", freshness)
    # get response
    response = get_response(es, "federal_senate_lobbying_disclosures,federal_house_lobbying_disclosures", q, skip, limit, count, histogram,
        date_field="processed.date_submitted", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.processed"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for hit in response:
            if collapse is not None:
                elements.append({
                    "registrant_senate_id": hit["_source"]["processed"]["registrant"].get("senate_id"),
                })
            else:
                elements.append({
                    "date_submitted": hit["_source"]["processed"].get("date_submitted")[:10],
                    "filing_year": hit["_source"]["processed"].get("filing_year"),
                    "filing_type": hit["_source"]["processed"].get("filing_type"),
                    "client_name": hit["_source"]["processed"]["client"].get("name").upper() if hit["_source"]["processed"]["client"].get("name") is not None else None,
                    "registrant_name": hit["_source"]["processed"]["registrant"].get("name").upper() if hit["_source"]["processed"]["registrant"].get("name") is not None else None,
                    "registrant_house_id": hit["_source"]["processed"]["registrant"].get("house_id"),
                    "registrant_senate_id": hit["_source"]["processed"]["registrant"].get("senate_id"),
                    "issue_area_code": ", ".join(list(set(flatten([activity.get("issue_area_code") for activity in hit["_source"]["processed"].get("activities", [])])))),
                    "url": hit["_source"]["processed"].get("url"),
                })
        return elements
    return response

def data_calculate_recipe_lobbying_disclosures_nested(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram, freshness=None, collapse=None):
    # build query
    q = make_query()
    q = set_query_dates(q, "parent.date_submitted", mindate, maxdate)
    if count is False and collapse is not None:
        q["collapse"] = {
            "field": collapse
        }
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["PLWg"],
            "terms": [{
                "action": "match_phrase",
                "field": "parent.registrant.name",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["QJeb"],
            "terms": [{
                "action": "match_phrase",
                "field": "parent.client.name",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["nNKT"],
            "terms": [{
                "action": "match_phrase",
                "field": "child.specific_issues",
                "slop": 5
            }],
            "ids": ["child.issue_area_code"]
        }
    ], include=include, exclude=exclude)
    # set sort
    if orderby == "date":
        q["sort"] = {
            "parent.date_submitted": {"order": orderdir},
        }
    # filter on freshness
    if freshness is not None:
        q = set_freshness(q, "context.last_indexed", freshness)
    # get response
    response = get_response(es, "federal_senate_lobbying_disclosures_nested,federal_house_lobbying_disclosures_nested", q, skip, limit, count, histogram,
        date_field="parent.date_submitted", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for hit in response:
            if collapse is not None:
                if collapse == "parent.registrant.senate_id.keyword":
                    elements.append({
                        "registrant_senate_id": hit["_source"]["parent"]["registrant"].get("senate_id"),
                    })
                elif collapse == "child.lobbyist.name.keyword":
                    elements.append({
                        "lobbyist_name": hit["_source"]["child"].get("lobbyist", {}).get("name"),
                    })
                elif collapse == "child.lobbyist.id":
                    elements.append({
                        "lobbyist_id": hit["_source"]["child"].get("lobbyist", {}).get("id"),
                    })
            else:
                elements.append({
                    "date_submitted": hit["_source"]["parent"].get("date_submitted")[:10],
                    "filing_year": hit["_source"]["parent"].get("filing_year"),
                    "filing_type": hit["_source"]["parent"].get("filing_type"),
                    "client_name": hit["_source"]["parent"]["client"].get("name").upper() if hit["_source"]["parent"]["client"].get("name") is not None else None,
                    "registrant_name": hit["_source"]["parent"]["registrant"].get("name").upper() if hit["_source"]["parent"]["registrant"].get("name") is not None else None,
                    "registrant_house_id": hit["_source"]["parent"]["registrant"].get("house_id"),
                    "registrant_senate_id": hit["_source"]["parent"]["registrant"].get("senate_id"),
                    "lobbyist_name": hit["_source"]["child"].get("lobbyist", {}).get("name"),
                    "lobbyist_id": hit["_source"]["child"].get("lobbyist", {}).get("id"),
                    "covered_position": hit["_source"]["child"].get("covered_position"),
                    "issue_area_code": hit["_source"]["child"].get("issue_area_code"),
                    "specific_issues": hit["_source"]["child"].get("specific_issues"),
                    "url": hit["_source"]["parent"].get("url"),
                })
        return elements
    return response

def data_calculate_recipe_lobbying_contributions_nested(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram, freshness=None):
    # build query
    q = make_query()
    q = set_query_dates(q, "child.date", mindate, maxdate)
    if template in ["V5Gh", "3Nrt", "Q23x", "Hsqk", "JCXA", "7EyP"]:
        q = add_filter_clause(q, {
            "term": {
                "child.contribution_type": "honorary"
            }
        })
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["PjyR", "WGb3", "MK93", "rXwv", "i5xq", "V5Gh", "3Nrt", "Q23x", "JCXA", "7EyP"],
            "ids": ["parent.registrant.senate_id"]
        }, {
            "position": 0,
            "templates": ["A3ue", "Hsqk"],
            "terms": [{
                "action": "match_phrase",
                "field": "child.lobbyist.name",
                "slop": 2
            }]
        }, {
            "position": 1,
            "templates": ["rXwv", "i5xq", "JCXA", "7EyP"],
            "terms": [{
                "action": "terms",
                "field": "child.lobbyist.name"
            }],
            "ids": ["child.lobbyist.id"]
        }
    ], include=include, exclude=exclude)
    # set sort
    if orderby == "date":
        q["sort"] = {
            "child.date": {"order": orderdir},
        }
    elif orderby == "amount":
        q["sort"] = {
            "child.amount": {"order": orderdir},
        }
    # filter on freshness
    if freshness is not None:
        q = set_freshness(q, "context.last_indexed", freshness)
    # get response
    response = get_response(es, "federal_senate_lobbying_contributions_nested,federal_house_lobbying_contributions_nested", q, skip, limit, count, histogram,
        date_field="child.date", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for hit in response:
            elements.append({
                "amount": hit["_source"]["child"].get("amount"),
                "date_contribution": hit["_source"]["child"].get("date")[:10],
                "date_submitted": hit["_source"]["parent"].get("date_submitted")[:10],
                "contribution_type": hit["_source"]["child"]["contribution_type"].upper(),
                "contributor_name": hit["_source"]["child"]["contributor_name"].upper(),
                "payee_name": hit["_source"]["child"]["payee_name"].upper(),
                "recipient_name": hit["_source"]["child"]["recipient_name"].upper(),
                "filing_year": hit["_source"]["parent"].get("filing_year"),
                "filing_type": hit["_source"]["parent"].get("filing_type"),
                "registrant_name": hit["_source"]["parent"]["registrant"].get("name").upper() if hit["_source"]["parent"]["registrant"].get("name") is not None else None,
                "registrant_house_id": hit["_source"]["parent"]["registrant"].get("house_id"),
                "registrant_senate_id": hit["_source"]["parent"]["registrant"].get("senate_id"),
                "lobbyist_name": hit["_source"]["parent"].get("lobbyist", {}).get("name"),
                "lobbyist_id": hit["_source"]["parent"].get("parent", {}).get("id"),
                "url": hit["_source"]["parent"].get("url"),
            })
        return elements
    return response

def data_calculate_recipe_990(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram, freshness=None):
    # preprocess some recipes
    if template in ["GCv2"]:
        if len(include["ids"][0]) > 0 or len(include["filters"][0]):
            committees = data_preview_organization_committee(es, None, include["ids"][0], include["filters"][0], None, None, None, 0, 10000, False)
            for committee in committees:
                include["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
            include["terms"][0] = list(set(include["terms"][0]))
        if len(exclude["ids"][0]) > 0 or len(exclude["filters"][0]) > 0:
            committees = data_preview_organization_committee(es, None, exclude["ids"][0], exclude["filters"][0], None, None, None, 0, 10000, False)
            for committee in committees:
                exclude["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
            exclude["terms"][0] = list(set(exclude["terms"][0]))
    if template in ["mFF7"]:
        if len(include["ids"][0]) > 0 or len(include["filters"][0]):
            candidates = data_preview_person_candidate(es, None, include["ids"][0], include["filters"][0], None, None, None, 0, 10000, False)
            for candidate in candidates:
                include["terms"][0].append(candidate["cand_name"])
            include["terms"][0] = list(set(include["terms"][0]))
        if len(exclude["ids"][0]) > 0 or len(exclude["filters"][0]) > 0:
            candidates = data_preview_person_candidate(es, None, exclude["ids"][0], exclude["filters"][0], None, None, None, 0, 10000, False)
            for candidate in candidates:
                exclude["terms"][0].append(candidate["cand_name"])
            exclude["terms"][0] = list(set(exclude["terms"][0]))
    # build query
    q = make_query()
    q = set_query_dates(q, "row.sub_date", mindate, maxdate)
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["GCv2", "P34n"],
            "terms": [{
                "action": "multi_match",
                "type": "phrase",
                "fields": [
                    "row.taxpayer_name",
                    "obj.ReturnHeader990x.schedule_parts.returnheader990x_part_i.BsnssNm_BsnssNmLn1Txt",
                    "obj.ReturnHeader990x.schedule_parts.returnheader990x_part_i.PrprrFrmNm_BsnssNmLn1Txt",
                    "obj.IRS990ScheduleI.groups.SkdIRcpntTbl.RcpntBsnssNm_BsnssNmLn1Txt",
                    "obj.IRS990ScheduleR.groups.SkdRIdDsrgrddEntts.DsrgrddEnttyNm_BsnssNmLn1Txt",
                    "obj.IRS990ScheduleR.groups.SkdRIdRltdTxExmptOrg.DsrgrddEnttyNm_BsnssNmLn1Txt",
                    "obj.IRS990ScheduleR.groups.SkdRIdRltdOrgTxblPrtnrshp.RltdOrgnztnNm_BsnssNmLn1Txt",
                    "obj.IRS990ScheduleR.groups.SkdRIdRltdOrgTxblCrpTr.RltdOrgnztnNm_BsnssNmLn1Txt",
                    "obj.IRS990ScheduleR.groups.SkdRTrnsctnsRltdOrg.BsnssNmLn1Txt",
                    "obj.IRS990PF.groups.PFCmpnstnOfHghstPdCntrct.CmpnstnOfHghstPdCntrct_BsnssNmLn1",
                    "obj.IRS990PF.groups.PFGrntOrCntrbtnPdDrYr.RcpntBsnssNm_BsnssNmLn1Txt",
                ],
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["K23r", "mFF7"],
            "terms": [{
                "action": "multi_match",
                "type": "phrase",
                "fields": [
                    "obj.ReturnHeader990x.schedule_parts.returnheader990x_part_i.BsnssOffcr_PrsnNm",
                    "obj.ReturnHeader990x.schedule_parts.returnheader990x_part_i.PrprrPrsn_PrprrPrsnNm",
                    "obj.IRS990.groups.Frm990PrtVIISctnA.PrsnNm",
                    "obj.IRS990.groups.CntrctrCmpnstn.CntrctrNm_PrsnNm",
                    "obj.IRS990EZ.groups.EZOffcrDrctrTrstEmpl.PrsnNm",
                    "obj.IRS990PF.groups.PFOffcrDrTrstKyEmpl.OffcrDrTrstKyEmpl_PrsnNm",
                    "obj.IRS990PF.groups.PFCmpnstnHghstPdEmpl.CmpnstnHghstPdEmpl_PrsnNm",
                ],
                "slop": 2
            }]
        }, {
            "position": 0,
            "templates": ["9q84"],
            "terms": [{
                "action": "multi_match",
                "type": "phrase",
                "fields": [
                    "obj.IRS990.schedule_parts.part_i.ActvtyOrMssnDsc",
                    "obj.IRS990.schedule_parts.part_iii.Dsc",
                    "obj.IRS990.schedule_parts.part_iii.MssnDsc",
                    "obj.IRS990.groups.PrgSrvcAccmActyOthr.Dsc",
                    "obj.IRS990EZ.schedule_parts.ez_part_iii.PrmryExmptPrpsTxt",
                    "obj.IRS990EZ.groups.EZPrgrmSrvcAccmplshmnt.DscrptnPrgrmSrvcAccmTxt",
                ],
                "slop": 5
            }]
        }
    ], include=include, exclude=exclude)
    # set sort
    if orderby == "date":
        q["sort"] = {
            "row.sub_date": {"order": orderdir},
        }
    # filter on freshness
    if freshness is not None:
        q = set_freshness(q, "context.last_indexed", freshness)
    # get response
    response = get_response(es, "federal_irs_990,federal_irs_990ez,federal_irs_990pf", q, skip, limit, count, histogram,
        date_field="row.sub_date", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.row"],
        highlight=True
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for hit in response:
            row = {
                "submission_date": hit["_source"]["row"]["sub_date"][:10],
                "ein": hit["_source"]["row"]["ein"],
                "taxpayer_name": hit["_source"]["row"]["taxpayer_name"],
                "return_type": hit["_source"]["row"]["return_type"],
                "tax_period": hit["_source"]["row"]["tax_period"],
                "xml_url": "https://s3.amazonaws.com/irs-form-990/" + str(hit["_source"]["row"]["object_id"]) + "_public.xml",
                "pdf_url": "https://apps.irs.gov/pub/epostcard/cor/" + str(hit["_source"]["row"]["ein"]) + "_" + str(hit["_source"]["row"]["tax_period"]) + "_" + str(hit["_source"]["row"]["return_type"]) + "_" + hit["_source"]["row"]["sub_date"][:10].replace("-", "") + str(hit["_source"]["row"]["return_id"]) + ".pdf",
                "values_matched": flatten(list(hit["highlight"].values()))
            }
            elements.append(row)
        return elements
    return response
