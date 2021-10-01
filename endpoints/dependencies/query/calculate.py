from ..helpers import clean_committees_names
from .preview import data_preview_organization_committee
from .builder.functions import make_query, set_query_dates, set_query_clauses, add_must_clause, add_not_clause, add_filter_clause
from .builder.responses import get_response

def data_calculate_recipe_article(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram):
    # preprocess some recipes
    if template in ["PMYZ"]:
        committees = data_preview_organization_committee(es, None, include["ids"][0], include["filters"][0], None, None, None, 0, 10000, False)
        for committee in committees:
            include["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
        if len(exclude["ids"][0]) > 0:
            committees = data_preview_organization_committee(es, None, exclude["ids"][0], exclude["filters"][0], None, None, None, 0, 10000, False)
            for committee in committees:
                exclude["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
    # build query
    q = make_query()
    q = set_query_dates(q, "extracted.date", mindate, maxdate)
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["PMYZ", "WdMv", "RasK"],
            "terms": [{
                "action": "match_phrase",
                "field": "extracted.text",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["GSmB"],
            "terms": [{
                "action": "match_phrase",
                "field": "extracted.text",
                "slop": 5
            }]
        }
    ], include=include, exclude=exclude)
    # set sort
    if orderby == "date":
        q["sort"] = {
            "extracted.date": {"order": orderdir},
        }
    # get response
    response = get_response(es, "news_articles", q, skip, limit, count, histogram,
        date_field="extracted.date", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.extracted.title", "hits.hits._source.extracted.date", "hits.hits._source.extracted.url"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for source in response:
            row = {
                "date": source["extracted"]["date"][:10],
                "title": source["extracted"]["title"],
                "url": source["extracted"]["url"]
            }
            elements.append(row)
        return elements
    return response

def data_calculate_recipe_ad(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram):
    # preprocess some recipes
    if template in ["D3WE", "BuW8"]:
        committees = data_preview_organization_committee(es, None, include["ids"][0], include["filters"][0], None, None, None, 0, 10000, False)
        for committee in committees:
            include["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
        if len(exclude["ids"][0]) > 0:
            committees = data_preview_organization_committee(es, None, exclude["ids"][0], exclude["filters"][0], None, None, None, 0, 10000, False)
            for committee in committees:
                exclude["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
    # build query
    q = make_query()
    q = set_query_dates(q, "obj.ad_creation_time", mindate, maxdate)
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["D3WE"],
            "terms": [{
                "action": "match_phrase",
                "field": "obj.page_name",
                "slop": 5
            }, {
                "action": "match_phrase",
                "field": "obj.funding_entity",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["BuW8", "P2HG", "N7Jk"],
            "terms": [{
                "action": "match_phrase",
                "field": "obj.ad_creative_body",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["8HcR"],
            "terms": [{
                "action": "match_phrase",
                "field": "obj.ad_creative_body",
                "slop": 5
            }]
        }
    ], include=include, exclude=exclude)
    # set sort
    if orderby == "date":
        q["sort"] = {
            "obj.ad_creation_time": {"order": orderdir},
        }
    # get response
    response = get_response(es, "facebook_ads", q, skip, limit, count, histogram,
        date_field="obj.ad_creation_time", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.obj.ad_creation_time", "hits.hits._source.obj.page_name", "hits.hits._source.obj.funding_entity", "hits.hits._source.obj.id"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for source in response:
            row = {
                "created_at": source["obj"]["ad_creation_time"][:10],
                "page_name": source["obj"]["page_name"],
                "funding_entity": source["obj"].get("funding_entity"),
                "archive_url": "https://facebook.com/ads/library/?id=" + str(source["obj"]["id"])
            }
            elements.append(row)
        return elements
    return response

def data_calculate_recipe_contribution(template, es, include, exclude, skip, limit, mindate, maxdate, filters, orderby, orderdir, count, histogram):
    # preprocess some recipes
    if template in ["DXhw", "KYWZ", "WK3K", "KR64", "F7Xn", "gXjA"]:
        # get committee ids for candidates
        list_settings = [
            {
                "position": 0,
                "templates": ["DXhw"],
                "terms": [{
                    "action": "match_phrase",
                    "field": "processed.row.cand_name",
                    "slop": 5
                }],
                "ids": [{
                    "action": "term",
                    "field": "row.cand_id"
                }],
                "filters": ["candidate"]
            }, {
                "position": 1,
                "templates": ["WK3K", "KR64", "F7Xn"],
                "terms": [{
                    "action": "match_phrase",
                    "field": "processed.row.cand_name",
                    "slop": 5
                }],
                "ids": [{
                    "action": "term",
                    "field": "row.cand_id"
                }],
                "filters": ["candidate"]
            }, {
                "position": 2,
                "templates": ["gXjA"],
                "terms": [{
                    "action": "match_phrase",
                    "field": "processed.row.cand_name",
                    "slop": 5
                }],
                "ids": [{
                    "action": "term",
                    "field": "row.cand_id"
                }],
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
    elif template in ["dFMy", "KYWZ"]:
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
            "ids": [{
                "action": "term",
                "field": "row.source.committee.cmte_id"
            }],
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
            "ids": [{
                "action": "term",
                "field": "row.target.committee.cmte_id"
            }],
            "filters": ["target.committee"]
        }, {
            "position": 0,
            "templates": ["DXhw", "KYWZ"],
            "ids": [{
                "action": "term",
                "field": "row.target.committee.cmte_id"
            }]
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
            "ids": [{
                "action": "term",
                "field": "row.target.committee.cmte_id"
            }],
            "filters": ["target.committee"]
        }, {
            "position": 1,
            "templates": ["WK3K", "KR64", "F7Xn"],
            "ids": [{
                "action": "term",
                "field": "row.target.committee.cmte_id"
            }]
        }, {
            "position": 2,
            "templates": ["F2mS"],
            "terms": [{
                "action": "match_phrase",
                "field": "row.target.committee.cmte_nm",
                "slop": 5
            }],
            "ids": [{
                "action": "term",
                "field": "row.target.committee.cmte_id"
            }],
            "filters": ["target.committee"]
        }, {
            "position": 2,
            "templates": ["gXjA"],
            "ids": [{
                "action": "term",
                "field": "row.target.committee.cmte_id"
            }]
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
    # get response
    response = get_response(es, "federal_fec_contributions", q, skip, limit, count, histogram,
        date_field="processed.date", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.processed", "hits.hits._source.row"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for source in response:
            row = {
                "recipient_cmte_id": source["row"]["target"]["committee"]["cmte_id"],
                "recipient_cmte_nm": source["row"]["target"]["committee"]["cmte_nm"],
                "date": source["processed"]["date"][:10],
                "transaction_amt": source["row"]["transaction_amt"],
                "transaction_tp": source["row"]["transaction_tp"],
                "reported_by": "contributor" if source["row"]["transaction_tp"].startswith("2") or source["row"]["transaction_tp"].startswith("4") and source["row"]["transaction_tp"] != "24I" and source["row"]["transaction_tp"] != "24T" else "recipient",
                "url": "https://docquery.fec.gov/cgi-bin/fecimg/?" + str(source["row"]["image_num"]),
                "sub_id": str(source["row"]["sub_id"])
            }
            if "committee" in source["row"]["source"]:
                row["contributor_cmte_id"] = source["row"]["source"]["committee"]["cmte_id"]
                row["contributor_cmte_nm"] = source["row"]["source"]["committee"]["cmte_nm"]
            elif "donor" in source["row"]["source"]:
                row["donor_name"] = source["processed"]["source"]["donor"]["name"]
                row["donor_zip_code"] = source["row"]["source"]["donor"]["zip_code"]
                row["donor_employer"] = source["row"]["source"]["donor"]["employer"]
                row["donor_occupation"] = source["row"]["source"]["donor"]["occupation"]
            elements.append(row)
        return elements
    return response

def data_calculate_recipe_lobbying_disclosures(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram, concise=False):
    # build query
    q = make_query()
    q = set_query_dates(q, "processed.date_submitted", mindate, maxdate)
    if count is False and concise is True:
        q["collapse"] = {
            "field": "processed.registrant.senate_id.keyword"
        }
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["kMER"],
            "terms": [{
                "action": "match_phrase",
                "field": "processed.client.name",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["wLvp"],
            "terms": [{
                "action": "match_phrase",
                "field": "processed.registrant.name",
                "slop": 5
            }]
        }, {
            "position": 0,
            "templates": ["MJdb"],
            "terms": [{
                "action": "match_phrase",
                "field": "processed.activities",
                "slop": 5
            }, {
                "action": "match_phrase",
                "field": "processed.issues.display",
                "slop": 5
            }],
            "ids": [{
                "action": "term",
                "field": "processed.issues.code"
            }]
        }
    ], include=include, exclude=exclude)
    # set sort
    if orderby == "date":
        q["sort"] = {
            "processed.date_submitted": {"order": orderdir},
        }
    # get response
    response = get_response(es, "federal_senate_lobbying_disclosures,federal_house_lobbying_disclosures", q, skip, limit, count, histogram,
        date_field="processed.date_submitted", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.processed"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for source in response:
            if concise is True:
                elements.append({
                    "registrant_senate_id": source["processed"]["registrant"].get("senate_id"),
                })
            else:
                elements.append({
                    "date_submitted": source["processed"].get("date_submitted")[:10],
                    "filing_year": source["processed"].get("filing_year"),
                    "filing_type": source["processed"].get("filing_type"),
                    "client_name": source["processed"]["client"].get("name").upper() if source["processed"]["client"].get("name") is not None else None,
                    "registrant_name": source["processed"]["registrant"].get("name").upper() if source["processed"]["registrant"].get("name") is not None else None,
                    "registrant_house_id": source["processed"]["registrant"].get("house_id"),
                    "registrant_senate_id": source["processed"]["registrant"].get("senate_id"),
                    "lobbyists": ", ".join([i["name"] for i in source["processed"].get("lobbyists", [])]),
                    "lobbying_activities": "; ".join(source["processed"].get("activities", [])),
                    "lobbying_issues": ", ".join([i["code"] for i in source["processed"].get("issues", [])]),
                    "lobbying_coverage": "; ".join(source["processed"].get("coverage", [])),
                    "url": source["processed"].get("url"),
                })
        return elements
    return response

def data_calculate_recipe_lobbying_contributions(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram):
    # build query
    q = make_query()
    q = set_query_dates(q, "processed.date_submitted", mindate, maxdate)
    q = add_filter_clause(q, {
        "term": {
            "processed.no_contributions": False
        }
    })
    if template in ["V5Gh", "3Nrt", "Q23x"]:
        q = add_filter_clause(q, {
            "term": {
                "processed.contributions.contribution_type": "honorary"
            }
        })
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["PjyR", "WGb3", "MK93", "V5Gh", "3Nrt", "Q23x"],
            "ids": [{
                "action": "term",
                "field": "processed.registrant.senate_id"
            }]
        }
    ], include=include, exclude=exclude)
    # set sort
    if orderby == "date":
        q["sort"] = {
            "processed.date_submitted": {"order": orderdir},
        }
    # get response
    response = get_response(es, "federal_senate_lobbying_contributions,federal_house_lobbying_contributions", q, skip, limit, count, histogram,
        date_field="processed.date_submitted", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.processed"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for source in response:
            contributions = source["processed"].get("contributions")
            if template in ["V5Gh", "3Nrt", "Q23x"]:
                contributions = [c for c in contributions if c["contribution_type"] == "Honorary Expenses"]
            for contribution in contributions or []:
                contribution["date_contribution"] = contribution.pop("date")[:10]
                contribution["date_submitted"] = source["processed"].get("date_submitted")[:10]
                contribution["contribution_type"] = contribution["contribution_type"].upper()
                contribution["contributor_name"] = contribution["contributor_name"].upper()
                contribution["payee_name"] = contribution["payee_name"].upper()
                contribution["recipient_name"] = contribution["recipient_name"].upper()
                contribution["filing_year"] = source["processed"].get("filing_year")
                contribution["filing_type"] = source["processed"].get("filing_type")
                contribution["registrant_name"] = source["processed"]["registrant"].get("name")
                contribution["registrant_house_id"] = source["processed"]["registrant"].get("house_id")
                contribution["registrant_senate_id"] = source["processed"]["registrant"].get("senate_id")
                contribution["lobbyist_name"] = source["processed"].get("lobbyist", {}).get("name").upper() if source["processed"].get("lobbyist", {}).get("name") is not None else None
                contribution["lobbyist_id"] = source["processed"].get("lobbyist", {}).get("id")
                contribution["url"] = source["processed"].get("url")
                elements.append(contribution)
        return elements
    return response

def data_calculate_recipe_990(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram):
    # preprocess some recipes
    if template in ["GCv2"]:
        committees = data_preview_organization_committee(es, None, include["ids"][0], include["filters"][0], None, None, None, 0, 10000, False)
        for committee in committees:
            include["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
        if len(exclude["ids"][0]) > 0:
            committees = data_preview_organization_committee(es, None, exclude["ids"][0], exclude["filters"][0], None, None, None, 0, 10000, False)
            for committee in committees:
                exclude["terms"][0].append(clean_committees_names(committee["cmte_nm"]))
    # build query
    q = make_query()
    q = set_query_dates(q, "row.sub_date", mindate, maxdate)
    q = set_query_clauses(q, template, list_settings=[
        {
            "position": 0,
            "templates": ["GCv2"],
            "terms": [{
                "action": "multi_match",
                "field": "query",
                "slop": 5
            }]
        }
    ], include=include, exclude=exclude)
    # set sort
    if orderby == "date":
        q["sort"] = {
            "row.sub_date": {"order": orderdir},
        }
    # get response
    response = get_response(es, "federal_irs_990,federal_irs_990ez,federal_irs_990pf", q, skip, limit, count, histogram,
        date_field="row.sub_date", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.row"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for source in response:
            row = {
                "submission_date": source["row"]["sub_date"][:10],
                "ein": source["row"]["ein"],
                "taxpayer_name": source["row"]["taxpayer_name"],
                "return_type": source["row"]["return_type"],
                "tax_period": source["row"]["tax_period"],
                "xml_url": "https://s3.amazonaws.com/irs-form-990/" + str(source["row"]["object_id"]) + "_public.xml",
                "pdf_url": "https://apps.irs.gov/pub/epostcard/cor/" + str(source["row"]["ein"]) + "_" + str(source["row"]["tax_period"]) + "_" + str(source["row"]["return_type"]) + "_" + source["row"]["sub_date"][:10].replace("-", "") + str(source["row"]["return_id"]) + ".pdf"
            }
            elements.append(row)
        return elements
    return response
