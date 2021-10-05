from ..helpers import clean_committees_names, flatten
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

def data_calculate_recipe_lobbying_disclosures(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram, collapse=None):
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
    # get response
    response = get_response(es, "federal_senate_lobbying_disclosures,federal_house_lobbying_disclosures", q, skip, limit, count, histogram,
        date_field="processed.date_submitted", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source.processed"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for source in response:
            if collapse is not None:
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
                    "issue_area_code": ", ".join(list(set(flatten([activity.get("issue_area_code") for activity in source["processed"].get("activities", [])])))),
                    "url": source["processed"].get("url"),
                })
        return elements
    return response

def data_calculate_recipe_lobbying_disclosures_nested(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram, collapse=None):
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
    # get response
    response = get_response(es, "federal_senate_lobbying_disclosures_nested,federal_house_lobbying_disclosures_nested", q, skip, limit, count, histogram,
        date_field="parent.date_submitted", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for source in response:
            if collapse is not None:
                if collapse == "parent.registrant.senate_id.keyword":
                    elements.append({
                        "registrant_senate_id": source["parent"]["registrant"].get("senate_id"),
                    })
                elif collapse == "child.lobbyist.name.keyword":
                    elements.append({
                        "lobbyist_name": source["child"].get("lobbyist", {}).get("name").upper() if source["child"].get("lobbyist", {}).get("name") is not None else None,
                    })
                elif collapse == "child.lobbyist.id":
                    elements.append({
                        "lobbyist_id": source["child"].get("lobbyist", {}).get("id"),
                    })
            else:
                elements.append({
                    "date_submitted": source["parent"].get("date_submitted")[:10],
                    "filing_year": source["parent"].get("filing_year"),
                    "filing_type": source["parent"].get("filing_type"),
                    "client_name": source["parent"]["client"].get("name").upper() if source["parent"]["client"].get("name") is not None else None,
                    "registrant_name": source["parent"]["registrant"].get("name").upper() if source["parent"]["registrant"].get("name") is not None else None,
                    "registrant_house_id": source["parent"]["registrant"].get("house_id"),
                    "registrant_senate_id": source["parent"]["registrant"].get("senate_id"),
                    "lobbyist_name": source["child"].get("lobbyist", {}).get("name").upper() if source["child"].get("lobbyist", {}).get("name") is not None else None,
                    "lobbyist_id": source["child"].get("lobbyist", {}).get("id"),
                    "covered_position": source["child"].get("covered_position"),
                    "issue_area_code": source["child"].get("issue_area_code"),
                    "specific_issues": source["child"].get("specific_issues"),
                    "url": source["parent"].get("url"),
                })
        return elements
    return response

def data_calculate_recipe_lobbying_contributions_nested(template, es, include, exclude, skip, limit, mindate, maxdate, orderby, orderdir, count, histogram):
    # build query
    q = make_query()
    q = set_query_dates(q, "child.date", mindate, maxdate)
    if template in ["V5Gh", "3Nrt", "Q23x", "JCXA", "7EyP"]:
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
    # get response
    response = get_response(es, "federal_senate_lobbying_contributions_nested,federal_house_lobbying_contributions_nested", q, skip, limit, count, histogram,
        date_field="child.date", mindate=mindate, maxdate=maxdate,
        filter_path=["hits.hits._source"]
    )
    # process rows
    if count is not True and histogram is not True:
        elements = []
        for source in response:
            elements.append({
                "date_contribution": source["child"].get("date")[:10],
                "date_submitted": source["parent"].get("date_submitted")[:10],
                "contribution_type": source["child"]["contribution_type"].upper(),
                "contributor_name": source["child"]["contributor_name"].upper(),
                "payee_name": source["child"]["payee_name"].upper(),
                "recipient_name": source["child"]["recipient_name"].upper(),
                "filing_year": source["parent"].get("filing_year"),
                "filing_type": source["parent"].get("filing_type"),
                "registrant_name": source["parent"]["registrant"].get("name").upper() if source["parent"]["registrant"].get("name") is not None else None,
                "registrant_house_id": source["parent"]["registrant"].get("house_id"),
                "registrant_senate_id": source["parent"]["registrant"].get("senate_id"),
                "lobbyist_name": source["parent"].get("lobbyist", {}).get("name").upper() if source["parent"].get("lobbyist", {}).get("name") is not None else None,
                "lobbyist_id": source["parent"].get("parent", {}).get("id"),
                "url": source["parent"].get("url"),
            })
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
