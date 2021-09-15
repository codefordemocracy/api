def format_graph(graph):
    def censor(element):
        if "labels" in element:
            if "Ad" in element["labels"]:
                element["properties"] = {
                    "url": "facebook.com/ads/library/?id=" + element["properties"]["id"],
                    "uuid": element["properties"]["uuid"]
                }
        return element
    def stringify(element):
        if "date" in element["properties"]:
            element["properties"]["date"] = str(element["properties"]["date"])[:10]
        if "datetime" in element["properties"]:
            element["properties"]["datetime"] = str(element["properties"]["datetime"]).replace(".000000000", "")
        if "creation_time" in element["properties"]:
            element["properties"]["creation_time"] = str(element["properties"]["creation_time"]).replace(".000000000", "")
        if "delivery_start_time" in element["properties"]:
            element["properties"]["delivery_start_time"] = str(element["properties"]["delivery_start_time"]).replace(".000000000", "")
        if "delivery_stop_time" in element["properties"]:
            element["properties"]["delivery_stop_time"] = str(element["properties"]["delivery_stop_time"]).replace(".000000000", "")
        return element
    elements = []
    for node in graph.nodes:
        elements.append(censor(stringify({
            "element": "node",
            "id": node.id,
            "labels": node.labels,
            "properties": dict(list(sorted(node.items())))
        })))
    for edge in graph.relationships:
        elements.append(censor(stringify({
            "element": "edge",
            "id": edge.id,
            "type": edge.type,
            "source": edge.start_node.id,
            "target": edge.end_node.id,
            "properties": dict(list(sorted(edge.items())))
        })))
    return elements

def prepare_lists(lists, include_terms, include_ids, exclude_terms, exclude_ids, db):
    try:
        lists = [i for i in lists.split(",")]
    except:
        lists = []
    try:
        include_terms = [i for i in include_terms.split(",")]
    except:
        include_terms = []
    try:
        include_ids = [i for i in include_ids.split(",")]
    except:
        include_ids = []
    try:
        exclude_terms = [i for i in exclude_terms.split(",")]
    except:
        exclude_terms = []
    try:
        exclude_ids = [i for i in exclude_ids.split(",")]
    except:
        exclude_ids = []
    # grab list definition from firestore
    for list in lists:
        doc = db.collection('lists').document(list).get().to_dict()
        include = doc.get("include", {})
        list_include_terms = include.get("terms")
        list_include_ids = include.get("ids")
        if list_include_terms is not None and len(list_include_terms) > 0:
            include_terms.append(list_include_terms)
        else:
            include_terms.append(None)
        if list_include_ids is not None and len(list_include_ids) > 0:
            include_ids.append(list_include_ids)
        else:
            include_ids.append(None)
        exclude = doc.get("exclude", {})
        list_exclude_terms = exclude.get("terms")
        list_exclude_ids = exclude.get("ids")
        if list_exclude_terms is not None and len(list_exclude_terms) > 0:
            exclude_terms.append(list_exclude_terms)
        else:
            exclude_terms.append(None)
        if list_exclude_ids is not None and len(list_exclude_ids) > 0:
            exclude_ids.append(list_exclude_ids)
        else:
            exclude_ids.append(None)
    # set empty values to none
    if include_terms is not None and len(include_terms) == 0:
        include_terms = None
    if include_ids is not None and len(include_ids) == 0:
        include_ids = None
    if exclude_terms is not None and len(exclude_terms) == 0:
        exclude_terms = None
    if exclude_ids is not None and len(exclude_ids) == 0:
        exclude_ids = None
    return {
        "include": {
            "terms": include_terms,
            "ids": include_ids
        },
        "exclude": {
            "terms": exclude_terms,
            "ids": exclude_ids
        }
    }

def clean_committees_names(name):
    name = name.replace("COMMITTEE", "")
    name = name.replace("POLITICAL", "")
    name = name.replace("ACTION", "")
    name = name.replace("CORPORATION", "")
    name = name.replace("CORP", "")
    name = name.replace("LLC", "")
    name = name.replace("EMPLOYEES", "")
    name = name.replace("EMPLOYEE", "")
    name = name.replace("INC", "")
    name = name.replace("PAC", "")
    name = name.replace("FEDERAL", "")
    name = name.split("- ")[0]
    name = name.split("(")[0]
    name = name.split(",")[0]
    name = name.replace(".", "")
    name = name.replace(" '", "")
    name = name.replace("  ", " ")
    return name

def calc_affinity(count_a, count_b, count_both, count_total):
    support_a = count_a/count_total if count_total != 0 else 0
    support_b = count_b/count_total if count_total != 0 else 0
    support_ab = count_both/count_total if count_total != 0 else 0
    confidence_a = count_both/count_b if count_b != 0 else 0
    confidence_b = count_both/count_a if count_a != 0 else 0
    expected = support_a * support_b
    actual = support_ab
    lift = actual/expected if expected != 0 else 0
    return {
        "count": {
            "total": count_total,
            "a": count_a,
            "b": count_b,
            "both": count_both
        },
        "support": {
            "a": support_a,
            "b": support_b,
        },
        "confidence": {
            "a": confidence_a,
            "b": confidence_b,
        },
        "affinity": {
            "actual": actual,
            "expected": expected,
            "lift": lift
        }
    }

def calc_share(numerator, denominator):
    return {
        "numerator": numerator,
        "denominator": denominator,
        "share": numerator/denominator
    }
