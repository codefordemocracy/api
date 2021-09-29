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

def prepare_lists(lists, db):
    include = dict()
    exclude = dict()
    # set default list values
    for k in ["terms", "ids", "filters"]:
        include[k] = []
    for k in ["terms", "ids", "filters"]:
        exclude[k] = []
    # get list definitions
    for list in lists or []:
        doc = db.collection('lists').document(list).get().to_dict()
        # process included entities
        list_include = doc.get("include", {})
        list_include_terms = list_include.get("terms")
        list_include_ids = list_include.get("ids")
        list_include_filters = list_include.get("filters")
        if list_include_terms is not None and len(list_include_terms) > 0:
            include["terms"].append(list_include_terms)
        else:
            include["terms"].append([])
        if list_include_ids is not None and len(list_include_ids) > 0:
            include["ids"].append(list_include_ids)
        else:
            include["ids"].append([])
        if list_include_filters is not None and len(list_include_filters) > 0:
            include["filters"].append(list_include_filters)
        else:
            include["filters"].append({})
        # process excluded entities
        list_exclude = doc.get("exclude", {})
        list_exclude_terms = list_exclude.get("terms")
        list_exclude_ids = list_exclude.get("ids")
        list_exclude_filters = list_exclude.get("filters")
        if list_exclude_terms is not None and len(list_exclude_terms) > 0:
            exclude["terms"].append(list_exclude_terms)
        else:
            exclude["terms"].append([])
        if list_exclude_ids is not None and len(list_exclude_ids) > 0:
            exclude["ids"].append(list_exclude_ids)
        else:
            exclude["ids"].append([])
        if list_exclude_filters is not None and len(list_exclude_filters) > 0:
            exclude["filters"].append(list_exclude_filters)
        else:
            exclude["filters"].append({})
    return {
        "include": include,
        "exclude": exclude
    }

def map_keys(entity, key, value):
    if entity in ["candidate", "committee"]:
        return "row."+key
    elif entity in ["source.candidate", "source.committee", "target.committee"]:
        return "row."+entity+"."+key
    elif entity == "donor":
        return "processed.source.donor."+key
    return key

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
    name = name.replace("OF", "")
    name = name.replace("THE", "")
    name = name.replace("FOR", "")
    name = name.split("- ")[0]
    name = name.split("(")[0]
    name = name.split(",")[0]
    name = name.replace(".", "")
    name = name.replace(" '", "")
    name = name.replace("  ", " ")
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
