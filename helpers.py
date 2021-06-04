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

def prepare_lists(lists, terms, ids, db):
    try:
        lists = [i for i in lists.split(",")]
    except:
        lists = []
    try:
        terms = [i for i in terms.split(",")]
    except:
        terms = []
    try:
        ids = [i for i in ids.split(",")]
    except:
        ids = []
    # grab list definition from firestore
    for list in lists:
        include = db.collection('lists').document(list).get().to_dict().get("include")
        list_terms = include.get("terms")
        list_ids = include.get("ids")
        if list_terms is not None and len(list_terms) > 0:
            terms.append(list_terms)
        else:
            terms.append(None)
        if list_ids is not None and len(list_ids) > 0:
            ids.append(list_ids)
        else:
            ids.append(None)
    # set empty values to none
    if terms is not None and len(terms) == 0:
        terms = None
    if ids is not None and len(ids) == 0:
        ids = None
    return {
        "terms": terms,
        "ids": ids
    }

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
