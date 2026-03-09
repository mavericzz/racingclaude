"""Australian race class to numeric ladder conversion."""

CLASS_LADDER = {
    "mdn": 1, "maiden": 1, "mdn-sw": 1, "mdn sw": 1,
    "2y mdn": 1, "3y mdn": 1, "2y mdn-sw": 1, "3y mdn-sw": 1,
    "cl1": 2, "class 1": 2, "rst": 2,
    "cl2": 3, "class 2": 3,
    "cl3": 4, "class 3": 4,
    "cl4": 5, "class 4": 5,
    "cl5": 6, "class 5": 6,
    "cl6": 7, "class 6": 7,
    "bm50": 2.5, "bm54": 2.8, "bm58": 3.0,
    "bm60": 3.2, "bm64": 3.5, "bm66": 3.8,
    "bm68": 4.0, "bm70": 4.2, "bm72": 4.5,
    "bm74": 4.8, "bm76": 5.0, "bm78": 5.5,
    "bm80": 6.0, "bm82": 6.5, "bm84": 7.0,
    "bm86": 7.5, "bm88": 8.0, "bm90": 8.5,
    "listed": 9, "list": 9,
    "g3": 10, "group 3": 10,
    "g2": 11, "group 2": 11,
    "g1": 12, "group 1": 12,
    "open": 8, "hcp": 5, "wfa": 8,
    "quality": 7, "stakes": 9,
}


def parse_class_numeric(class_str):
    if not class_str or not isinstance(class_str, str):
        return 5.0

    lower = class_str.lower().strip()

    # Direct match
    if lower in CLASS_LADDER:
        return CLASS_LADDER[lower]

    # Partial match
    for key, val in CLASS_LADDER.items():
        if key in lower:
            return val

    # BM number extraction
    import re
    bm_match = re.search(r"bm\s*(\d+)", lower)
    if bm_match:
        num = int(bm_match.group(1))
        return min(8.5, max(2.5, num / 10))

    return 5.0
