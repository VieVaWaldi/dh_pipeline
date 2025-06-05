def clean_extractor_name(extractor_name: str):
    return (
        extractor_name.replace(" ", "")
        .replace("'", "")
        .replace("*", "STAR")
        .replace("=", "IS")
        .replace("+", "PLUS")
        .replace(":", "COLON")
        .replace("(", "LB")
        .replace(")", "RB")
    )
