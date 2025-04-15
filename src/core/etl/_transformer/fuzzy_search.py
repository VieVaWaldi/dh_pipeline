from sqlalchemy import text, func, or_


def fuzzy_search(
    session, id_column, name_column, search_term, max_distance_percent=0.15
):
    """
    Generic fuzzy search function that works with any model that has ID and name/title columns.

    Parameters:
    - session: SQLAlchemy session
    - id_column: The ID column of the model (e.g., Projects.id)
    - name_column: The name/title column to search against (e.g., Projects.title, Institutions.name)
    - search_term: The search string
    - max_distance_percent: Maximum Levenshtein distance as a percentage of the search term length

    Returns:
    - List of tuples containing the name/title and the distance score
    """
    # Normalizations and calculations
    normalized_search = text(
        "regexp_replace(lower(:search_term), '[^a-z0-9]', '', 'g')"
    )
    # Use the column name dynamically
    normalized_name = text(
        f"regexp_replace(lower({name_column.key}), '[^a-z0-9]', '', 'g')"
    )

    search_term_len = len("".join(ch for ch in search_term.lower() if ch.isalnum()))
    max_distance = int(search_term_len * max_distance_percent)

    # Extract significant words (4+ chars) for word overlap matching
    significant_words = [word for word in search_term.lower().split() if len(word) >= 4]
    word_conditions = []

    for word in significant_words:
        word_conditions.append(func.lower(name_column).like(f"%{word}%"))

    # Start basic pre-filter query
    pre_filter_query = session.query(id_column, name_column).filter(
        # Length filtering
        func.abs(func.length(normalized_name) - func.length(normalized_search))
        <= max_distance
    )

    # Add word overlap condition if we have any significant words
    if word_conditions:
        pre_filter_query = pre_filter_query.filter(or_(*word_conditions))

    # Add trigram similarity filter and parameters
    pre_filter_query = pre_filter_query.filter(
        func.similarity(func.lower(name_column), func.lower(search_term)) > 0.25
    ).params(search_term=search_term)

    # Get pre-filtered IDs
    candidate_ids = [item[0] for item in pre_filter_query.all()]

    if not candidate_ids:
        return []

    # Final query with Levenshtein only on pre-filtered results
    final_query = (
        session.query(
            name_column,
            func.levenshtein(normalized_name, normalized_search).label("distance"),
        )
        .filter(id_column.in_(candidate_ids))
        .filter(func.levenshtein(normalized_name, normalized_search) <= max_distance)
        .params(search_term=search_term)
        .order_by(text("distance"))
    )

    return final_query.all()


# Example usage functions for each model type
def fuzzy_search_projects(session, search_term, max_distance_percent=0.15):
    from datamodels.digicher.entities import Projects

    return fuzzy_search(
        session, Projects.id, Projects.title, search_term, max_distance_percent
    )


def fuzzy_search_institutions(session, search_term, max_distance_percent=0.15):
    from datamodels.digicher.entities import Institutions

    return fuzzy_search(
        session, Institutions.id, Institutions.name, search_term, max_distance_percent
    )


def fuzzy_search_research_outputs(session, search_term, max_distance_percent=0.15):
    from datamodels.digicher.entities import ResearchOutputs

    return fuzzy_search(
        session,
        ResearchOutputs.id,
        ResearchOutputs.title,
        search_term,
        max_distance_percent,
    )


def fuzzy_search_funding_programmes(session, search_term, max_distance_percent=0.15):
    from datamodels.digicher.entities import FundingProgrammes

    return fuzzy_search(
        session,
        FundingProgrammes.id,
        FundingProgrammes.title,
        search_term,
        max_distance_percent,
    )
