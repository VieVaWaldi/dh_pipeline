import datetime
import logging
from concurrent.futures import as_completed, ProcessPoolExecutor
from typing import List, Tuple

import numpy as np
import pandas as pd
import psutil
import spacy
from pandas import DataFrame
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import func

from elt.core_orm_model import (
    JResearchOutputTopicOpenalexKeywordDensity,
    TopicOpenalexKeywordDensity,
    ResearchOutput,
)
from enrichment.utils.batch_requester import BatchRequester
from lib.database.create_db_session import create_db_session
from lib.database.get_or_create import get_or_create
from lib.file_handling.path_utils import get_project_root_path
from utils.logger.logger import setup_logging

vectorizer = None
topic_vectors = None
topic_ids = None
max_workers = psutil.cpu_count(logical=False)
batch_size = 128
sample_size = 10000  # Documents to sample for tf idf vocabulary building

try:
    nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
except OSError as e:
    raise Exception(
        "Don't forget to download the dataset first: `python -m spacy download en_core_web_sm`."
    )


def run_topic_modelling_tfidf(batch_requester: BatchRequester, offset=0):
    if offset > 0:
        logging.info(f"Skipping {offset} rows.")

    for idx, batch in enumerate(batch_requester.next_ro_batch(offset_start=offset)):
        start = datetime.datetime.now()
        processed_batch = []

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    process_document_tfidf, doc, vectorizer, topic_vectors, topic_ids
                )
                for doc in batch
            ]

            for future in as_completed(futures):
                doc_id, topic_id, score = future.result()
                processed_batch.append((doc_id, topic_id, score))

        logging.info(
            f"Took {datetime.datetime.now() - start} seconds to process batch #{idx}"
        )
        upload_topics_to_db(processed_batch)


def process_document_tfidf(
    doc_data: Tuple[int, str], vectorizer_data, topic_vectors_data, topic_ids_list
) -> Tuple[int, int, float]:
    doc_id, full_text = doc_data
    normalized_text = _normalise_text(full_text)
    topic_id, score = _assign_topic_tfidf(
        normalized_text, vectorizer_data, topic_vectors_data, topic_ids_list
    )
    return doc_id, topic_id, float(score)


def _normalise_text(text: str) -> str:
    doc = nlp(text.lower()[:999_900])
    tokens = [
        token.lemma_
        for token in doc
        if not token.is_stop
        and not token.is_punct
        and not token.is_space
        and token.is_alpha
    ]
    return " ".join(tokens)


def _assign_topic_tfidf(
    normalized_text: str, vectorizer_data, topic_vectors_data, topic_ids_list
) -> Tuple[int, float]:
    if not normalized_text.strip():
        return topic_ids_list[0], 0.0

    # Transform document to TF-IDF vector
    doc_vector = vectorizer_data.transform([normalized_text])

    # Calculate cosine similarity with all topic vectors
    similarities = cosine_similarity(doc_vector, topic_vectors_data)[0]

    # Find best match
    best_idx = np.argmax(similarities)
    best_topic = topic_ids_list[best_idx]
    best_score = similarities[best_idx]

    return best_topic, best_score


def upload_topics_to_db(batch: List[Tuple[int, int, float]]):
    with create_db_session()() as session:
        junction_records = [
            JResearchOutputTopicOpenalexKeywordDensity(
                researchoutput_id=doc_id,
                topic_openalex_keyword_density_id=topic_id,
                score=score,
            )
            for doc_id, topic_id, score in batch
        ]
        session.add_all(junction_records)
        session.commit()


def load_topics_tfidf(do_seed=False):
    global vectorizer, topic_vectors, topic_ids

    start = datetime.datetime.now()
    logging.info("Loading topics and building TF-IDF vectorizer...")

    # Load topic data
    df = pd.read_csv(get_project_root_path() / "data/topics/openalex_topic_mapping.csv")

    if do_seed:
        _seed_topics(df)

    # Normalize topic keywords
    topic_texts = []
    topic_ids = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(normalise_topic_text, row) for _, row in df.iterrows()
        ]

        for future in as_completed(futures):
            topic_id, topic_text = future.result()
            topic_ids.append(topic_id)
            topic_texts.append(topic_text)

    logging.info(f"Normalized {len(topic_texts)} topics")

    # Get sample documents for vocabulary building
    sample_texts = get_random_sample_texts(sample_size)
    logging.info(f"Loaded {len(sample_texts)} sample documents")

    # Combine topic and sample texts for vocabulary
    all_texts = topic_texts + sample_texts

    # Build TF-IDF vectorizer
    logging.info("Building TF-IDF vectorizer...")
    vectorizer = TfidfVectorizer(
        lowercase=False,  # spaCy already handles this
        stop_words=None,  # spaCy already removes stop words
        max_features=5000,  # Limit vocabulary size for performance
        min_df=2,  # Ignore terms that appear in fewer than 2 documents
        max_df=0.95,  # Ignore terms that appear in more than 95% of documents
    )

    vectorizer.fit(all_texts)

    # Pre-compute topic vectors
    topic_vectors = vectorizer.transform(topic_texts)

    logging.info(f"Vocabulary size: {len(vectorizer.vocabulary_)}")
    logging.info(
        f"Took {datetime.datetime.now() - start} seconds to load topics and build vectorizer"
    )


def normalise_topic_text(row) -> Tuple[int, str]:
    keywords = row["keywords"].split(";")
    normalized_keywords = []

    for keyword in keywords:
        normalized_keyword = _normalise_text(keyword.strip())
        if normalized_keyword:
            normalized_keywords.append(normalized_keyword)

    if not normalized_keywords:
        # Fallback to topic name if no keywords normalize
        topic_text = _normalise_text(row["topic_name"])
        if not topic_text:
            raise Exception(
                f"No valid keywords or topic name for topic {row['topic_id']}"
            )
    else:
        topic_text = " ".join(normalized_keywords)

    return row["topic_id"], topic_text


def get_random_sample_texts(sample_size: int) -> List[str]:
    """Load a random sample of research output texts for vocabulary building."""
    logging.info(f"Loading {sample_size} random documents for vocabulary building...")

    with create_db_session()() as session:
        # Get random sample of documents
        sample_docs = (
            session.query(ResearchOutput.abstract, ResearchOutput.title)
            .filter(ResearchOutput.abstract.isnot(None))
            .order_by(func.random())
            .limit(sample_size)
            .all()
        )

        sample_texts = []
        for doc in sample_docs:
            # Combine title and abstract
            full_text = f"{doc.title or ''} {doc.abstract or ''}".strip()
            if full_text:
                normalized_text = _normalise_text(full_text)
                if normalized_text:
                    sample_texts.append(normalized_text)

        logging.info(f"Successfully normalized {len(sample_texts)} sample documents")
        return sample_texts


def _seed_topics(df: DataFrame):
    df = df.replace({np.nan: None})
    with create_db_session()() as session:
        for idx, row in df.iterrows():
            fields = {
                "id": row["topic_id"],
                "subfield_id": row["subfield_id"],
                "field_id": row["field_id"],
                "domain_id": row["domain_id"],
                "topic_name": row["topic_name"],
                "subfield_name": row["subfield_name"],
                "field_name": row["field_name"],
                "domain_name": row["domain_name"],
                "keywords": row["keywords"],
                "summary": row["summary"],
                "wikipedia_url": row["wikipedia_url"],
            }
            get_or_create(
                session,
                TopicOpenalexKeywordDensity,
                unique_key={"id": row["topic_id"]},
                **fields,
            )
            if idx % 100 == 0:
                session.commit()
                session.expunge_all()
                print(f"Processed {idx} rows.")
        session.commit()


"""

ToDo:
* Turn topic.score to float
* rename topic tables
* test run with one batch!

"""


if __name__ == "__main__":
    setup_logging("enrichment-topic_modelling", "tfidf-keyword_density")
    logging.info("Starting TF-IDF topic enrichment.")
    logging.info(f"Got {max_workers} CPUs available.")
    logging.info(f"Using batch size {batch_size}.")
    logging.info(f"Using sample size {sample_size} for vocabulary building.")

    load_topics_tfidf(do_seed=True)

    run_topic_modelling_tfidf(BatchRequester(ResearchOutput), offset=0)

