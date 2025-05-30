import datetime
import logging
import os
from concurrent.futures import as_completed, ProcessPoolExecutor
from pathlib import Path
from typing import List, Tuple, Set

import numpy as np
import pandas as pd
import psutil
import spacy
from pandas import DataFrame

from enrichment.data_model import JResearchOutputTopicOpenalexKeywordDensity, \
    TopicOpenalexKeywordDensity, ResearchOutput
from enrichment.utils.batch_requester import BatchRequester
from lib.file_handling.file_utils import get_project_root_path
from lib.loader.create_db_session import create_db_session
from lib.loader.get_or_create import get_or_create
from utils.logger.logger import setup_logging

topic_keyword_map = {}
# max_workers = os.cpu_count()
max_workers = psutil.cpu_count(logical=False)  # Gets physical cores only
batch_size = 96

try:
    nlp = spacy.load("en_core_web_sm")
except OSError as e:
    raise Exception("Dont forget to download the dataset first: `python -m spacy download en_core_web_sm`.")


def run_topic_modelling(batch_requester: BatchRequester):
    for idx, batch in enumerate(batch_requester.next_batch()):
        # if idx > 0:
        #     return
        start = datetime.datetime.now()
        processed_batch = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_document, doc, topic_keyword_map)
                for doc in batch
            ]

            for future in as_completed(futures):
                doc_id, topic_id, score = future.result()
                processed_batch.append((doc_id, topic_id, score))
        logging.info(f"took {datetime.datetime.now() - start} seconds to process batch #{idx}")
        upload_topics_to_db(processed_batch)


def process_document(doc_data: Tuple[int, str], topic_keywords: dict) -> Tuple[int, int, int]:
    doc_id, full_text = doc_data
    process_id = os.getpid()
    logging.info(f"Process {process_id} processing doc {doc_id}")

    text = _normalise_text(full_text)
    topic_id, score = _assign_topic(text, topic_keywords)

    # del text ?

    return doc_id, topic_id, score


def _normalise_text(text: str) -> str:
    doc = nlp(text.lower()[:1000000])
    """
        ValueError: [E088] Text of length 1604351 exceeds maximum of 1000000.
    The parser and NER models require roughly 1GB of temporary memory per 100,000 characters in the input.
    This means long texts may cause memory allocation errors. If you're not using the parser or NER,
    it's probably safe to increase the `nlp.max_length` limit. The limit is in number of characters,
    so you can check whether your inputs are too long by checking `len(text)`.
    """
    tokens = [
        token.lemma_
        for token in doc
        if not token.is_stop
        and not token.is_punct
        and not token.is_space
        and token.is_alpha
    ]
    return " ".join(tokens)


def _assign_topic(text: str, topic_keywords: dict) -> Tuple[int, int]:
    best_topic, highest_score = -1, -1
    for topic_id, keywords in topic_keywords.items():
        score = _calculate_topic_significance_keyword_density(text, keywords)
        if score > highest_score:
            best_topic, highest_score = topic_id, score
    return best_topic, highest_score


def _calculate_topic_significance_keyword_density(normalized_text: str, topic_keywords: list) -> float:
    text_tokens = set(normalized_text.split())
    matches = sum(1 for keyword in topic_keywords if keyword in text_tokens)
    return matches / len(topic_keywords)


def upload_topics_to_db(batch: List[Tuple[int, int, int]]):
    with create_db_session()() as session:
        junction_records = [
            JResearchOutputTopicOpenalexKeywordDensity(
                researchoutput_id=doc_id,
                topic_openalex_keyword_density_id=topic_id,
                score=score
            )
            for doc_id, topic_id, score in batch
        ]
        session.add_all(junction_records)
        session.commit()


def load_topics(do_seed=False):
    start = datetime.datetime.now()
    df = pd.read_csv(get_project_root_path() / 'data/topics/openalex_topic_mapping.csv')

    if do_seed:
        _seed_topics(df)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(normalise_topics, row)
            for _, row in df.iterrows()
        ]

        for future in as_completed(futures):
            topic_key, normalised_keywords = future.result()
            topic_keyword_map[topic_key] = normalised_keywords
    logging.info(f"took {datetime.datetime.now() - start} seconds to load the topics.")


def normalise_topics(row) -> Tuple[str, Set[str]]:
    keywords = row['keywords'].split(';')
    normalised_keywords = set()
    for keyword in keywords:
        normalized_keyword = _normalise_text(keyword.strip())
        if normalized_keyword:
            normalised_keywords.add(normalized_keyword)
    if not normalised_keywords:
        raise Exception("nope")
    return row['topic_id'], normalised_keywords


def _seed_topics(df: DataFrame):
    df = df.replace({np.nan: None})
    with create_db_session()() as session:
        for idx, row in df.iterrows():
            fields = {
                'id': row['topic_id'],
                'subfield_id': row['subfield_id'],
                'field_id': row['field_id'],
                'domain_id': row['domain_id'],

                'topic_name': row['topic_name'],
                'subfield_name': row['subfield_name'],
                'field_name': row['field_name'],
                'domain_name': row['domain_name'],

                'keywords': row['keywords'],
                'summary': row['summary'],
                'wikipedia_url': row['wikipedia_url'],
            }
            get_or_create(session, TopicOpenalexKeywordDensity, unique_key={"id": row['topic_id']}, **fields)
            if idx % 100 == 0:
                session.commit()
                session.expunge_all()
                print(f"Processed {idx} rows.")


"""
ToDo:
- increase spacy limit to process whole doc if that works
- disabe others using fat node while i let this run okay
- Should we keep not using get_or_create for creating the junctions for the topics? if we created one the batch requester should ignore it i think

Optimization:
- Bigger batch, less overhead
- Figure out what takes long, spacy, some few very big docs?
- Multi Node!
"""

"""
Mac M2:
- max_workers = 1, 82 seconds topics
- max_workers = 12, 12 seconds topics
- max_workers = 12, 1:30 seconds on avg per batch
"""

if __name__ == "__main__":
    setup_logging(Path(), "topic_modelling_open_alex_keyword_density")
    logging.info(f"Starting topic enrichment.")
    logging.info(f"Got {max_workers} CPUs available.")
    logging.info(f"Using batch size {batch_size}.")

    load_topics(do_seed=False)

    run_topic_modelling(BatchRequester(ResearchOutput))
