import psutil
import os
import statistics
import gc
from collections import defaultdict
from pathlib import Path
from typing import Dict, Any, Tuple, List

from utils.config_loader import get_config
from utils.web_requests import get_base_url
from utils.xml_parser import get_all_elements_as_dict

def print_total_number_weblinks(path: str):
    link_elements = get_all_elements_as_dict(
        path,
        "webLink",
    )
    eu_links = [
        dic
        for dic in link_elements
        if get_base_url(dic["physUrl"]["text"]) == "europa.eu"
    ]
    print(
        f"Total number of all eu links and all weblinks {len(eu_links)} / {len(link_elements)} in {path}"
    )

def get_count_of_each_xml_tag(path: Path, batch_size: int = 1000):
    """
    Counts all tags and subtags in all xmls given the path, calculates average text size,
    prints the results, and saves them to a file. Processes files in batches.
    """
    config = get_config()
    analysis_path = Path(config['analysis_path'])
    analysis_path.mkdir(parents=True, exist_ok=True)
    output_file = analysis_path / 'tag_analysis.txt'
    intermediate_file = analysis_path / 'intermediate_results.txt'

    print("Before processing:")
    _print_memory_usage()

    all_files = list(path.rglob('*.xml'))
    total_files = len(all_files)
    global_tag_counts = defaultdict(int)
    global_tag_text_sizes = defaultdict(list)

    for i in range(0, total_files, batch_size):
        batch = all_files[i:i+batch_size]
        process_batch(batch, global_tag_counts, global_tag_text_sizes, intermediate_file)
        print(f"Processed {min(i+batch_size, total_files)}/{total_files} files:")
        _print_memory_usage()
        gc.collect()

    print("Before writing final results:")
    _print_memory_usage()

    with open(output_file, 'w') as f:
        for tag, count in global_tag_counts.items():
            avg_size = statistics.mean(global_tag_text_sizes[tag]) if global_tag_text_sizes[tag] else 0
            output = f"{tag}: Count = {count}, Average Text Size = {avg_size:.2f}"
            print(output)
            f.write(output + '\n')

    print("After writing final results:")
    _print_memory_usage()

    print(f"Analysis results saved to {output_file}")
    os.remove(intermediate_file)  # Remove the intermediate file

def process_batch(files: List[Path], global_tag_counts: Dict[str, int], 
                  global_tag_text_sizes: Dict[str, List[int]], intermediate_file: Path):
    batch_tag_counts = defaultdict(int)
    batch_tag_text_sizes = defaultdict(list)

    for file in files:
        elements = get_all_elements_as_dict(file, "project")
        for element in elements:
            _count_tags_and_text_sizes(element, counts=batch_tag_counts, text_sizes=batch_tag_text_sizes)

    # Update global counts and sizes
    for tag, count in batch_tag_counts.items():
        global_tag_counts[tag] += count
        global_tag_text_sizes[tag].extend(batch_tag_text_sizes[tag])

    # Write intermediate results
    with open(intermediate_file, 'a') as f:
        for tag, count in batch_tag_counts.items():
            avg_size = statistics.mean(batch_tag_text_sizes[tag]) if batch_tag_text_sizes[tag] else 0
            f.write(f"{tag}: Count = {count}, Average Text Size = {avg_size:.2f}\n")

def _count_tags_and_text_sizes(
    data: Dict[str, Any],
    parent: str = None,
    counts: Dict[str, int] = None,
    text_sizes: Dict[str, list] = None,
) -> Tuple[Dict[str, int], Dict[str, list]]:
    if counts is None:
        counts = defaultdict(int)
    if text_sizes is None:
        text_sizes = defaultdict(list)

    for key, value in data.items():
        tag = f"{parent}.{key}" if parent else key
        counts[tag] += 1

        if isinstance(value, dict):
            if "text" in value:
                text_size = len(value["text"]) if value["text"] else 0
                text_sizes[tag].append(text_size)
            _count_tags_and_text_sizes(value, tag, counts, text_sizes)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _count_tags_and_text_sizes(item, tag, counts, text_sizes)

    return counts, text_sizes

def _print_memory_usage():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    print(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")

def main():
    config = get_config()
    pile_dir = (
        Path(config["data_path"]) / "extractors" / "cordis_contenttype=projectANDSTAR"
    )
    get_count_of_each_xml_tag(pile_dir)

if __name__ == "__main__":
    main()