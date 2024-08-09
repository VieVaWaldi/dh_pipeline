import logging
from pathlib import Path
from extractors.non_contextual_transformations import trim_excessive_whitespace
import requests
import time
import shutil
from typing import Any, List, Dict 

import xml.etree.ElementTree as xml
from extractors.i_extractor import IExtractor  
from utils.file_handling import ensure_path_exists, load_file, write_file
from utils.logger import setup_logging, log_and_raise_exception

class ArxivExtractor(IExtractor):
    def __init__(self, extractor_name: str, checkpoint_name: str):
        super().__init__(extractor_name, checkpoint_name)
        self.processed_ids_file = self.data_path / 'processed_ids.txt'
        self.setup_paths()

    def setup_paths(self):
        ensure_path_exists(self.data_path)
        ensure_path_exists(self.logging_path)
        setup_logging(self.logging_path)

#read the last checkpoint value from a file.
    def restore_checkpoint(self) -> str:
        checkpoint_file = self.checkpoint_path
        if checkpoint_file.exists():
            return load_file(checkpoint_file).strip()
        return None

#passthrough method that does not modify the checkpoint value
    def create_next_checkpoint_end(self, next_checkpoint: str) -> str:
        return next_checkpoint

#Fetch checkpoint. Thus, query data from the last published date onward. 
#Track processed IDs to avoid duplicates
#Extracting data from the arXiv API (incl. pagination) until checkpoint or completion of extraction process
    def extract_until_next_checkpoint(self, query: str) -> None:
        last_published_date = self.restore_checkpoint()
        processed_ids = self.get_processed_ids()
        
        QUERY_TEMPLATE = (
            "search_query=all:computing+AND+(all:humanities+OR+all:heritage)"
            "&start={start}&max_results={max_results}&sortBy=lastUpdatedDate&sortOrder=descending"
        )

        if last_published_date:
            QUERY_TEMPLATE += f"&start_date={last_published_date}"

        start = 0
        total_results = float('inf')  # Start with an infinitely large number

#Pagination loop
#Extract Metdata and Entries in XML format
        while start < total_results:
            QUERY = QUERY_TEMPLATE.format(start=start, max_results=2000)
            URL = f"https://export.arxiv.org/api/query?{QUERY}"
            
            logging.info(f"Fetching results {start} to {start + 2000}...")
            xml_content = self.fetch_arxiv_data(URL)
            
            meta_data = self.print_arxiv_meta_data(xml_content)
            total_results = meta_data["total_results"]
            entries = self.extract_entries(xml_content)

#Check for new entries (not processed) with the help of IDS
#Save new entries
#Update the set of processed IDs 
#Rate Limiting: Fetch at once a batch of 2000 entries and add 3 seconds delay before fetching a new batch. 
#Log a completion message once all pages/entries have been processed.
            new_entries = []
            for entry in entries:
                entry_element = xml.fromstring(entry)
                entry_id = entry_element.find("{http://www.w3.org/2005/Atom}id").text
                if entry_id not in processed_ids:
                    new_entries.append(entry)
                    processed_ids.add(entry_id)

            if new_entries:
                self.save_entries_and_papers(new_entries)
                self.save_processed_ids(processed_ids)
            else:
                logging.info("No new entries found.")

            if start + 2000 < total_results:
                logging.info("Waiting for 3 seconds before the next iteration...")
                time.sleep(3)
            
            start += 2000

        logging.info(">>> Successfully finished extraction")

#HTTP GET request to fetch data from URL (arXiv API endpoint)
#Log HTTP errors 
    def fetch_arxiv_data(self, url: str) -> str:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            log_and_raise_exception(f"Error fetching data from {url}: {e}")

#Extract metadata as a dictionary: total_results, start_index, items_per_page
#Log any errors 
    def print_arxiv_meta_data(self, xml_content: str) -> Dict[str, Any]:
        try:
            root = xml.fromstring(xml_content)
            ns = {
                "atom": "http://www.w3.org/2005/Atom",
                "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
            }
            total_results = int(root.find("opensearch:totalResults", ns).text)
            start_index = int(root.find("opensearch:startIndex", ns).text)
            items_per_page = int(root.find("opensearch:itemsPerPage", ns).text)
            logging.info(f"Total Results: {total_results}")
            logging.info(f"Start Index: {start_index}")
            logging.info(f"Items Per Page: {items_per_page}")
            return {
                "total_results": total_results,
                "start_index": start_index,
                "items_per_page": items_per_page
            }
        except Exception as e:
            log_and_raise_exception(f"Error parsing metadata: {e}")

    #Parse XML content, extract individual entries and return them as a list of strings
    #Log any errors 
    def extract_entries(self, xml_content: str) -> List[str]:
        try:
            root = xml.fromstring(xml_content)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            entries = root.findall("atom:entry", ns)
            return [xml.tostring(entry, encoding="unicode") for entry in entries]
        except Exception as e:
            log_and_raise_exception(f"Error extracting entries: {e}")

    def save_entries_and_papers(self, entries: List[str]) -> None:
        for entry in entries:
            # Parse the XML entry
            try: 
                entry_element = xml.fromstring(entry) #Convert XML string into ElementTree object
                title = entry_element.find("{http://www.w3.org/2005/Atom}title").text #Extract title
                category = entry_element.find("{http://www.w3.org/2005/Atom}category").attrib.get('term', 'No Category') #Extract Category. If no category found: No category
                published_date = entry_element.find("{http://www.w3.org/2005/Atom}published").text #Extract published date
                safe_title = "".join([c if c.isalnum() else "_" for c in title]) #title: replace non-alphanumeric characters with underscores
                
                # Create a directory for an entry
                entry_dir = self.data_path / safe_title
                entry_dir.mkdir(parents=True, exist_ok=True) #Creates the directory if it doesn’t already exist.
                
                # Define the path for the XML file of the entry
                xml_file_path = entry_dir / f"entry_{safe_title}.xml"

                # Add the 'category_term' element to the XML file 
                category_element = xml.Element("category_term") #Creates a new XML element for the category term.
                category_element.text = category #Sets the text of the new category element
                entry_element.append(category_element) #Appends the new category element to the original XML entry.
                tree = xml.ElementTree(entry_element) #Creates an ElementTree object from the updated XML element.
                tree.write(xml_file_path, encoding="utf-8", xml_declaration=True) #Writes the XML data to the specified file path
                
                # Find and save the PDF file
                pdf_url = None # Initializes the PDF URL variable
                # Iterates over all link elements in the XML
                for link in entry_element.findall("{http://www.w3.org/2005/Atom}link"):
                    if link.attrib.get('title') == 'pdf': #Checks if the link’s title attribute is 'pdf'.
                        pdf_url = link.attrib['href'] # Extracts the URL if found.
                        break
                
                #IF found, download and save the PDF File
                if pdf_url:
                    pdf_file_path = entry_dir / f"paper_{safe_title}.pdf" #Defines the path for the PDF file
                    response = requests.get(pdf_url) #GET request to download the PDF.
                    response.raise_for_status() 
                    with pdf_file_path.open('wb') as f:
                        f.write(response.content)
                    logging.info(f"Saved {pdf_file_path}")
                
                # Update the checkpoint based on the published date of the latest entry
                self.save_checkpoint(published_date)
                
            except Exception as e:
                log_and_raise_exception(f"Error saving entries and papers to file: {e}")

#Manage a record of IDs to prevent duplication
#Update the file with new processed IDs
    def get_processed_ids(self) -> set:
        if self.processed_ids_file.exists():
            return {line.strip() for line in self.processed_ids_file.open('r')}
        return set()

    def save_processed_ids(self, ids: set) -> None:
        with self.processed_ids_file.open('a') as f:
            for id in ids:
                f.write(f"{id}\n")

    def save_extracted_data(self, data: str | Dict[str, Any]) -> Path:
        raise NotImplementedError

    #Trim excessive whitespace from the XML content
    def non_contextual_transformation(self, data_path: Path):
        #iteriate over each item in directoy data_path
        for entry_dir in Path(data_path).iterdir():
            if not entry_dir.is_dir():
                log_and_raise_exception(f"Expected directory but found {entry_dir}")

        #within each directory iterates over files to find xml suffix
        for file_path in entry_dir.iterdir():
            if not file_path.is_file() or file_path.suffix != ".xml":
                log_and_raise_exception(f"Found non-XML file: {file_path}")

            # Read and trim excessive whitespace from XML content
            xml_content_transformed = trim_excessive_whitespace(load_file(file_path))
            
            # Write the transformed XML content back to the file
            write_file(file_path, xml_content_transformed)

        # Remove the original directory after processing all files
        shutil.rmtree(entry_dir)

    def get_new_checkpoint(self) -> str:
        raise NotImplementedError

def main():
    extractor_name = 'arxiv'
    checkpoint_name = 'arxiv_checkpoint'
    
    # Define your query here or leave it empty for default behavior
    query = "search_query=all:computing+AND+(all:humanities+OR+all:heritage)"
    
    extractor = ArxivExtractor(extractor_name=extractor_name, checkpoint_name=checkpoint_name)
    extractor.extract_until_next_checkpoint(query=query)

if __name__ == "__main__":
    main()
    