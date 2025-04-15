import requests
import pandas as pd
from typing import Optional, Dict, Any
import datetime
import os
import sys
import signal
import traceback
import time

# Debug flag to limit API calls during testing
DEBUG = True
DEBUG_LIMIT = 2

# API configuration
API_KEY = "34bb7772ddfe0f3898308ee07f7499cc"
BASE_URL = "https://proxy.opendatagermany.io/api"

# Dataset dictionary
FILTER_DS_DICT = {
    "Cave or Mine": "https://semantify.it/ds/DStmDugCfIoc",
    "Food Establishment": "https://semantify.it/ds/zmoYZEMoSAKS",
    "Landmarks or Historical Buildings": "https://semantify.it/ds/nFVMlquREalK",
    "Local Business": "https://semantify.it/ds/dXppPkIfxyxA",
    "Lodging Business": "https://semantify.it/ds/RIExHpTXyEAD",
    "Museum": "https://semantify.it/ds/CFdoUqQAkeBa",
    "Nature Attraction": "https://semantify.it/ds/IJetdtllQtvS",
    "Tourist Attraction": "https://semantify.it/ds/pxNgHzzhpeUu",
    "Tourist Information Center": "https://semantify.it/ds/wNrMWBpNQYJm",
}

# Pagination parameters
PAGE_SIZE = 10

# Headers
headers = {
    "X-API-KEY": API_KEY,
    "accept": "application/json",
    "content-type": "application/ld+json",
}

# Global variable to track the CSV filename
csv_filename = ""


# Signal handler for graceful exit
def signal_handler(sig, frame):
    print("\nProcess interrupted by user. Data saved up to this point.")
    sys.exit(0)


# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)


def get_things(
    filter_ds,
    filter_ds_list="",
    do_type_count=False,
    page=0,
    page_size=PAGE_SIZE,
    max_retries=3,
    initial_backoff=5,
):
    for retry_count in range(max_retries + 1):
        try:
            url = f"{BASE_URL}/ts/v2/kg/things"

            params = {
                "doTypeCount": str(do_type_count).lower(),
                "filterDs": filter_ds,
                "filterDsList": filter_ds_list,
            }

            # Add pagination headers
            request_headers = headers.copy()
            request_headers.update({"page": str(page), "page-size": str(page_size)})

            # Make the request
            response = requests.get(url, headers=request_headers, params=params)

            # Check if the request was successful
            if response.status_code == 200:
                return response.json()
            else:
                # If we have retries left
                if retry_count < max_retries:
                    backoff_time = initial_backoff * (
                        2**retry_count
                    )  # Exponential backoff
                    print(
                        f"Error: {response.status_code}. Retrying in {backoff_time} seconds..."
                    )
                    print(response.text)
                    time.sleep(backoff_time)
                else:
                    print(f"Error: {response.status_code}. Maximum retries reached.")
                    print(response.text)
                    return None
        except Exception as e:
            # If we have retries left
            if retry_count < max_retries:
                backoff_time = initial_backoff * (2**retry_count)
                print(
                    f"Error in get_things: {str(e)}. Retrying in {backoff_time} seconds..."
                )
                time.sleep(backoff_time)
            else:
                print(f"Error in get_things: {str(e)}. Maximum retries reached.")
                return None

    return None


def get_thing_details(
    namespace, thing_id, sleep_for=0.5, max_retries=3, initial_backoff=5
):
    time.sleep(sleep_for)
    for retry_count in range(max_retries + 1):
        try:
            url = f"{BASE_URL}/ts/v1/kg/things/{thing_id}"
            params = {"ns": namespace}

            # Make the request
            response = requests.get(url, headers=headers, params=params)

            # Check if the request was successful
            if response.status_code == 200:
                return response.json()
            else:
                # If we have retries left
                if retry_count < max_retries:
                    backoff_time = initial_backoff * (
                        2**retry_count
                    )  # Exponential backoff
                    print(
                        f"Error: {response.status_code}. Retrying in {backoff_time} seconds..."
                    )
                    print(response.text)
                    time.sleep(backoff_time)
                else:
                    print(f"Error: {response.status_code}. Maximum retries reached.")
                    print(response.text)
                    return None
        except Exception as e:
            # If we have retries left
            if retry_count < max_retries:
                backoff_time = initial_backoff * (2**retry_count)
                print(
                    f"Error in get_thing_details: {str(e)}. Retrying in {backoff_time} seconds..."
                )
                time.sleep(backoff_time)
            else:
                print(f"Error in get_thing_details: {str(e)}. Maximum retries reached.")
                return None

    return None


# Function to get state from Mapbox
def get_state_from_address(
    address_street: Optional[str] = None,
    address_locality: Optional[str] = None,
    address_postal: Optional[str] = None,
    address_region: Optional[str] = None,
    address_country: Optional[str] = None,
    mapbox_access_token: str = None,
) -> Dict[str, Any]:
    try:
        if mapbox_access_token is None:
            return {
                "status": "error",
                "state": None,
                "region_code": None,
            }

        # Build the query parameters
        query_parts = []

        # Add non-null address components to the query
        if address_street:
            query_parts.append(address_street)
        if address_locality:
            query_parts.append(address_locality)
        if address_postal:
            query_parts.append(address_postal)
        if address_region:
            query_parts.append(address_region)
        if address_country:
            query_parts.append(address_country)

        # If no valid parts, return an error
        if not query_parts:
            return {
                "status": "error",
                "state": None,
                "region_code": None,
            }

        # Join the parts to create the search query
        query = " ".join(query_parts)

        # Prepare API request parameters
        params = {
            "q": query,
            "access_token": mapbox_access_token,
            "limit": 1,  # Only request a single result
        }

        # Add optional country code if provided to narrow results
        # No country codes here :(
        # if address_country:
        #     params["country"] = address_country

        # Mapbox Search Box API endpoint
        url = "https://api.mapbox.com/search/searchbox/v1/forward"

        # Make the API request
        response = requests.get(url, params=params)

        # Check if request was successful
        response.raise_for_status()

        # Parse the JSON response
        data = response.json()

        # If no features found
        if not data.get("features") or len(data["features"]) == 0:
            return {
                "status": "error",
                "state": None,
                "region_code": None,
            }

        # Extract the state from the first result
        first_result = data["features"][0]
        context = first_result.get("properties", {}).get("context", {})

        # Mapbox provides region information in the context.region object
        region_info = context.get("region", {})
        state = region_info.get("name") if region_info else None
        region_code = region_info.get("region_code") if region_info else None

        return {
            "status": "success",
            "state": state,
            "region_code": region_code,
        }

    except requests.exceptions.RequestException as e:
        print(f"API request error: {str(e)}")
        return {
            "status": "error",
            "state": None,
            "region_code": None,
        }
    except ValueError as e:
        print(f"JSON parsing error: {str(e)}")
        return {
            "status": "error",
            "state": None,
            "region_code": None,
        }
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {
            "status": "error",
            "state": None,
            "region_code": None,
        }


# Helper function to sanitize text for CSV
def sanitize_for_csv(text):
    if text is None:
        return None

    # Convert to string if not already
    if not isinstance(text, str):
        text = str(text)

    # Replace quotes with single quotes
    text = text.replace('"', "'")

    # Replace commas, semicolons and tabs with spaces
    text = text.replace(",", " ")
    text = text.replace(";", " ")
    text = text.replace("\t", " ")

    # Collapse multiple spaces into one
    import re

    text = re.sub(r"\s+", " ", text)

    return text.strip()


# Helper function to ensure value is a list
def ensure_list(value):
    if value is None:
        return []
    elif isinstance(value, list):
        return value
    elif isinstance(value, (tuple, set)):
        return list(value)
    else:
        return [value]


# Helper function to extract value from potentially complex schema.org fields
def extract_value(field):
    # If the field is None, return None
    if field is None:
        return None

    # If the field is a simple string or number, return it directly
    if isinstance(field, (str, int, float)) or not isinstance(field, (dict, list)):
        return field

    # If the field is a dictionary
    if isinstance(field, dict):
        # If it has a @value field, return that
        if "@value" in field:
            return field["@value"]
        # Otherwise return the whole dict as string
        return str(field)

    # If the field is a list
    if isinstance(field, list):
        # Try to find an English entry first
        for item in field:
            if (
                isinstance(item, dict)
                and item.get("@language") == "de"
                and "@value" in item
            ):
                return item["@value"]

        # If no English entry, try to find a German entry
        for item in field:
            if (
                isinstance(item, dict)
                and item.get("@language") == "en"
                and "@value" in item
            ):
                return item["@value"]

        # If no language-specific entry found, return the first value if available
        if field and isinstance(field[0], dict) and "@value" in field[0]:
            return field[0]["@value"]

        # As a last resort, return the whole list as string
        return str(field)

    # Default case
    return str(field)


# Function to save dataframe to CSV
def save_to_csv(df, append=True):
    global csv_filename

    if df.empty:
        return

    try:
        file_exists = os.path.isfile(csv_filename)
        if append and file_exists:
            df.to_csv(csv_filename, mode="a", header=not file_exists, index=False)
            print(f"Appended {len(df)} rows to {csv_filename}")
        else:
            df.to_csv(csv_filename, index=False)
            print(f"Created new file {csv_filename} with {len(df)} rows")
    except Exception as e:
        print(f"Error saving to CSV: {str(e)}")
        # Try to save to a backup file in case of error
        backup_file = f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        try:
            df.to_csv(backup_file, index=False)
            print(f"Saved backup to {backup_file}")
        except:
            print("Failed to save backup file")


# Main function to process all entities
def main():
    global csv_filename

    try:
        # Create timestamp for file names
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"entities_data_{timestamp}.csv"

        # Define columns for our dataframe
        columns = [
            "dataset_key",
            "namespace",
            "id",
            "name",
            "description",
            "url",
            "address_country",
            "address_locality",
            "address_postal",
            "address_street",
            "address_region",
            "address_geo",
            "mapbox_state",
            "mapbox_region_code",
        ]

        # Create empty CSV file with headers
        pd.DataFrame(columns=columns).to_csv(csv_filename, index=False)
        print(f"Created new CSV file: {csv_filename}")

        entity_count = 0

        # Loop through each dataset in the dictionary
        for dataset_key, dataset_url in FILTER_DS_DICT.items():
            print(f"\nProcessing dataset: {dataset_key} - {dataset_url}")

            current_page = 0
            has_more_pages = True

            # Paginate through all results
            while has_more_pages:
                print(f"Fetching page {current_page}...")

                # Create a dataframe for this page
                page_df = pd.DataFrame(columns=columns)

                try:
                    things_response = get_things(
                        filter_ds=dataset_url, page=current_page, page_size=PAGE_SIZE
                    )

                    if not things_response or not things_response.get("data"):
                        print("No data returned or error occurred")
                        break

                    # Get pagination information from metaData
                    metadata = things_response.get("metaData", {})
                    total_items = int(metadata.get("total", 0))
                    page_size = int(metadata.get("page-size", PAGE_SIZE))
                    current_page_from_api = int(metadata.get("current-page", 1))

                    print(
                        f"Total items: {total_items}, Page size: {page_size}, Current page: {current_page_from_api}"
                    )

                    # Check if we have more pages
                    has_more_pages = current_page_from_api * page_size < total_items

                    # Process entities on this page
                    for entity in things_response["data"]:
                        try:
                            # Split the ID to get namespace and ID parts
                            parts = entity["@id"].rsplit("/", 1)
                            namespace = parts[0]
                            entity_id = parts[1]

                            # Get detailed information for this entity
                            entity_details = get_thing_details(namespace, entity_id)

                            if entity_details and len(entity_details) > 0:
                                # Extract additional entity information
                                entity_data = entity_details[0]

                                # Get name, description, and URL
                                entity_name = sanitize_for_csv(
                                    extract_value(
                                        entity_data.get("https://schema.org/name")
                                    )
                                )
                                entity_description = sanitize_for_csv(
                                    extract_value(
                                        entity_data.get(
                                            "https://schema.org/description"
                                        )
                                    )
                                )
                                entity_url = sanitize_for_csv(
                                    extract_value(
                                        entity_data.get("https://schema.org/url")
                                    )
                                )

                                # Extract address information
                                address = entity_data.get(
                                    "https://schema.org/address", {}
                                )

                                address_country = sanitize_for_csv(
                                    extract_value(
                                        address.get("https://schema.org/addressCountry")
                                    )
                                )
                                address_locality = sanitize_for_csv(
                                    extract_value(
                                        address.get(
                                            "https://schema.org/addressLocality"
                                        )
                                    )
                                )
                                address_postal = sanitize_for_csv(
                                    extract_value(
                                        address.get("https://schema.org/postalCode")
                                    )
                                )
                                address_street = sanitize_for_csv(
                                    extract_value(
                                        address.get("https://schema.org/streetAddress")
                                    )
                                )

                                # Handle address region which might have language tags
                                address_region_raw = address.get(
                                    "https://schema.org/addressRegion"
                                )
                                address_region = sanitize_for_csv(
                                    extract_value(address_region_raw)
                                )

                                address_geo = address.get(
                                    "https://schema.org/geo"
                                )

                                # Check if we need to get state from Mapbox
                                mapbox_state = None
                                mapbox_region_code = None

                                if address_region is None or (
                                    isinstance(address_region, str)
                                    and address_region.lower() != "thüringen"
                                ):
                                    mapbox_response = get_state_from_address(
                                        address_street,
                                        address_locality,
                                        address_postal,
                                        address_region,
                                        address_country,
                                        mapbox_access_token="pk.eyJ1IjoidmlldmF3YWxkaSIsImEiOiJjbTN5b3N6cjMxbHowMmxyM3pnNzZ2cHhoIn0.sPcqCspBDeVg8FbpV0-_zQ",
                                    )

                                    if mapbox_response["status"] == "success":
                                        mapbox_state = mapbox_response["state"]
                                        mapbox_region_code = mapbox_response[
                                            "region_code"
                                        ]

                                # Add data to page dataframe
                                new_row = {
                                    "dataset_key": dataset_key,
                                    "namespace": namespace,
                                    "id": entity_id,
                                    "name": entity_name,
                                    "description": entity_description,
                                    "url": entity_url,
                                    "address_country": address_country,
                                    "address_locality": address_locality,
                                    "address_postal": address_postal,
                                    "address_street": address_street,
                                    "address_region": address_region,
                                    "address_geo": address_geo,
                                    "mapbox_state": mapbox_state,
                                    "mapbox_region_code": mapbox_region_code,
                                }

                                page_df = pd.concat(
                                    [page_df, pd.DataFrame([new_row])],
                                    ignore_index=True,
                                )

                                entity_count += 1
                                print(
                                    f"Processed entity {entity_count}: {namespace}/{entity_id}"
                                )

                                # Check if we've reached the debug limit
                                if DEBUG and entity_count >= DEBUG_LIMIT:
                                    print(
                                        f"Debug mode: Stopping after {DEBUG_LIMIT} entities"
                                    )
                                    has_more_pages = False
                                    break

                        except Exception as e:
                            print(f"Error processing entity: {str(e)}")
                            traceback.print_exc()
                            continue  # Continue with next entity

                    # Save data for this page after processing all entities
                    if not page_df.empty:
                        save_to_csv(page_df, append=True)

                    # Move to next page if we're continuing
                    if has_more_pages:
                        current_page += 1

                    # Exit all loops if debug limit reached
                    if DEBUG and entity_count >= DEBUG_LIMIT:
                        break

                except Exception as e:
                    print(f"Error processing page {current_page}: {str(e)}")
                    traceback.print_exc()

                    # Save any data collected on this page before moving on
                    if not page_df.empty:
                        save_to_csv(page_df, append=True)

                    # Try to continue with next page
                    current_page += 1

                    # If we've had multiple consecutive errors, maybe break
                    # (This is optional - you can remove this if you want to keep trying)
                    if current_page > 3:  # arbitrary limit
                        print("Multiple errors encountered, moving to next dataset")
                        has_more_pages = False

            # Exit the dataset loop if debug limit reached
            if DEBUG and entity_count >= DEBUG_LIMIT:
                break

        # Print summary
        print("\nProcessing complete:")
        print(f"Total entities processed: {entity_count}")
        print(f"Data saved to: {csv_filename}")

        # Return the complete dataset
        if os.path.isfile(csv_filename):
            try:
                complete_df = pd.read_csv(csv_filename)
                print(f"CSV file contains {len(complete_df)} rows")
                return complete_df
            except Exception as e:
                print(f"Error reading complete CSV: {str(e)}")
                return pd.DataFrame()
        else:
            return pd.DataFrame()

    except Exception as e:
        print(f"Unexpected error in main function: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame()


# Run the main function
if __name__ == "__main__":
    try:
        df = main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Data saved up to the last completed page.")
    except Exception as e:
        print(f"Unhandled exception: {str(e)}")
        traceback.print_exc()
