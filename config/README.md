# Config

Configure paths and database connection [here](config.json).

Configure sources and queries [here](config_queries.json). Each data source contains:

- **checkpoint:** The field in the raw data used to track extraction progress
- **queries:** List of search queries to execute
- **checkpoint_range:** Defines the extraction window size
- **checkpoint_start:** Starting point for initial extraction (if no checkpoint exists)