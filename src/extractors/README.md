# Extractors

For each extractor the interface `i_extractor` should be implemented.

It provides the following:

* Configures logging: Use `import logging` and call `logging.info()`.
* Member Variables for checkpoint and data paths.
    * `self.last_checkpoint`, reads and holds the last checkpoint value.
    * `self.checkpoint_path`, holds the path to the checkpoint.
    * `self.data_path`, holds the path to the data directory.
    * Each extracted record should have its own directory in `self.data_path`.
    * For further downloads create a directory called `attachments` in the record directory.

Furthermore, it enforces the following interface:

1. Create the end for the next checkpoint.
2. Extract data until next checkpoint.
3. Save the data.
4. Non contextually transform the data (This includes simple sanitization and attachment downloading).
5. Get the checkpoint.
6. Save the checkpoint.