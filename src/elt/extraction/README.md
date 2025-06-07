# Extraction

General information on extraction.

## Extractor Runner

- Config
- Should Continue

## Extractor Interface

- Parent of all extractors
- ...

> Should just explain the methods and interface

## Saving data

- Data has to be saved under `self.data_path` which is configured in `i_extractor.py`
- Each response gets its own folder, preferably named: `YYYY-MM-DD_HH-MM-sanitize(title)`
- Files are saved as `response_folder/sanitize(title)`
- Attachments are saved under `response_folder/atachments/sanitize(title)`
- When extracting the same data again, it will overwrite existing data if given the same name. This is also updates the
  file mtime which is important for the loader checkpoint

## Checkpoint

> Should continue extraction ? usually we want to extract until today (minus 1 day to start next extraction not with
> overlap),
> Or until a time in the future, eg for cordis where there is more data in the future

> Save checkpoint: for simple cases, when should not continue extraction, save last checkpoint, dont update it. so we
> get the later data next run.
> for cases like cordis we might want to store a checkpoint from n years ago to
> refetch updates for past projects.

Each extractor needs their custom checkpoint logic.

The relevant thoughts to make are:

- Is it date, total results etc. ?
- Do we need to update older data? (Loader checks file modification dates and gets updates)

## Sleep between requests

- Generally advised between pagination and next extraction
- A retry mechanism with exponential backoff is available for making requests, use as @decorator
