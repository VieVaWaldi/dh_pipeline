# OpenAire Model

This document explains what entities and columns to ingest from the OpenAIRE dump and how to load them into DuckDB.

---

## Entities to load

| Entity | Rows | Notes |
|---|---|---|
| `organization` | 448,161 | Flat, fast |
| `project` | 3,673,360 | Small |
| `publication` | ~165M | Largest entity |
| `relation` | ~2B (raw) → filtered | See filter rules below |

## Entities to ignore

| Entity | Notes |
|---|---|
| `communities_infrastructures` | Not relevant |
| `dataset` | Excluded for now, may revisit |
| `datasource` | Not needed |
| `otherresearchproduct` | Not needed |
| `software` | Not needed |

---

## DuckDB target

**Single file: `openaire.duckdb`** — all four tables live in one DuckDB file.
Cross-entity joins (publications ↔ relations ↔ organizations ↔ projects) require a single file to use DuckDB's query optimizer. Separate files would require ATTACH and complicate joins.

Additionally, a flat lookup table `organization_pids` is derived from `organization.pids` (see below).

---

## Column selection

### Keep all columns for all entities

We do not drop columns at load time. It is easier to drop data later after inspecting it than to re-run the load.
Exception: the 9 sparse JSON fields at the bottom of the publication schema that are near-null for publication-type records are likely wasteful, but are kept for now.

---

## Nested data strategy

DuckDB stores STRUCTs natively and supports dot-notation queries (e.g. `language.code`, `indicators.citationImpact.citationCount`). No need to unnest at load time.

**One exception: `organization.pids` → separate `organization_pids` table.**

This is the ROR merge key (step 4 in the pipeline). Querying a nested array for a specific scheme value on every join is painful. A flat lookup table is much cleaner.

### Tested unnesting query (DuckDB 1.2.2, live-tested on part-00000)

```sql
SELECT org.id, org.legalName, p.scheme, p.value
FROM read_json('.../organization/*.json.gz', format='newline_delimited', compression='gzip'),
UNNEST(pids) AS t(p)
```

Works correctly. Scheme distribution in part-00000 (3,869 orgs):

| scheme | count |
|---|---|
| ROR | 1,365 |
| GRID | 1,239 |
| PIC | 760 |
| RNSR | 734 |
| ISNI | 710 |
| Wikidata | 675 |
| mag_id | 379 |
| FundRef | 256 |
| OrgRef | 191 |
| ... | ... |

**Important:** ROR values are stored as full URLs: `https://ror.org/00mesrk97`.
When merging with ROR data in step 4, strip the prefix to get the bare ID (`00mesrk97`), or normalize both sides to the same format.

### organization_pids table schema

```sql
CREATE TABLE organization_pids AS
SELECT org.id AS org_id, p.scheme, p.value
FROM read_json('.../organization/*.json.gz', format='newline_delimited', compression='gzip'),
UNNEST(pids) AS t(p)
WHERE pids IS NOT NULL
```

---

## Relation filter rules

The raw `relation` directory has ~2B rows. We filter to only what is needed.

### Step 1: filter by relation type

```sql
WHERE relType.type IN ('affiliation', 'outcome', 'citation', 'participation')
```

This excludes `provision` (product↔datasource, dominant by volume) and `similarity` (top-N similar docs).

Relation types kept:

| relType.type | relType.name examples | Entities |
|---|---|---|
| `affiliation` | hasAuthorInstitution, isAuthorInstitutionOf | product ↔ organization |
| `outcome` | produces, isProducedBy | project ↔ product |
| `citation` | Cites, IsCitedBy | product ↔ product |
| `participation` | isParticipant | organization ↔ project |

### Step 2: filter product-side to publications only

The `relation` table uses `sourceType = 'product'` / `targetType = 'product'` for all research output types (publications, datasets, software, other). It does not carry the publication `type` field.

**We cannot filter to publication-only products during the initial relation scan.** The publication IDs are needed first.

**Loading order:**
1. `organization`
2. `project`
3. `publication` → after load, the publication ID set is available in DuckDB
4. `relation` → filter by relType.type (step 1) + semi-join against publication IDs

```sql
-- Step 4: load relations filtered to publication products only
CREATE TABLE relation AS
SELECT r.*
FROM read_json('.../relation/*.json.gz', format='newline_delimited', compression='gzip') r
WHERE relType.type IN ('affiliation', 'outcome', 'citation', 'participation')
  AND (
    (r.sourceType = 'product' AND r.source IN (SELECT id FROM publication))
    OR (r.targetType = 'product' AND r.target IN (SELECT id FROM publication))
    OR (r.sourceType != 'product' AND r.targetType != 'product')  -- org↔project participation
  )
```

Note: `participation` (organization↔project) has no product side, so it passes through the type filter without needing the publication semi-join.

---

## Data quality notes

- **`publicationDate`**: values like `0001-12-30` exist. Load as VARCHAR, clean in step 3 (dedup/clean).
- **`dump uses 'product'`**: the relation table uses `product` as sourceType/targetType for research outputs; the API docs say `result`.
- **ROR URL format**: `pids.value` for scheme=ROR is a full URL (`https://ror.org/XXXXX`), not a bare ID.

---

## Open questions (carried over)

- [ ] Does `union_by_name=true` handle type conflicts across all 1500 publication parts?
- [ ] Are the 9 JSON-typed fields (codeRepositoryUrl, programmingLanguage, etc.) ever non-null in publication records?
- [ ] Memory required for the relation semi-join against 165M publication IDs — likely fine with 200GB RAM but verify.
- [ ] Wall-clock time for filtered relation load — schedule as dedicated Slurm job.
- [ ] What fraction of the ~2B relations pass the relType filter? Estimate before running.
