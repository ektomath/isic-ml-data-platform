<!-- Generated from ISIC_2019_Data_Engineering_Project_Plan_v3.docx. -->
<!-- Keep the Word and Markdown plans synchronized when scope changes. -->

**Implementation playbook**

# ISIC 2019 Image Data Platform

*A showcase-first Bronze-Silver-Gold implementation plan*

| Field | Plan |
|---|---|
| Primary objective | Ingest, validate, version and publish the ISIC 2019 train + test archives (33,569 images) as reusable ML data products. |
| MVP finish line | Gold classifier manifest, one reproducible baseline, one workflow, fixture-based CI and a public case study. |
| Release 2 | Versioned super-resolution pairs and experiments, started only after the showcase MVP is published. |
| Reference stack | Databricks Unity Catalog (catalog + bronze/silver/gold schemas, Volumes for file storage) + Python + Delta Silver tables + Parquet Gold manifest + Databricks Jobs/MLflow + local or third-party GPU. |
| Plan date | 2026-08-18 (implementation sections last reconciled 2026-09-01) |

## 1. Project outcome and boundaries

> **Project Outcome:** A reviewer can trace a model run back to an immutable Gold manifest, a validated Silver inventory and the exact Bronze image checksums used to create it.

### Success criteria
- ~~A rerun of the Bronze backfill overwrites no verified source object.~~ Superseded by a deliberate tradeoff — Bronze ingestion writes directly to the production table and a rerun rebuilds it from scratch rather than skipping already-verified images; see the BRZ-002 deviation note in Phase 2 and `docs/decisions/002-store-bronze-images-as-delta-blobs.md`.
- Every image is reconciled to a label and has an accepted or rejected state.
- Train/validation/test splits keep patient, lesion and duplicate groups together where identifiers permit.
- Gold manifests are immutable, checksummed and tied to source, processing and Git versions.
- Training uses a local SSD cache and logs dataset lineage to MLflow.
- CI proves behaviour with fixtures and never downloads the full ISIC dataset.
- The project is described as research/education, not as a clinical diagnostic system.

### MVP versus extension

| MVP - finish before expanding | Extension - begin only after MVP release |
|---|---|
| Bronze source preservation, checksums and idempotency | Super-resolution eligibility, pair construction and shards |
| Silver validation, normalized metadata and exact duplicates | Optional preprocessing experiments after the baseline |
| Grouped Gold classifier split and immutable manifest | Perceptual near-duplicate review, if exact hashes are insufficient |
| One transfer-learning baseline with MLflow lineage | Incremental ingestion from newer ISIC collections |
| One workflow, fixture CI, run summary and public case study | Multi-environment CI/CD, advanced monitoring and managed alerts |

## 2. Target data architecture

ISIC source  ->  Bronze originals  ->  Silver validated inventory  ->  Gold model-ready manifests  ->  local/GPU training  ->  MLflow

| Layer | Purpose | Physical assets | Must not happen |
|---|---|---|---|
| Bronze | Preserve source faithfully | Original image bytes in Delta, CSV/JSON metadata, source manifest, ingestion ledger | Resize, recompress or overwrite source images |
| Silver | Validate and normalize | Delta inventory, labels, group IDs, quality and rejected records | Copy every JPEG merely to claim another layer |
| Gold | Publish for a specific consumer | Classifier manifest, preprocessing config, SR pair manifest/shards, dataset card | Change a published version in place |

### Preprocessing placement

| Transformation | Recommended placement | Reason |
|---|---|---|
| Decode and dimension checks | Silver | Consumer-independent quality validation |
| Metadata and label normalization | Silver | Creates a trustworthy canonical schema |
| Resize, crop and augmentation | Gold config / runtime | Model- and experiment-specific |
| ImageNet mean/std | Training and inference runtime | Produces model-specific floating-point tensors; storing them wastes space |
| SR degradation and patches | Gold SR product | Consumer-specific derived pairs |

## Phase 0. Scope the showcase and create the fixture project

**Phase Goal:** Lock a finishable classifier MVP and prove the project structure before using cloud resources.

> **Phase Gate:** A clean checkout runs the fixture pipeline locally and the README states the MVP, Release 2 and non-goals.

### Ordered tasks

#### SCP-001 Create the repository, scope and fixture pipeline

**Outcome:** A coherent portfolio project that can prove behaviour without downloading ISIC data.

**Depends On:** None

- Define the MVP as Bronze ingestion, Silver validation, one Gold classifier manifest, one baseline model and a public case study.
- Create pyproject.toml, src/data_platform, pipelines, tests, config, docs and notebooks directories.
- Cover valid, corrupt, duplicate and missing-label cases with synthetic fixtures — implemented as fixtures generated on the fly in tests (`tmp_path`, in-memory archives) rather than files checked into `tests/fixtures/`.
- List non-goals: clinical deployment, full-archive ingestion, real-time streaming, multi-environment CI/CD and super-resolution before Release 2.

**Acceptance evidence**
- A clean checkout installs and runs pytest.
- The fixture pipeline completes locally with no cloud credentials.
- README contains objective, MVP, Release 2 and non-goals.

## Phase 1. Create the minimum cloud foundation

**Phase Goal:** Provision only the storage and configuration required by the showcase.

> **Phase Gate:** The fixture pipeline writes to ADLS, structured run metrics are visible and no credential is stored in Git.

### Ordered tasks

#### FND-001 Provision storage and configure the pipeline

**Outcome:** One simple storage layout that works locally, in Databricks and from a training machine.

**Depends On:** SCP-001

- Use one Databricks Unity Catalog catalog (`derm_showcase_project`) with `bronze`/`silver`/`gold` schemas for tables, and Unity Catalog Volumes (`landing`, `files`) for archive/metadata file storage — Unity Catalog governance in place of a raw ADLS Gen2 container with layer prefixes.
- Use one pipeline identity or developer identity for ingestion; issue narrowly scoped read access to external GPU compute only when needed.
- Disable public blob access and configure one budget alert.
- Use Delta for Silver and operational tables, and publish the portable Gold training manifest as Parquet.
- Use environment-variable overrides and emit run_id, stage, counts, bytes, duration and status as structured logs.

**Acceptance evidence**
- The fixture pipeline writes Bronze and metadata outputs.
- Public access is disabled and no key or token is committed.
- One fixture run produces a unique run ID and stage metrics.

## Phase 2. Build the Bronze backfill

**Phase Goal:** Acquire the fixed ISIC 2019 release efficiently and preserve original source bytes.

> **Phase Gate:** All expected records are accounted for and a second run performs zero unnecessary downloads or overwrites.

### Ordered tasks

#### BRZ-001 Ingest official metadata and labels

**Outcome:** Immutable source metadata with checksums and an expected-image ledger.

**Depends On:** FND-001

- Download the official ISIC 2019 training metadata and ground truth before images.
- Record source URL, retrieval time, file size, SHA-256, source version, licence and attribution.
- Validate 25,331 unique labelled IDs across the expected eight training classes.

**Acceptance evidence**
- Metadata checksums remain stable across reruns.
- Unexpected labels, duplicate IDs and missing required columns fail the stage.

#### BRZ-002 Ingest original images and prove idempotency

**Outcome:** Original image bytes stored as stable Bronze table rows with concise rerun evidence.

**Depends On:** BRZ-001

- Use the official challenge archive for the bulk historical load (train + test archives).
- Stage each archive locally, calculate SHA-256 from local image bytes, and write image rows with batched Spark table writes.
- Record downloaded, skipped, failed, bytes and duration; never resize or recompress Bronze images.
- **Deviation from the original idempotency goal:** implemented as a direct write, not stage-then-atomically-replace — the first batch of a run overwrites `bronze.isic_2019_image_blobs` and every batch after that appends, so a rerun always rebuilds the whole table from scratch rather than skipping unchanged images. Accepted deliberately for this project's scale (archives are already staged to local disk, so a full rerun re-reads local disk and rewrites Delta rows, not the per-file Volume API calls that caused the original ingestion cost problem) — see `docs/decisions/002-store-bronze-images-as-delta-blobs.md`. Revisit if ingestion runs unattended or archive count grows.

**Acceptance evidence**
- Every expected ID is stored or has an explicit failure record.
- Every stored image has a checksum and source lineage.
- ~~The second run reports zero unnecessary downloads and zero overwrites.~~ Superseded — see the deviation note above. A rerun today fully rewrites the table rather than reporting zero overwrites; rerun/merge/dedup logic is instead covered by unit tests against fixture archives (`tests/test_files.py`), not by a real-data rerun.

## Phase 3. Build the trustworthy Silver inventory

**Phase Goal:** Validate and standardize the source without creating redundant image copies.

> **Phase Gate:** Every Bronze image has an accepted or rejected state and all Silver counts reconcile with the source ledger.

### Ordered tasks

#### SLV-001 Build the validated image inventory

**Outcome:** One typed Silver record per source image plus simple quality evidence.

**Depends On:** BRZ-001, BRZ-002

- Decode each JPEG and record width, height, channels, format, file size, checksum and Bronze URI.
- Normalize image IDs, diagnosis codes and missing values while preserving original source values.
- Join labels to inventory and identify corrupt files, orphan labels and unlabelled images.
- Write rejected records with image ID, error code, message and source path; block Gold publication when reconciliation fails.

**Acceptance evidence**
- Inventory count reconciles with Bronze.
- No accepted image lacks a valid label.
- A deliberately corrupt fixture blocks publication and appears in rejected records.

#### SLV-002 Create leakage-control groups

**Outcome:** Patient, lesion and exact-duplicate grouping fields for honest evaluation.

**Depends On:** SLV-001

- Group exact duplicates by SHA-256.
- Retain patient and lesion identifiers where supplied and report their completeness.
- Defer perceptual near-duplicate detection unless exact hashes and source identifiers prove insufficient.

**Acceptance evidence**
- Exact duplicate groups are materialized and reported.
- No grouping identifier is fabricated when the source does not provide one.

## Phase 4. Publish the Gold classifier product

**Phase Goal:** Give training code one immutable contract instead of exposing pipeline internals.

> **Phase Gate:** A manifest checksum identifies the exact images, labels and grouped split assignment.

### Ordered tasks

#### GLD-001 Generate grouped splits and publish the Gold manifest

**Outcome:** A deterministic, portable classifier dataset with source-to-model lineage.

**Depends On:** SLV-002

- Prioritize patient groups, then lesion groups, and always keep exact duplicates together.
- Stratify diagnosis distribution where grouping constraints permit.
- Publish image ID, Bronze URI, label, split, grouping fields and dataset version as Parquet.
- Record source version, Silver version, pipeline Git commit, class counts, exclusions and manifest SHA-256 in a concise dataset card.

**Acceptance evidence**
- No known group crosses splits.
- Two runs produce identical assignments and checksum.
- A clean environment can verify every referenced object.

#### GLD-002 Version preprocessing and synchronize a local cache

**Outcome:** Fast training with reproducible runtime transforms and no per-epoch cloud streaming.

**Depends On:** GLD-001

- Version resize, aspect-ratio handling, augmentation and ImageNet mean/std as model runtime configuration.
- Do not persist resized images or normalized tensors unless measured training performance later justifies a disposable cache.
- Synchronize manifest images to local SSD and verify checksums before training.

**Acceptance evidence**
- A second cache sync downloads zero unchanged files.
- Training works offline after synchronization.
- Training and inference import the same deterministic preprocessing implementation.

## Phase 5. Prove the product with one classifier

**Phase Goal:** Demonstrate Gold usability without turning the project into a broad model-search exercise.

> **Phase Gate:** One baseline experiment completes with dataset, preprocessing and Git lineage in MLflow.

### Ordered tasks

#### ML-001 Validate locally and train one baseline

**Outcome:** One reproducible model result tied to exact data and code versions.

**Depends On:** GLD-002

- Run one local CPU smoke epoch on a small stratified subset before paid GPU use.
- Fine-tune one modest pretrained classifier such as ResNet-18.
- Report balanced accuracy, per-class recall, confusion matrix and a concise error analysis.
- Log dataset version, manifest checksum, Git commit, preprocessing version, parameters and artifacts to MLflow.

**Acceptance evidence**
- One command reproduces the smoke run from a clean checkout.
- MLflow links the baseline to one immutable Gold version.
- The report discusses class imbalance, grouping limitations and medical-use limitations.

## Phase 6. Add visible operational proof

**Phase Goal:** Show production-minded behaviour without building an enterprise platform for a fixed dataset.

> **Phase Gate:** One workflow, one CI pipeline and one quality notebook provide the required operational evidence.

### Ordered tasks

#### OPS-001 Add one orchestrated workflow and fixture-based CI

**Outcome:** A visible pipeline DAG plus fast automated evidence for the important contracts.

**Depends On:** BRZ-002, SLV-001, GLD-001

- **Deviation:** implemented notebook-first instead of CLI-stages-plus-Databricks-Job — `ingest.py`/`publish.py` remain empty placeholder modules; the real Bronze/Silver logic lives in `src/data_platform/spark_io.py`, `files.py`, `validate.py` and the numbered notebooks (`notebooks/isic_2019/`), run manually in sequence. Orchestrating these as an actual Databricks Job/Workflow is still open (see `docs/project-checklist.md`, Automation section).
- GitHub Actions (`.github/workflows/ci.yml`) currently runs unit tests only (`pytest`); formatting/linting and an end-to-end fixture-pipeline run are not wired in yet.
- Cover corrupt image, missing label, idempotent rerun and deterministic split cases; never download the full dataset in CI. (Idempotent-rerun coverage is unit tests against fixture archives, not a real-data rerun — see the BRZ-002 deviation note above.)
- Do not create development, test and production deployment pipelines for the MVP.

**Acceptance evidence**
- A failed quality gate prevents Gold publication.
- ~~A successful rerun skips unchanged Bronze images.~~ Superseded — Bronze reruns fully rewrite the table by design; see BRZ-002.
- CI passes without cloud secrets and displays a passing badge.

#### OPS-002 Produce one quality and run-summary notebook

**Outcome:** A single reviewer-facing view of pipeline behaviour and dataset quality.

**Depends On:** OPS-001

- Show discovered, downloaded, skipped, rejected, bytes, duration and published version.
- Include image dimensions, diagnosis distribution, duplicate groups, split distribution and rejection reasons.
- Add a cache-hit summary if it is already available; do not build a separate dashboard.

**Acceptance evidence**
- The latest successful run is understandable from one notebook.
- Displayed counts reconcile across Bronze, Silver and Gold.

## Phase 7. Publish and verify the showcase

**Phase Goal:** Make the work understandable without requiring access to Azure or Databricks.

> **Phase Gate:** A logged-out reviewer can understand the outcome, inspect evidence and reproduce the fixture pipeline.

### Ordered tasks

#### SHOW-001 Publish and verify the portfolio case study

**Outcome:** A polished release that prioritizes evidence over additional infrastructure.

**Depends On:** ML-001, OPS-002

- Make the repository README the primary case study: problem, architecture, counts, idempotency, quality gates, grouped splits, caching, lineage and baseline results.
- Add architecture and quality screenshots; create a short walkthrough video only if it materially improves comprehension.
- Treat GitHub Pages as optional rather than a release dependency.
- Run the fixture pipeline from a clean checkout, verify public links while logged out, stop paid compute and tag the release.

**Acceptance evidence**
- The README communicates value before code navigation.
- All public links work while logged out and screenshots reveal no secrets.
- No paid compute remains running after demonstration.

## 3. Deferred work after the showcase release

> **Deferred Means Deferred:** These items may improve a later release, but none should delay the classifier MVP or public case study.

### Release 2: super-resolution

| Order | Release 2 work item | Completion evidence |
|---|---|---|
| 1 | Define minimum source resolution and reuse the grouped classifier split before extracting patches | No patient, lesion or duplicate group crosses SR splits |
| 2 | Version one deterministic x2/x4 degradation recipe | Pair ID reproduces the same LR/HR pixels from source ID, crop, seed and recipe |
| 3 | Publish a pair manifest and moderate-sized training shards | Shard checksums and member counts reconcile with the manifest |
| 4 | Run one modest SR baseline and inspect representative outputs | PSNR/SSIM, visual review and hallucination limitations are documented |
| 5 | Compare native-resize and SR-assisted classification | Both experiments use the same grouped source split and traceable dataset versions |

### Production details intentionally omitted from the MVP
- Separate development, test and production workspaces and deployment pipelines.
- Enterprise key rotation, elaborate identity topology and broad infrastructure-as-code coverage.
- Real-time ISIC notifications, streaming infrastructure and retraining triggers.
- Full monitoring and alerting stack with SLAs, paging and long-term operational support.
- Multiple orchestration systems, Kubernetes, feature stores or services not consumed by the project.
- Physical Silver copies of every JPEG or persisted ImageNet-normalized tensors.

## 4. Definition of Done

> **Rule:** A task is not Done because code exists. It is Done when its acceptance evidence is visible and repeatable.

### Definition of Done for every task
- Code is committed and readable.
- Relevant unit or integration tests pass.
- Rerun behaviour is understood and safe.
- Documentation or configuration is updated when behaviour changes.
- Acceptance evidence is captured in `docs/project-checklist.md` or the README.

### MVP release checklist

| Area | Release condition |
|---|---|
| Data | Bronze count reconciles; Silver quality gate passes; Gold manifest is immutable and checksummed |
| Security | No secrets in Git; trainer is read-only; public blob access disabled |
| Cost | Budget alerts active; no GPU/cluster left running; disposable volumes reviewed |
| Reproducibility | Fixture pipeline runs from clean checkout; split and manifest are deterministic |
| ML | Baseline metrics and limitations published; MLflow run includes dataset and code lineage |
| Portfolio | README, architecture, quality evidence, screenshots/video and logged-out links verified |
| Ethics | Research-only disclaimer, licence attribution and medical limitations are visible |

### Key risks and controls

| Risk | Control |
|---|---|
| Scope expands into SR before the showcase works | Release 2 remains blocked until Phase 7 and the MVP release checklist are complete |
| Network access starves GPU training | Manifest-driven local SSD cache; synchronize once and verify checksums |
| Patient/lesion leakage inflates results | Group-aware splits and duplicate-group assertions, scoped per dataset — cross-dataset leakage between onboarded datasets is not checked; see `docs/decisions/004-cross-dataset-leakage-not-checked.md` |
| Preprocessing cannot be reproduced | Versioned config, shared train/inference package and deterministic tests |
| Derived images obscure source lineage | Immutable Bronze originals and parent/output checksums for every physical variant |
| Medical results are overstated | Research-only framing, calibration/error analysis and explicit limitations |
| Cloud cost surprises | Budget alerts, short GPU sessions, lifecycle rules and shutdown checklist |

### Authoritative project sources
- [ISIC challenge datasets](https://challenge.isic-archive.com/data/)
- [ISIC 2019 challenge](https://challenge.isic-archive.com/landing/2019/)
- [ISIC API bulk-download guidance](https://api.isic-archive.com/api/docs/swagger/)
- [Official isic-cli](https://github.com/ImageMarkup/isic-cli)
- [ISIC AWS Open Data listing](https://registry.opendata.aws/isic-archive/)
