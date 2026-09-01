# Silver Validation Rules

Per-dataset image validation criteria used by `data_platform.validate.decode_image`/`decode_batch` via `data_platform.spark_io.validate_images`. Defaults live in `data_platform/validate.py`; a dataset's Silver notebook only needs an entry here if it overrides them, and only needs to actually override them if its images are a structurally different kind of thing.

This is a living reference, not a point-in-time decision record — update it whenever a dataset's validation criteria are set or changed. For *why* image validation criteria are allowed to differ by dataset at all (rather than one fixed global rule), see the "Silver contract" section of `AGENT.md`.

## Defaults (`data_platform/validate.py`)

| Rule | Default | Meaning |
|---|---|---|
| `MIN_DIMENSION` | 50px | Images narrower or shorter than this are rejected as likely corrupted or degenerate. |
| `MAX_DIMENSION` | 15000px | Images wider or taller than this are rejected as likely corrupted, or the wrong kind of file for this pipeline. |

## `isic_2019`

Uses the defaults above, unmodified.

- **Rationale**: sanity guard-rails for close-up dermoscopy photography, not derived from a measured percentile of the real ISIC 2019 dimension distribution. Reasonable for macro clinical/dermoscopic photos; would need reconsidering for a structurally different image domain — for example, whole-slide histopathology scans routinely exceed 15,000px on a side as completely normal, not corrupted, and would need a much larger (or no) `max_dimension` override.
- A near-uniform-color rejection check was tried and removed — not trusted as a reliable corruption signal for this data (a valid image can legitimately have low pixel variance depending on domain, e.g. lots of dark background). See `AGENT.md`.

## `milk10k`

Uses the defaults above, unmodified — same dermoscopic/clinical photography domain as `isic_2019`, and no evidence yet of a structurally different dimension distribution. Revisit once the dataset has actually been run through Silver and real dimension data is available.

## Adding a new dataset

If a dataset's images are a structurally different kind of thing (different capture device, different expected size range, a different signal for "this looks wrong"), override `min_dimension`/`max_dimension` when calling `validate_images(...)` from that dataset's Silver notebook rather than editing the shared defaults, and add an entry here explaining what was changed and why — so "how strict is validation for dataset X" stays a one-line lookup instead of a code-reading exercise.
