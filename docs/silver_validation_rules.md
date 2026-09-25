# Silver Validation Rules

Per-dataset image validation criteria used by `data_platform.validate.decode_image`/`decode_batch` via `data_platform.spark_io.validate_images`. Defaults live in `data_platform/validate.py`; a dataset only needs an entry here if its images are a structurally different kind of thing and it overrides them.

A living reference, not a point-in-time decision record — update it whenever a dataset's criteria change. Criteria are allowed to differ by dataset because source datasets differ structurally (for example, in image size ranges); defaults live in `data_platform/validate.py`.

## Defaults (`data_platform/validate.py`)

| Rule | Default | Meaning |
|---|---|---|
| `MIN_DIMENSION` | 50px | Images narrower or shorter than this are rejected as likely corrupted or degenerate. |
| `MAX_DIMENSION` | 15000px | Images wider or taller than this are rejected as likely corrupted, or the wrong kind of file for this pipeline. |

## `isic_2019`

Uses the defaults above, unmodified.

- **Rationale**: sanity guard-rails for close-up dermoscopy photography, not derived from a measured percentile of the real dimension distribution. Reasonable for macro clinical/dermoscopic photos; would need reconsidering for a structurally different domain — whole-slide histopathology scans, for example, routinely exceed 15,000px as completely normal, not corrupted, and would need a much larger (or no) `max_dimension`.
- A near-uniform-color rejection check was tried and removed — not a reliable corruption signal here (a valid image can legitimately have low pixel variance, e.g. lots of dark background).

## `milk10k`

Uses the defaults above, unmodified — same photography domain as `isic_2019`, no evidence yet of a different dimension distribution. Revisit once the dataset has actually run through Silver and real dimension data exists.

## Adding a new dataset

If a dataset's images are structurally different (different capture device, expected size range, or corruption signal), override `min_dimension`/`max_dimension` when calling `validate_images(...)` from its Silver notebook rather than editing the shared defaults, and add an entry here explaining what changed and why — so "how strict is validation for dataset X" stays a one-line lookup, not a code-reading exercise.
