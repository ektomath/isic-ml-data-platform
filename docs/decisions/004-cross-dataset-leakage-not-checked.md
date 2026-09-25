# 004. Cross-dataset patient/lesion leakage is not checked (duplicate images are)

Status: accepted.

## Summary

Leakage groups keep images of the same patient, the same lesion, or the exact same image in a single split. They're built separately for each dataset, because patient and lesion IDs issued by different sources can't be compared. Exact duplicate images across datasets are caught by checksum before a combined training release is built. The same patient appearing in two datasets under different IDs is not detected, and that's a documented limitation.

## Context

Once a training release combines ISIC 2019 and MILK10k, the same patient, lesion or image could appear in both. If the two copies land in different splits, test results are inflated.

## Decision

- **Leakage grouping stays per dataset.** `group_id` includes the dataset key, so matching ID strings from two datasets never merge into one group. Each source issues its own `patient_id` and `lesion_id`, and nothing shows they share a namespace. A coincidental match would wrongly join unrelated images, and a non-match proves nothing.
- **Exact duplicates are checked across datasets.** A checksum is a property of the image itself, not a label a dataset assigns, so a match is a sound signal. Before sampling, the Gold manifest notebook calls `assert_no_cross_dataset_duplicate_checksums`, which fails the run if any image checksum appears in more than one of the release's datasets.

## Alternatives considered

- **Merge groups on matching IDs across datasets.** Rejected as unsound, for the reason above.
- **Detect the same patient or lesion across datasets.** This would need an authoritative cross-dataset patient registry or near-duplicate image matching, and neither exists here.

## Consequences

- The pipeline assumes, without verifying, that onboarded datasets don't share patients or lesions. The same lesion photographed twice, or the same image re-encoded so its bytes differ, isn't caught.
