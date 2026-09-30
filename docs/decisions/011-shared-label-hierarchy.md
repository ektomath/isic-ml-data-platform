# 011. Combine labels across datasets through one shared hierarchy; each Gold release picks a level

Status: accepted, not yet implemented. What exists today is described under Consequences.

## Summary

Datasets label images at different levels of detail, and the right level for a training release depends on which datasets it combines. Rather than a Silver label column for every possible combination, each dataset maps its labels once into one shared diagnosis hierarchy, stored in Silver as one column per level. A Gold release then chooses the level to train on, and any level every included dataset fills is usable, with finer labels rolling up to their ancestors. The cost is a one-off mapping per dataset into the shared hierarchy, and more label columns in Silver.

## Context

A Gold release can combine several datasets, and it needs one label with one meaning across all of them. Two datasets can describe the same lesion at different depths: one might only say "malignant, melanocytic", another "malignant, melanocytic, melanoma, invasive". Which depth a release can use depends on the datasets in it, and new combinations appear whenever a release is defined.

The ISIC Archive already publishes a shared hierarchical diagnosis taxonomy, which is what both onboarded datasets use (`diagnosis_1` to `diagnosis_N`, five levels in ISIC 2019 and four in MILK10k).

## Decision

- **One shared hierarchy in Silver.** `silver.image_inventory` holds the diagnosis as one nullable column per level, from coarsest to finest. The ISIC Archive taxonomy is the shared hierarchy, because both current datasets already use it.
- **One mapping per dataset.** Each dataset's Silver step maps its raw labels into that hierarchy down to the finest level it actually provides, and leaves deeper levels empty. A dataset outside the ISIC family supplies its own mapping function into the ISIC taxonomy, and allowed values at each level are enforced the same way `malignancy`'s are today.
- **Gold picks the level.** The manifest config names a level (for example `label_level: 3`) in place of a fixed label column. Building the release checks that every included image has a value at that level and fails otherwise. The chosen level is recorded with the release's config, seeds and git commit.

## Alternatives considered

- **A Silver label column for each combination or use case.** The number of columns grows with the combinations, and every new release could need a schema change.
- **Unify labels when metadata is encoded for a model ([ADR 009](009-metadata-features-wait-for-a-model.md)).** That encoding produces model input features, not training targets, so it's the wrong place to collapse a label hierarchy.
- **Map labels per Gold release.** It's flexible, but the same dataset would be mapped again and differently in every release, and releases would stop being comparable.

## Consequences

- Today, Silver still stores `malignancy` (level 1 of the ISIC taxonomy, with an enforced vocabulary) and `specific_diagnosis` (the deepest value, free text), and the `sample-v1` release trains on `malignancy`. That's enough while both datasets share the ISIC taxonomy and releases train at level 1.
- Build this when the first dataset arrives whose labels don't fit the ISIC taxonomy directly, or when a release needs a level finer than `malignancy`. It needs a Silver rerun on Databricks for every dataset, so it's best done together with recording the code commit on Silver rows, which also only fills in when Silver runs.
- A shared taxonomy has to be curated: a new dataset whose categories don't fit it means extending the taxonomy, not just adding a mapping.
