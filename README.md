# Reconciliation Dashboard dbt Package

A dbt package that builds month-level reconciliation outputs from Tuva core models and includes a local HTML viewer for CSV-based review.

## What this package builds

- `reconciliation__monthly_summary`
  - Grain: `data_source`, `payer`, `plan`, `year_month_int` (`YYYYMM`)
  - Upstream refs:
    - `{{ ref('core__medical_claim') }}`
    - `{{ ref('core__member_months') }}`
    - `{{ ref('reference_data__calendar') }}`

- `reconciliation__mapping_validity_monthly`
  - Grain: `data_source`, `payer`, `plan`, `field_name`, `year_month_int` (`YYYYMM`)
  - Upstream refs:
    - `{{ ref('input_layer__medical_claim') }}`
    - `{{ ref('reference_data__calendar') }}`
    - Tuva terminology refs (for code-set validation joins)
  - Tracks mapping status rates for claim-line fields against Tuva terminology sets.
  - Fields currently checked:
    - `drg_code`
    - `revenue_center_code`
    - `hcpcs_code`
    - `ndc_code`
    - `rendering_npi`
    - `billing_npi`
    - `facility_npi`
    - `diagnosis_code_1`
    - `diagnosis_code_2`
    - `diagnosis_code_3`
    - `procedure_code_1`
    - `procedure_code_2`
    - `procedure_code_3`
    - `discharge_disposition_code`
    - `admit_source_code`
    - `admit_type_code`
    - `bill_type_code`
    - `place_of_service_code`

## Install in a downstream dbt project

Add to downstream `packages.yml`:

```yml
packages:
  - git: "https://github.com/<your-org>/reconciliation_dashboard.git"
    revision: "master"
```

This package intentionally does not declare transitive dependencies to avoid version conflicts in client projects.
The downstream project must already include:

- Tuva package (or equivalent fork) that provides the referenced core/input/terminology models
- `dbt_utils` (used by model tests in this package)

Example:

```yml
packages:
  - package: tuva-health/the_tuva_project
    version: [">=0.15.0", "<1.0.0"]
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

Run:

```bash
dbt deps
dbt run -s reconciliation_dashboard
```

## Output schema

By default, models are created in:

- `data_quality.*`
- or `<tuva_schema_prefix>_data_quality.*` when `tuva_schema_prefix` is set.

## CSV export and local viewer

Open:

- `ui/reconciliation_dashboard.html`

It now has two tabs:

- `Reconciliation Summary` tab expects `reconciliation__monthly_summary` CSV.
- `Mapping Validity` tab expects `reconciliation__mapping_validity_monthly` CSV.

Both tabs support:

- CSV upload or paste
- Multi-select filters for `data_source`, `payer`, `plan`
- Month-level trend visuals
- Aggregated summary tables
- PHI-free summarized outputs only
