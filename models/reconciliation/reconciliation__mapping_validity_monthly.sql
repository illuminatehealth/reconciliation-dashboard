{{ config(
    enabled = var('reconciliation_dashboard_enabled', true) | as_bool
) }}

with medical_claim as (
    select
        data_source,
        payer,
        plan,
        cast(coalesce(claim_line_start_date, claim_start_date) as date) as service_date,
        claim_type,
        drg_code_type,
        drg_code,
        revenue_center_code,
        hcpcs_code,
        discharge_disposition_code,
        admit_source_code,
        admit_type_code,
        bill_type_code,
        place_of_service_code,
        rendering_npi,
        billing_npi,
        facility_npi,
        diagnosis_code_1,
        diagnosis_code_2,
        diagnosis_code_3,
        procedure_code_1,
        procedure_code_2,
        procedure_code_3
    from {{ ref('input_layer__medical_claim') }}
),

pharmacy_claim as (
    select
        data_source,
        payer,
        plan,
        cast(coalesce(dispensing_date, paid_date) as date) as service_date,
        ndc_code
    from {{ ref('input_layer__pharmacy_claim') }}
),

claim_with_month as (
    select
        mc.data_source,
        mc.payer,
        mc.plan,
        c.year_month_int,
        c.year_month,
        mc.claim_type,
        mc.drg_code_type,
        mc.drg_code,
        mc.revenue_center_code,
        mc.hcpcs_code,
        mc.discharge_disposition_code,
        mc.admit_source_code,
        mc.admit_type_code,
        mc.bill_type_code,
        mc.place_of_service_code,
        mc.rendering_npi,
        mc.billing_npi,
        mc.facility_npi,
        mc.diagnosis_code_1,
        mc.diagnosis_code_2,
        mc.diagnosis_code_3,
        mc.procedure_code_1,
        mc.procedure_code_2,
        mc.procedure_code_3
    from medical_claim as mc
    inner join {{ ref('reference_data__calendar') }} as c
        on mc.service_date = c.full_date
),

pharmacy_with_month as (
    select
        pc.data_source,
        pc.payer,
        pc.plan,
        c.year_month_int,
        c.year_month,
        pc.ndc_code
    from pharmacy_claim as pc
    inner join {{ ref('reference_data__calendar') }} as c
        on pc.service_date = c.full_date
),

field_observations as (
    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'drg_code' as field_name,
        case
            when m.claim_type <> 'institutional' then 'not_applicable'
            when nullif(m.drg_code, '') is null then 'null'
            when ms.ms_drg_code is not null then 'valid'
            when apr.apr_drg_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__ms_drg') }} as ms
        on m.drg_code = ms.ms_drg_code
        and m.drg_code_type = 'ms-drg'
    left join {{ ref('terminology__apr_drg') }} as apr
        on m.drg_code = apr.apr_drg_code
        and m.drg_code_type = 'apr-drg'

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'revenue_center_code' as field_name,
        case
            when m.claim_type <> 'institutional' then 'not_applicable'
            when nullif(m.revenue_center_code, '') is null then 'null'
            when rev.revenue_center_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__revenue_center') }} as rev
        on m.revenue_center_code = rev.revenue_center_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'hcpcs_code' as field_name,
        case
            when nullif(m.hcpcs_code, '') is null then 'null'
            when hcpcs.hcpcs is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__hcpcs_level_2') }} as hcpcs
        on m.hcpcs_code = hcpcs.hcpcs

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'diagnosis_code_1' as field_name,
        case
            when nullif(m.diagnosis_code_1, '') is null then 'null'
            when dx1.icd_10_cm is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__icd_10_cm') }} as dx1
        on m.diagnosis_code_1 = dx1.icd_10_cm

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'diagnosis_code_2' as field_name,
        case
            when nullif(m.diagnosis_code_2, '') is null then 'null'
            when dx2.icd_10_cm is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__icd_10_cm') }} as dx2
        on m.diagnosis_code_2 = dx2.icd_10_cm

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'diagnosis_code_3' as field_name,
        case
            when nullif(m.diagnosis_code_3, '') is null then 'null'
            when dx3.icd_10_cm is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__icd_10_cm') }} as dx3
        on m.diagnosis_code_3 = dx3.icd_10_cm

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'procedure_code_1' as field_name,
        case
            when m.claim_type <> 'institutional' then 'not_applicable'
            when nullif(m.procedure_code_1, '') is null then 'null'
            when pcs1.icd_10_pcs is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__icd_10_pcs') }} as pcs1
        on m.procedure_code_1 = pcs1.icd_10_pcs

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'procedure_code_2' as field_name,
        case
            when m.claim_type <> 'institutional' then 'not_applicable'
            when nullif(m.procedure_code_2, '') is null then 'null'
            when pcs2.icd_10_pcs is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__icd_10_pcs') }} as pcs2
        on m.procedure_code_2 = pcs2.icd_10_pcs

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'procedure_code_3' as field_name,
        case
            when m.claim_type <> 'institutional' then 'not_applicable'
            when nullif(m.procedure_code_3, '') is null then 'null'
            when pcs3.icd_10_pcs is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__icd_10_pcs') }} as pcs3
        on m.procedure_code_3 = pcs3.icd_10_pcs

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'discharge_disposition_code' as field_name,
        case
            when m.claim_type <> 'institutional' then 'not_applicable'
            when nullif(m.discharge_disposition_code, '') is null then 'null'
            when dd.discharge_disposition_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__discharge_disposition') }} as dd
        on m.discharge_disposition_code = dd.discharge_disposition_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'admit_source_code' as field_name,
        case
            when m.claim_type <> 'institutional' then 'not_applicable'
            when nullif(m.admit_source_code, '') is null then 'null'
            when ad_src.admit_source_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__admit_source') }} as ad_src
        on m.admit_source_code = ad_src.admit_source_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'admit_type_code' as field_name,
        case
            when m.claim_type <> 'institutional' then 'not_applicable'
            when nullif(m.admit_type_code, '') is null then 'null'
            when ad_type.admit_type_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__admit_type') }} as ad_type
        on m.admit_type_code = ad_type.admit_type_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'bill_type_code' as field_name,
        case
            when m.claim_type <> 'institutional' then 'not_applicable'
            when nullif(m.bill_type_code, '') is null then 'null'
            when bill.bill_type_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__bill_type') }} as bill
        on m.bill_type_code = bill.bill_type_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'place_of_service_code' as field_name,
        case
            when m.claim_type <> 'professional' then 'not_applicable'
            when nullif(m.place_of_service_code, '') is null then 'null'
            when pos.place_of_service_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__place_of_service') }} as pos
        on m.place_of_service_code = pos.place_of_service_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'rendering_npi' as field_name,
        case
            when nullif(m.rendering_npi, '') is null then 'null'
            when prov.npi is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__provider') }} as prov
        on m.rendering_npi = prov.npi

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'billing_npi' as field_name,
        case
            when nullif(m.billing_npi, '') is null then 'null'
            when prov.npi is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__provider') }} as prov
        on m.billing_npi = prov.npi

    union all

    select
        m.data_source,
        m.payer,
        m.plan,
        m.year_month_int,
        m.year_month,
        'facility_npi' as field_name,
        case
            when nullif(m.facility_npi, '') is null then 'null'
            when prov.npi is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from claim_with_month as m
    left join {{ ref('terminology__provider') }} as prov
        on m.facility_npi = prov.npi

    union all

    select
        p.data_source,
        p.payer,
        p.plan,
        p.year_month_int,
        p.year_month,
        'ndc_code' as field_name,
        case
            when nullif(p.ndc_code, '') is null then 'null'
            when ndc.ndc is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from pharmacy_with_month as p
    left join {{ ref('terminology__ndc') }} as ndc
        on p.ndc_code = ndc.ndc
),

aggregated as (
    select
        data_source,
        payer,
        plan,
        year_month_int,
        year_month,
        field_name,
        count(*) as claim_lines_total,
        sum(case when mapping_status <> 'not_applicable' then 1 else 0 end) as applicable_claim_lines,
        sum(case when mapping_status = 'valid' then 1 else 0 end) as valid_claim_lines,
        sum(case when mapping_status = 'invalid' then 1 else 0 end) as invalid_claim_lines,
        sum(case when mapping_status = 'null' then 1 else 0 end) as null_claim_lines,
        sum(case when mapping_status = 'not_applicable' then 1 else 0 end) as not_applicable_claim_lines
    from field_observations
    group by
        data_source,
        payer,
        plan,
        year_month_int,
        year_month,
        field_name
)

select
    data_source,
    payer,
    plan,
    year_month_int,
    year_month,
    field_name,
    claim_lines_total,
    applicable_claim_lines,
    valid_claim_lines,
    invalid_claim_lines,
    null_claim_lines,
    not_applicable_claim_lines,
    cast(valid_claim_lines as {{ dbt.type_numeric() }})
        / nullif(cast(claim_lines_total as {{ dbt.type_numeric() }}), 0) as valid_rate,
    cast(invalid_claim_lines as {{ dbt.type_numeric() }})
        / nullif(cast(claim_lines_total as {{ dbt.type_numeric() }}), 0) as invalid_rate,
    cast(null_claim_lines as {{ dbt.type_numeric() }})
        / nullif(cast(claim_lines_total as {{ dbt.type_numeric() }}), 0) as null_rate,
    cast(not_applicable_claim_lines as {{ dbt.type_numeric() }})
        / nullif(cast(claim_lines_total as {{ dbt.type_numeric() }}), 0) as not_applicable_rate,
    cast(valid_claim_lines as {{ dbt.type_numeric() }})
        / nullif(cast(applicable_claim_lines as {{ dbt.type_numeric() }}), 0) as valid_rate_applicable,
    '{{ var("tuva_last_run", run_started_at.astimezone(modules.pytz.timezone("UTC"))) }}' as tuva_last_run
from aggregated
order by
    data_source,
    payer,
    plan,
    field_name,
    year_month_int
