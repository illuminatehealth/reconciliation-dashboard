{{ config(
    enabled = var('reconciliation_dashboard_enabled', true) | as_bool
) }}

with calendar_month as (
    select distinct
        year_month_int,
        year_month
    from {{ ref('reference_data__calendar') }}
    where day = 1
),

member_months_normalized as (
    select
        mm.data_source,
        mm.payer,
        mm.{{ the_tuva_project.quote_column('plan') }} as plan_name,
        mm.person_id,
        c.year_month_int,
        c.year_month
    from {{ ref('core__member_months') }} as mm
    inner join calendar_month as c
        on cast(replace(cast(mm.year_month as {{ dbt.type_string() }}), '-', '') as {{ dbt.type_int() }}) = c.year_month_int
),

medical_claims_normalized as (
    select
        mc.data_source,
        mc.payer,
        mc.{{ the_tuva_project.quote_column('plan') }} as plan_name,
        mc.person_id,
        mc.claim_id,
        coalesce(mc.paid_amount, 0) as paid_amount,
        c.year_month_int,
        c.year_month
    from {{ ref('core__medical_claim') }} as mc
    inner join {{ ref('reference_data__calendar') }} as c
        on coalesce(mc.claim_line_start_date, mc.claim_start_date) = c.full_date
),

month_bounds as (
    select
        data_source,
        payer,
        plan_name,
        min(year_month_int) as min_year_month_int,
        max(year_month_int) as max_year_month_int
    from (
        select
            data_source,
            payer,
            plan_name,
            year_month_int
        from member_months_normalized

        union all

        select
            data_source,
            payer,
            plan_name,
            year_month_int
        from medical_claims_normalized
    ) as all_months
    group by
        data_source,
        payer,
        plan_name
),

month_spine as (
    select
        b.data_source,
        b.payer,
        b.plan_name,
        c.year_month_int,
        c.year_month
    from month_bounds as b
    inner join calendar_month as c
        on c.year_month_int between b.min_year_month_int and b.max_year_month_int
),

member_month_agg as (
    select
        data_source,
        payer,
        plan_name,
        year_month_int,
        min(year_month) as year_month,
        count(*) as member_months,
        count(distinct person_id) as members
    from member_months_normalized
    group by
        data_source,
        payer,
        plan_name,
        year_month_int
),

medical_claim_agg as (
    select
        data_source,
        payer,
        plan_name,
        year_month_int,
        min(year_month) as year_month,
        count(distinct claim_id) as claims,
        count(*) as claim_lines,
        sum(paid_amount) as paid_amount
    from medical_claims_normalized
    group by
        data_source,
        payer,
        plan_name,
        year_month_int
),

claim_members as (
    select distinct
        data_source,
        payer,
        plan_name,
        year_month_int,
        person_id
    from medical_claims_normalized
),

members_with_claims as (
    select
        mm.data_source,
        mm.payer,
        mm.plan_name,
        mm.year_month_int,
        count(distinct mm.person_id) as members_with_claims
    from member_months_normalized as mm
    inner join claim_members as cm
        on mm.data_source = cm.data_source
        and coalesce(mm.payer, '') = coalesce(cm.payer, '')
        and coalesce(mm.plan_name, '') = coalesce(cm.plan_name, '')
        and mm.year_month_int = cm.year_month_int
        and mm.person_id = cm.person_id
    group by
        mm.data_source,
        mm.payer,
        mm.plan_name,
        mm.year_month_int
)

select
    s.data_source,
    s.payer,
    s.plan_name as {{ the_tuva_project.quote_column('plan') }},
    s.year_month_int,
    s.year_month,
    coalesce(m.member_months, 0) as member_months,
    coalesce(m.members, 0) as members,
    coalesce(c.claims, 0) as claims,
    coalesce(c.claim_lines, 0) as claim_lines,
    coalesce(c.paid_amount, 0) as paid_amount,
    coalesce(w.members_with_claims, 0) as members_with_claims,
    cast(coalesce(w.members_with_claims, 0) as {{ dbt.type_numeric() }})
        / nullif(cast(coalesce(m.member_months, 0) as {{ dbt.type_numeric() }}), 0) as pct_members_with_claims,
    cast(coalesce(c.claims, 0) as {{ dbt.type_numeric() }}) * 1000
        / nullif(cast(coalesce(m.member_months, 0) as {{ dbt.type_numeric() }}), 0) as claims_per_1000,
    cast(coalesce(c.paid_amount, 0) as {{ dbt.type_numeric() }})
        / nullif(cast(coalesce(m.member_months, 0) as {{ dbt.type_numeric() }}), 0) as pmpm_paid,
    cast(coalesce(c.paid_amount, 0) as {{ dbt.type_numeric() }})
        / nullif(cast(coalesce(c.claims, 0) as {{ dbt.type_numeric() }}), 0) as avg_paid_per_claim,
    '{{ var("tuva_last_run", run_started_at.astimezone(modules.pytz.timezone("UTC"))) }}' as tuva_last_run
from month_spine as s
left join member_month_agg as m
    on s.data_source = m.data_source
    and coalesce(s.payer, '') = coalesce(m.payer, '')
    and coalesce(s.plan_name, '') = coalesce(m.plan_name, '')
    and s.year_month_int = m.year_month_int
left join medical_claim_agg as c
    on s.data_source = c.data_source
    and coalesce(s.payer, '') = coalesce(c.payer, '')
    and coalesce(s.plan_name, '') = coalesce(c.plan_name, '')
    and s.year_month_int = c.year_month_int
left join members_with_claims as w
    on s.data_source = w.data_source
    and coalesce(s.payer, '') = coalesce(w.payer, '')
    and coalesce(s.plan_name, '') = coalesce(w.plan_name, '')
    and s.year_month_int = w.year_month_int
