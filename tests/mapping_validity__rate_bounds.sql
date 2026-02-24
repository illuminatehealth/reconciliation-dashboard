select *
from {{ ref('reconciliation__mapping_validity_monthly') }}
where valid_rate < 0
   or valid_rate > 1
   or invalid_rate < 0
   or invalid_rate > 1
   or null_rate < 0
   or null_rate > 1
   or not_applicable_rate < 0
   or not_applicable_rate > 1
   or (valid_rate_applicable is not null and (valid_rate_applicable < 0 or valid_rate_applicable > 1))
