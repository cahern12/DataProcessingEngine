select * from {{ ref('stg_events') }} where valid and event_time<=cutoff and identity_rank=1
