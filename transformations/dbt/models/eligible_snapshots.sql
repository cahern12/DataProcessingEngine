select * from {{ ref('stg_snapshots') }} where valid and source_updated_at<=cutoff and identity_rank=1
