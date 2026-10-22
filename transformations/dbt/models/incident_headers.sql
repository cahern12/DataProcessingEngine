select case_id,incident_id,
  min_by(priority,row(source_updated_at,source_version)) priority_at_creation,
  min_by(service_id,row(source_updated_at,source_version)) creation_service,
  max_by(created_at,row(source_updated_at,source_version)) created_at,
  max_by(priority,row(source_updated_at,source_version)) current_priority,
  max_by(service_id,row(source_updated_at,source_version)) service_id,
  max_by(group_id,row(source_updated_at,source_version)) group_id,
  max_by(requester_id,row(source_updated_at,source_version)) requester_id,
  max(cutoff) cutoff,max(report_start) report_start
from {{ ref('eligible_snapshots') }} group by 1,2
