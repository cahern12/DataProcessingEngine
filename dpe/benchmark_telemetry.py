"""Persisted Spark events and explicit unsupported metrics, not guessed cloud meters."""
from pathlib import Path
import json


def spark_events(directory):
    result={'task_count':0,'executor_cpu_seconds':0,'executor_run_milliseconds':0,'input_bytes':0,'input_records':0,
            'output_bytes':0,'output_records':0,'shuffle_remote_read_bytes':0,'shuffle_write_bytes':0,
            'memory_spilled_bytes':0,'disk_spilled_bytes':0,'peak_task_execution_memory_bytes':0,'task_failures':0,
            'event_files':[],'unsupported':['object_store_billable_request_counts','provider_billed_capacity','per_query_process_peak_rss']}
    for path in sorted(Path(directory).glob('*')):
        if not path.is_file() or path.name.startswith('.'):continue
        result['event_files'].append(str(path))
        with path.open() as stream:
            for line in stream:
                try:event=json.loads(line)
                except json.JSONDecodeError:continue
                if event.get('Event')!='SparkListenerTaskEnd':continue
                result['task_count']+=1
                metrics=event.get('Task Metrics',{})
                result['executor_cpu_seconds']+=metrics.get('Executor CPU Time',0)/1e9
                result['executor_run_milliseconds']+=metrics.get('Executor Run Time',0)
                result['memory_spilled_bytes']+=metrics.get('Memory Bytes Spilled',0)
                result['disk_spilled_bytes']+=metrics.get('Disk Bytes Spilled',0)
                result['peak_task_execution_memory_bytes']=max(result['peak_task_execution_memory_bytes'],metrics.get('Peak Execution Memory',0))
                for name,key,field in [('input_bytes','Input Metrics','Bytes Read'),('input_records','Input Metrics','Records Read'),
                        ('output_bytes','Output Metrics','Bytes Written'),('output_records','Output Metrics','Records Written'),
                        ('shuffle_remote_read_bytes','Shuffle Read Metrics','Remote Bytes Read'),('shuffle_write_bytes','Shuffle Write Metrics','Shuffle Bytes Written')]:
                    result[name]+=metrics.get(key,{}).get(field,0)
                if event.get('Task End Reason',{}).get('Reason') not in (None,'Success'):result['task_failures']+=1
    result['status']='captured' if result['task_count'] else 'unavailable'
    return result
