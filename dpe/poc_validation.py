"""Local POC interoperability and recovery acceptance; no Azure gate claims."""
from pathlib import Path
from .common import ROOT,DATASETS,canonical,read_json,write_json
from .engines import spark
from . import lakehouse
from .synthetic import generate
from .ingestion import load_batch
from .rules import evaluate
from .benchmark_reports import scorecard
from .benchmark_telemetry import spark_events


def _verify(workspace,count=1000,acceptance_path=None):
    workspace=Path(workspace)
    if workspace.exists():raise ValueError('workspace exists')
    workspace.mkdir(parents=True)
    from .experiment import acceptance
    from .experiment_observer import Observer
    criteria=acceptance(acceptance_path);write_json(workspace/'frozen_criteria.json',criteria)
    observer=Observer(workspace,criteria)
    evidence={'criteria':criteria,'status':'running','count':count,'revision':lakehouse.sql_revision(),'engines':{},'scope':'local publication verification; original Azure gates remain open'}
    try:
        observer.start();observer.mark('compatibility')
        evidence['compatibility']=spark.compatibility(workspace/'compatibility')
        observer.mark('fixtures-spark')
        evidence['spark_fixtures']=spark.fixtures(workspace/'fixtures-spark')
        observer.mark('fixtures-trino')
        evidence['trino_fixtures']=lakehouse.fixture_check(workspace/'fixtures-trino')
        generate(workspace/'source',count=count)
        initial=workspace/'source/initial';delta=workspace/'source/delta'
        from .bounded_io import enabled,sources,release
        if enabled():header,combined,_,_=sources([initial,delta])
        else:
            h,base,_=load_batch(initial);header,updates,_=load_batch(delta)
            combined={k:base[k]+updates[k] for k in DATASETS}
        expected=evaluate(combined,header['cutoff'],header['report_start'])
        if enabled():
            for key in list(expected):
                if key not in lakehouse.OUTPUTS:del expected[key]
            release()
        for engine,execute in [('trino',lakehouse.run),('spark',spark.run)]:
            platform=workspace/engine;entry={'failpoints':{}};evidence['engines'][engine]=entry
            observer.mark('initial-'+engine)
            entry['initial']=execute(initial,platform)
            active=(platform/'active.json').read_bytes();first=read_json(platform/'active.json')['publication_id']
            pinned={name:lakehouse.consume(platform,name,publication_id=first)['rows'] for name in lakehouse.OUTPUTS}
            for fail in ('between_gold_writes','before_publish'):
                observer.mark('failure-'+engine+'-'+fail)
                try:execute(delta,platform,fail_at=fail)
                except RuntimeError as error:
                    if 'injected failure: '+fail not in str(error):raise
                else:raise AssertionError('failpoint did not interrupt publication')
                if active!=(platform/'active.json').read_bytes():raise AssertionError('failed attempt changed active checkpoint')
                for name in lakehouse.OUTPUTS:
                    if pinned[name]!=lakehouse.consume(platform,name,publication_id=first)['rows']:raise AssertionError('partial product became visible')
                entry['failpoints'][fail]='passed'
            observer.mark('delta-'+engine)
            entry['delta']=execute(delta,platform)
            differences=[]
            for name in lakehouse.OUTPUTS:
                differences+=lakehouse.compare_rows(name,expected[name],lakehouse.consume(platform,name)['rows'])
                if pinned[name]!=lakehouse.consume(platform,name,publication_id=first)['rows']:raise AssertionError('new commit changed old pin')
            if differences:raise AssertionError('engine full rebuild differences: '+canonical(differences[:5]))
            entry['differences']=differences
            active=(platform/'active.json').read_bytes()
            for batch in [initial,delta]:
                if execute(batch,platform)['status']!='already_committed':raise AssertionError('replay not idempotent')
            if active!=(platform/'active.json').read_bytes():raise AssertionError('old replay rolled publication back')
            entry['status']='passed'
        fixture_count=evidence['trino_fixtures']['fixtures']
        if fixture_count!=evidence['spark_fixtures']['fixture_count']:raise AssertionError('engine fixture counts differ')
        findings={'spark_compatibility':'cross-engine Iceberg reads passed','fixtures':str(fixture_count)+' independent cases in both engines',
                  'local_publication':'both products, old pins, failures, retry and replay pass for Trino and Spark',
                  'scope':'local '+str(count)+' incidents; required distributed faults and Azure economics not assessed'}
        scorecard(workspace,findings=findings,criteria=criteria)
        evidence['status']='passed'
        return {'status':'passed','fixtures':fixture_count,'engines':['trino','spark'],'report':str(workspace/'verification.json'),'poc_verdict':'incomplete'}
    except Exception as error:evidence.update(status='failed',error=str(error));raise
    finally:
        observer.stop();write_json(workspace/'verification.json',evidence)


def _restart_check(workspace):
    workspace=Path(workspace);report={'status':'running','publications':[]}
    try:
        from .bounded_io import enabled,sources,release
        for engine in ('trino','spark','targeted'):
            platform=workspace/engine;combined={k:[] for k in DATASETS}
            if not (platform/'active.json').exists():continue
            for publication in reversed(list(lakehouse.committed_publications(platform))):
                if enabled():header,combined,_,_=sources([platform/path for path in publication['archives']])
                else:
                    path=platform/publication['archives'][-1];header,datasets,_=load_batch(path)
                    for name in DATASETS:combined[name]+=datasets[name]
                expected=evaluate(combined,header['cutoff'],header['report_start'])
                if enabled():
                    for name in list(expected):
                        if name not in lakehouse.OUTPUTS:del expected[name]
                    release()
                for name in lakehouse.OUTPUTS:
                    actual=lakehouse.consume(platform,name,publication_id=publication['publication_id'])['rows']
                    differences=lakehouse.compare_rows(name,expected[name],actual)
                    if differences:raise AssertionError('restart pin differences: '+canonical(differences[:5]))
                report['publications'].append({'engine':engine,'publication_id':publication['publication_id'],'status':'passed'})
                if enabled():del expected,actual;release()
        if not report['publications']:raise ValueError('no committed publications to verify after restart')
        report['status']='passed';return report
    except Exception as error:report.update(status='failed',error=str(error));raise
    finally:write_json(workspace/'restart.json',report)


def verify(workspace,count=1000,acceptance_path=None):
    from .experiment import admission,acceptance
    from .bounded_io import profile
    with admission(workspace),profile(acceptance(acceptance_path)):return _verify(workspace,count,acceptance_path)


def restart_check(workspace):
    from .experiment import admission,frozen
    from .bounded_io import profile
    with admission(workspace),profile(frozen(workspace)):return _restart_check(workspace)
