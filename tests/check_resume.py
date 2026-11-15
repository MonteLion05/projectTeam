"""Exercise original cleaning script, resume and source invalidation on a fixture."""
import gzip
import os
from pathlib import Path
import subprocess
import sys
from test_pipeline import PipelineTests, ROOT

PipelineTests.setUpClass()
PipelineTests.spark.stop()
try:
    env=dict(os.environ, PYTHONPATH=str(ROOT), PYSPARK_SUBMIT_ARGS='--master local[1] pyspark-shell')
    cmd=[sys.executable,str(ROOT/'scripts/clean_values.py'),'--tables','name.basics','title.basics','title.episode']
    run=subprocess.run(cmd,env=env,capture_output=True,text=True)
    if run.returncode: raise RuntimeError(run.stdout+'\n'+run.stderr)
    assert 'Value cleaning completed.' in run.stdout
    second=subprocess.run(cmd,env=env,capture_output=True,text=True)
    if second.returncode: raise RuntimeError(second.stdout+'\n'+second.stderr)
    assert second.stdout.count('Already completed:')==3,second.stdout
    target=PipelineTests.data/'raw/name.basics.tsv.gz'
    with gzip.open(target,'rt') as stream: contents=stream.read()
    with gzip.open(target,'wt') as stream: stream.write(contents.replace('Actor A','Actor Changed'))
    third=subprocess.run(cmd,env=env,capture_output=True,text=True)
    if third.returncode: raise RuntimeError(third.stdout+'\n'+third.stderr)
    assert 'Cleaning values: name.basics' in third.stdout
    assert third.stdout.count('Already completed:')==2,third.stdout
    print('PASS: full-column clean write/readback; resume 3 tables; source change recomputes only changed table.')
finally:
    PipelineTests.temp.cleanup()
