import urllib.request,gzip,hashlib,json,time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
root=Path(__file__).resolve().parent
names=['title.basics','title.ratings','title.crew','name.basics','title.episode','title.principals','title.akas']
def fetch(name):
 p=root/'raw'/f'{name}.tsv.gz';tmp=p.with_suffix('.gz.part');url=f'https://datasets.imdbws.com/{name}.tsv.gz'
 print('START',name,flush=True)
 for attempt in range(3):
  try:
   with urllib.request.urlopen(url,timeout=90) as source,tmp.open('wb') as out:
    while b:=source.read(1024*1024):out.write(b)
   rows=0
   with gzip.open(tmp,'rb') as f:
    while b:=f.read(4*1024*1024):rows+=b.count(b'\n')
   h=hashlib.sha256()
   with tmp.open('rb') as f:
    while b:=f.read(4*1024*1024):h.update(b)
   tmp.replace(p)
   result={'table':name,'url':url,'sha256':h.hexdigest(),'bytes':p.stat().st_size,'rows_excluding_header':rows-1,'retrieved_at_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
   (root/(name+'.json')).write_text(json.dumps(result,indent=2))
   print('VERIFIED',name,result['rows_excluding_header'],flush=True)
   return result
  except Exception as e:
   print('RETRY',name,str(e),flush=True)
 raise RuntimeError(name)
with ThreadPoolExecutor(max_workers=3) as ex:
 results=[f.result() for f in as_completed([ex.submit(fetch,n) for n in names])]
(root/'manifest.json').write_text(json.dumps(results,indent=2))
