"""Cached, rate-limited retrieval of human-authored Wikipedia text via exact IMDb IDs."""
import argparse,json,time,urllib.parse
from pathlib import Path
from datetime import datetime,timezone
import requests
p=argparse.ArgumentParser();p.add_argument('--results',default='results');p.add_argument('--contact',required=True)
p.add_argument('--pause',type=float,default=1.5);p.add_argument('--limit',type=int);a=p.parse_args()
root=Path(a.results)/'enrichment';cache=root/'cache';cache.mkdir(parents=True,exist_ok=True)
s=requests.Session();s.headers['User-Agent']=f'ECE-IMDb-course/1.0 ({a.contact})'
def get(url,params):
    for attempt in range(6):
        time.sleep(max(a.pause,1.0))
        r=s.get(url,params=params,timeout=60)
        if r.status_code==429 or r.status_code>=500:
            delay=r.headers.get('Retry-After','');time.sleep(float(delay) if delay.isdigit() else min(60,2**attempt));continue
        r.raise_for_status(); data=r.json()
        if data.get('error',{}).get('code')=='maxlag':time.sleep(5);continue
        if 'error' in data:raise RuntimeError(str(data['error']))
        return data
    raise RuntimeError('Retries exhausted')
targets={}
for file in sorted((root/'targets').glob('part-*')):
    for line in file.read_text().splitlines():
        x=json.loads(line);targets[x['imdb_id']]=x['kind']
for number,(ident,kind) in enumerate(sorted(targets.items())):
    if a.limit is not None and number>=a.limit:break
    dest=cache/f'{ident}.json'
    if dest.exists() and json.loads(dest.read_text()).get('status')!='error':continue
    record={'imdb_id':ident,'kind':kind,'retrieved_at':datetime.now(timezone.utc).isoformat(),
            'text':None,'source_url':None,'wikipedia_title':None,'status':'not_found'}
    try:
        query='''SELECT ?article WHERE { ?item wdt:P345 "%s" . ?article schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> . } LIMIT 2'''%ident
        result=get('https://query.wikidata.org/sparql',{'query':query,'format':'json'})['results']['bindings']
        if len(result)>1:record['status']='ambiguous'
        elif result:
            url=result[0]['article']['value']; title=urllib.parse.unquote(url.split('/wiki/',1)[1]).replace('_',' ')
            record.update(source_url=url,wikipedia_title=title)
            api='https://en.wikipedia.org/w/api.php'
            page=get(api,{'action':'query','prop':'extracts','explaintext':1,'titles':title,'format':'json','maxlag':5})
            text=next(iter(page['query']['pages'].values())).get('extract','')
            if kind=='film':
                import re
                match=re.search(r'(?mi)^==\s*(?:Plot|Synopsis)\s*==\s*\n',text)
                if match:
                    text=re.split(r'(?m)^==[^=]',text[match.end():],maxsplit=1)[0]
                    record['status']='ok'
                else:text=text.split('\n==',1)[0];record['status']='intro_fallback'
            else:text=text.split('\n==',1)[0];record['status']='ok'
            record['text']=text.strip() or None
            if not record['text']:record['status']='empty'
    except Exception as e:record.update(status='error',error=str(e))
    temp=dest.with_suffix('.tmp');temp.write_text(json.dumps(record,ensure_ascii=False),encoding='utf-8');temp.replace(dest)
    print(ident,record['status'],flush=True)
