"""Forward live Wikimedia recent changes to Kafka for a bounded test."""
import argparse,json,time,os
import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient,NewTopic
p=argparse.ArgumentParser();p.add_argument('--seconds',type=int,default=1800);p.add_argument('--contact',required=True)
p.add_argument('--broker',default='localhost:9092');p.add_argument('--topic',default='wikistreams');a=p.parse_args()
admin=AdminClient({'bootstrap.servers':a.broker})
for future in admin.create_topics([NewTopic(a.topic,1,1)]).values():
    try:future.result()
    except Exception as e:
        if 'TOPIC_ALREADY_EXISTS' not in str(e):raise
producer=Producer({'bootstrap.servers':a.broker})
metrics={'sent':0,'delivered':0,'failed':0,'started_at':time.time()}
def delivered(error,message):
    metrics['failed' if error else 'delivered']+=1
end=time.monotonic()+a.seconds
try:
    while time.monotonic()<end:
        try:
            with requests.get('https://stream.wikimedia.org/v2/stream/recentchange',stream=True,
                headers={'User-Agent':f'ECE-IMDb-course/1.0 ({a.contact})','Accept':'text/event-stream'},timeout=(20,20)) as response:
                response.raise_for_status();parts=[]
                for line in response.iter_lines(decode_unicode=True):
                    if time.monotonic()>=end:break
                    if line.startswith('data:'):parts.append(line[5:].lstrip())
                    elif not line and parts:
                        try:event=json.loads('\n'.join(parts))
                        except json.JSONDecodeError:parts=[];continue
                        parts=[]
                        if event.get('namespace')!=0 or event.get('type') not in ('edit','new'):continue
                        # All Wikipedia languages are retained; IMDb matching happens in Spark.
                        if not event.get('server_name','').endswith('.wikipedia.org'):continue
                        encoded=json.dumps(event).encode()
                        while True:
                            try:producer.produce(a.topic,value=encoded,on_delivery=delivered);break
                            except BufferError:producer.poll(1)
                        producer.poll(0);metrics['sent']+=1
        except requests.RequestException as e:print('Reconnect:',e,flush=True);time.sleep(2)
finally:
    remaining=producer.flush(30);metrics.update(queued_at_exit=remaining,finished_at=time.time())
    os.makedirs('results/streaming',exist_ok=True)
    with open('results/streaming/producer_metrics.json','w') as f:json.dump(metrics,f,indent=2)
    print(metrics)
