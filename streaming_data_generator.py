#!/usr/bin/env python
#
# Send to Kafka randomized messages based on the KDD 99 dataset.
#
# Written by: claudio.fahey@emc.com
#

from __future__ import division
from __future__ import print_function
import numpy as np
import pandas as pd
import datetime
import json
import uuid
from itertools import islice
from pykafka import KafkaClient
from time import sleep, time
from optparse import OptionParser
from common import get_kdd_schema_text
 
class TokenBucket(object):
    """An implementation of the token bucket algorithm.
    From http://en.sharejs.com/python/12878.
     
    >>> bucket = TokenBucket(80, 0.5)
    >>> print bucket.consume(10)
    True
    >>> print bucket.consume(90)
    False
    """

    def __init__(self, tokens, fill_rate):
        """tokens is the total tokens in the bucket. fill_rate is the
        rate in tokens/second that the bucket will be refilled."""
        self.capacity = float(tokens)
        self._tokens = float(tokens)
        self.fill_rate = float(fill_rate)
        self.timestamp = time()
 
    def consume(self, tokens):
        """Consume tokens from the bucket. Returns True if there were
        sufficient tokens otherwise False."""
        if tokens <= self.tokens:
            self._tokens -= tokens
        else:
            return False
        return True
 
    def get_tokens(self):
        if self._tokens < self.capacity:
            now = time()
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
            self.timestamp = now
        return self._tokens

    tokens = property(get_tokens) 

class KDDDataGenerator():
    """Generate random data based on the KDD 99 dataset."""

    def __init__(self, kdd_file, one_sequential_pass):
        self.sequence_id = 0
        self.random_state = np.random.RandomState(seed=None)
        self.kdd_file = kdd_file
        self.kdd_df = self.get_kdd_csv_dataframe()
        self.one_sequential_pass = one_sequential_pass

    def __iter__(self):
        """Iterator to return individual rows in random order."""
        if self.one_sequential_pass:
            for index, row in self.kdd_df.iterrows():
                row = row.to_dict()
                row.update(dict(
                    sequence_id=self.sequence_id, 
                    uuid=str(uuid.uuid4()),
                    utc=datetime.datetime.utcnow().isoformat(),
                    index=index))
                yield row
                self.sequence_id += 1
        else:
            while True:
                index = self.get_next_index()
                row = self.kdd_df.iloc[index].to_dict()
                row.update(dict(
                    sequence_id=self.sequence_id, 
                    uuid=str(uuid.uuid4()),
                    utc=datetime.datetime.utcnow().isoformat(), 
                    index=index))
                yield row
                self.sequence_id += 1

    def get_next_index(self):
        return self.random_state.random_integers(0, len(self.kdd_df) - 1)

    def get_kdd_csv_dataframe(self):
        """Read the entire dataset into memory as a Pandas dataframe."""
        schema_text = get_kdd_schema_text()
        cols = [x.split(': ')[0] for x in schema_text.split('\n')]
        print('Reading data file %s...' % self.kdd_file)
        df = pd.read_csv(self.kdd_file, names=cols)
        print('Read %d rows.' % len(df))
        return df

def main():
    parser = OptionParser()
    parser.add_option('', '--kdd_file', action='store', dest='kdd_file', help='path for KDD data')
    parser.add_option('', '--kafka_zookeeper_hosts', action='store', dest='kafka_zookeeper_hosts', help='list of Zookeeper hosts (host:port)')
    parser.add_option('', '--kafka_broker_list', action='store', dest='kafka_broker_list', help='list of Kafka brokers (host:port)')
    parser.add_option('', '--kafka_message_topic', action='store', dest='kafka_message_topic', help='topic to produce messages to')
    parser.add_option('', '--throttle_messages_per_sec', type='float', default=5.0,
        action='store', dest='throttle_messages_per_sec', help='Limit to specified rate')
    parser.add_option('', '--throttle_sleep_sec', type='float', default=0.25,
        action='store', dest='throttle_sleep_sec', help='Seconds to sleep when throttled')
    parser.add_option('', '--one_sequential_pass', action='store_true', dest='one_sequential_pass', help='Send entire data set in order exactly once')
    options, args = parser.parse_args()

    client = KafkaClient(zookeeper_hosts=options.kafka_zookeeper_hosts)
    topic = client.topics[options.kafka_message_topic]

    throttle = None
    if options.throttle_messages_per_sec > 0.0:
        tokens = max(1.0, 2.0*options.throttle_sleep_sec*options.throttle_messages_per_sec)
        throttle = TokenBucket(tokens=tokens, fill_rate=options.throttle_messages_per_sec)

    data_generator = KDDDataGenerator(kdd_file=options.kdd_file, one_sequential_pass=options.one_sequential_pass)

    last_report_time = 0.0

    with topic.get_producer(delivery_reports=False, linger_ms=10.0) as producer:
        msg_index = 0
        messages = data_generator
        for msg in messages:
            # Convert to JSON string and send to Kafka.
            msg_str = json.dumps(msg)
            producer.produce(msg_str)

            # Print status report to console.
            now = time()
            if now - last_report_time > 2.0:
                print('Sent sequence_id=%(sequence_id)d, index=%(index)d' % msg)
                last_report_time = now
            
            # Throttle message rate.
            if throttle:
                while not throttle.consume(1.0):
                    sleep(options.throttle_sleep_sec)
        print('Closing producer.')

    print('Done.')

if __name__ == '__main__':
    main()
