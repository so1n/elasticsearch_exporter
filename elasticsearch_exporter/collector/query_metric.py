from collections import OrderedDict
from functools import partial

from prometheus_client.core import GaugeMetricFamily


class QueryMetricCollector(object):
    def __init__(self, es_client):
        self.es_client = es_client
        self.custom_metric_dict = {}

    def gen_job(self, config):
        global_c = config['global']
        for metric_config_dict in config['metrics']:
            _interval = metric_config_dict.get('interval', global_c['interval'])
            if 'timeout' not in metric_config_dict:
                metric_config_dict['timeout'] = global_c['timeout']

            try:
                try:
                    interval = int(_interval)
                except Exception:
                    interval = int(_interval[:-1])
                    unit = _interval[-1]
                    if unit == 's':
                        pass
                    elif unit == 'm':
                        interval = interval * 60
                    elif unit == 'h':
                        interval = interval * 60 * 60
            except Exception:
                raise RuntimeError('Not support interval:{}'.format(_interval))

            yield (
                partial(self.get_metric, metric_config_dict),
                interval,
                metric_config_dict['name']
            )

    def _aggregations_handle(self, aggregations_dict, label_dict=None):
        for metric_key, metric_dict in aggregations_dict.items():
            if metric_key == 'key' or metric_key == 'doc_count':
                continue
            if label_dict is None:
                label_dict = OrderedDict()
            bucket_dict_list = metric_dict["buckets"]
            for bucket_dict in bucket_dict_list:
                label_dict[metric_key] = bucket_dict['key']
                if len(bucket_dict) > 2:
                    yield from self._aggregations_handle(
                        bucket_dict,
                        label_dict=label_dict
                    )
                else:
                    metric_value = bucket_dict['doc_count']
                    yield label_dict, metric_value

    def get_metric(self, metric_config_dict):
        response = self.es_client.search(
            index=metric_config_dict['index'],
            body=metric_config_dict['query_json'],
            request_timeout=metric_config_dict['timeout']
        )
        metric = metric_config_dict["metric"].format(**metric_config_dict)
        metric = metric.replace("*", "")
        if response['timed_out']:
            return

        key = metric + '_total_milliseconds'
        g = GaugeMetricFamily(
            key,
            metric_config_dict['doc'] + ' total_milliseconds',
            value=response['took']
        )
        self.custom_metric_dict[key] = g

        total = response['hits']['total']
        if isinstance(total, dict):
            total = total['value']

        key = metric + '_hits_total'
        g = GaugeMetricFamily(
            key,
            metric_config_dict['doc'] + ' hits_total',
            total
        )
        self.custom_metric_dict[key] = g

        if 'aggregations' in response:
            key = metric + '_aggregations'
            g = None
            for label_dict, metric_value in self._aggregations_handle(response['aggregations']):
                if not g:
                    label_key_list = label_dict.keys()
                    g = GaugeMetricFamily(
                        key,
                        metric_config_dict['doc'] + f' custom query {",".join(label_key_list)}',
                        labels=label_key_list
                    )
                g.add_metric(label_dict.values(), metric_value)
            self.custom_metric_dict[key] = g

    def collect(self):
        query_metrics = self.custom_metric_dict.copy()
        for metric_dict in query_metrics.values():
            yield metric_dict
