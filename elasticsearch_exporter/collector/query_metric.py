from collections import OrderedDict
from functools import partial
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

from elasticsearch import Elasticsearch
from prometheus_client.core import GaugeMetricFamily

from .base import interval_handle


class QueryMetricCollector(object):
    def __init__(self, es_client: 'Elasticsearch'):
        self.es_client: 'Elasticsearch' = es_client
        self.custom_metric_dict: Dict[str, GaugeMetricFamily] = {}

    def gen_job(self, config: Dict[str, Any]) -> Generator[Tuple[partial, Dict[str, Any]], None, None]:
        global_c: Dict[str, Any] = config['global']
        for metric_config_dict in config['metrics']:
            _interval: str = metric_config_dict.get('interval', global_c['interval'])
            if 'timeout' not in metric_config_dict:
                metric_config_dict['timeout'] = global_c['timeout']
            if 'jitter' not in metric_config_dict:
                metric_config_dict['jitter'] = global_c['jitter']
            metric_config_dict['interval'] = interval_handle(_interval)

            yield (
                partial(self.get_metric, metric_config_dict),
                metric_config_dict
            )

    def _aggregations_handle(
            self, aggregations_dict: Dict[str, Dict[str, Any]], label_dict: Optional[OrderedDict[str, str]] = None
    ) -> Generator[Tuple[OrderedDict[str, str], int], None, None]:
        for metric_key, metric_dict in aggregations_dict.items():
            if metric_key == 'key' or metric_key == 'doc_count':
                continue
            if label_dict is None:
                label_dict: OrderedDict[str, str] = OrderedDict()
            bucket_dict_list: List[Dict[str, Any]] = metric_dict["buckets"]
            for bucket_dict in bucket_dict_list:
                label_dict[metric_key] = bucket_dict['key']
                if len(bucket_dict) > 2:
                    yield from self._aggregations_handle(
                        bucket_dict,
                        label_dict=label_dict
                    )
                else:
                    metric_value: int = bucket_dict['doc_count']
                    yield label_dict, metric_value

    def get_metric(self, metric_config_dict: Dict[str, Any]):
        response: Dict[str, Any] = self.es_client.search(
            index=metric_config_dict['index'],
            body=metric_config_dict['query_json'],
            request_timeout=metric_config_dict['timeout']
        )
        metric: str = metric_config_dict["metric"].format(**metric_config_dict)
        metric = metric.replace("*", "")
        if response['timed_out']:
            return

        key: str = metric + '_total_milliseconds'
        g: 'GaugeMetricFamily' = GaugeMetricFamily(
            key,
            metric_config_dict['doc'] + ' total_milliseconds',
            value=response['took']
        )
        self.custom_metric_dict[key] = g

        total = response['hits']['total']
        if isinstance(total, dict):
            total = total['value']

        key: str = metric + '_hits_total'
        g: 'GaugeMetricFamily' = GaugeMetricFamily(
            key,
            metric_config_dict['doc'] + ' hits_total',
            total
        )
        self.custom_metric_dict[key] = g

        if 'aggregations' in response:
            key: str = metric + '_aggregations'
            g: 'Optional[GaugeMetricFamily]' = None
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

    def collect(self) -> Generator[GaugeMetricFamily, None, None]:
        query_metrics = self.custom_metric_dict.copy()
        for metric in query_metrics.values():
            yield metric
