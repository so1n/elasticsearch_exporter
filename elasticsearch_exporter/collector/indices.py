from typing import Any, Dict
from elasticsearch import Elasticsearch
from prometheus_client.core import GaugeMetricFamily

from .base import BaseEsCollector


class IndicesStatsCollector(BaseEsCollector):
    key: str = 'indices_stats'

    def __init__(self, es_client: 'Elasticsearch', config: Dict[str, Any]):
        super().__init__(es_client, config)

        self.status_dict: Dict[str, int] = {
            'green': 0,
            'yellow': 1,
            'red': 2
        }

    def _get_metric(self):
        response: Dict[str, Any] = self.es_client.indices.stats()
        indices = response['indices']
        indices['_all'] = response['_all']
        labels_key_list = ['index', 'context']

        for index, index_dict in indices.items():
            for key in ['primaries', 'total']:
                for metric_name, metric_doc, value in self.auto_gen_metric(self.key + '_', index_dict[key]):
                    if self._is_block(metric_name):
                        continue
                    g: 'GaugeMetricFamily' = GaugeMetricFamily(
                        metric_name,
                        metric_doc,
                        labels=labels_key_list
                    )
                    g.add_metric([index, key], value)
                    yield g

