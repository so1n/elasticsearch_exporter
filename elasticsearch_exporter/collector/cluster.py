from typing import Any, Dict
from elasticsearch import Elasticsearch
from prometheus_client.core import GaugeMetricFamily

from .base import BaseEsCollector


class EsClusterCollector(BaseEsCollector):
    key: str = 'es_cluster'

    def __init__(self, es_client: 'Elasticsearch', config: Dict[str, Any]):
        super().__init__(es_client, config)

        self.status_dict: Dict[str, int] = {
            'green': 0,
            'yellow': 1,
            'red': 2
        }

    def _get_metric(self):
        response: Dict[str, Any] = self.es_client.cluster.health(params=self.config.get('request_param', None))
        cluster_name: str = response['cluster_name']
        del response['cluster_name']
        del response['timed_out']
        status: str = response['status']
        response['status'] = self.status_dict.get(status, 2)
        for key, value in response.items():
            metric: str = f'{self.key}_{key}'
            if self._is_block(metric):
                continue
            g: 'GaugeMetricFamily' = GaugeMetricFamily(
                metric,
                key,
                labels=['cluster_name']
            )
            g.add_metric([cluster_name], value)
            yield g
