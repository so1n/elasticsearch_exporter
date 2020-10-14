from typing import Any, Dict
from elasticsearch import Elasticsearch
from prometheus_client.core import GaugeMetricFamily

from .base import BaseEsCollector


class EsClusterCollector(BaseEsCollector):
    key: str = 'es_cluster'

    def __init__(self, es_client: 'Elasticsearch', config: Dict[str, Any]):
        super().__init__(es_client, config)

        self.param_dict: Dict[str, Any] = self.get_request_param_from_config(
            (
                ('expand_wildcards', ('open', 'closed', 'none', 'all'), 'all'),
                ('level', ('cluster', 'indices', 'shards'), 'cluster'),
                ('local', ..., 'false'),
                ('timeout', ..., '30s'),
                ('master_timeout', ..., '30s'),
                ('wait_for_active_shards', ..., 0),
                ('wait_for_events', ('immediate', 'urgent', 'high', 'normal', 'low', 'languid'), ...),
                ('wait_for_no_initializing_shards', ..., 'false'),
                ('wait_for_no_relocating_shards', ..., 'false'),
                ('wait_for_nodes', ('>=N', '<=N', '>N', '<N', 'ge(N)', 'le(N)', 'gt(N)', 'lt(N)'), ...),
                ('wait_for_status', ('green', 'yellow', 'red'), ...)
            )
        )
        self.status_dict: Dict[str, int] = {
            'green': 0,
            'yellow': 1,
            'red': 2
        }

    def _get_metric(self):
        response: Dict[str, Any] = self.es_client.cluster.health(params=self.param_dict)
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
