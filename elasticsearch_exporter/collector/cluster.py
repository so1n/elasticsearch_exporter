from .base import BaseEsCollector
from prometheus_client.core import GaugeMetricFamily


class EsClusterCollector(BaseEsCollector):
    key = 'es_cluster'

    def __init__(self, es_client, config):
        self.es_client = es_client
        super().__init__(config)

        self.param_dict = self.get_request_param_from_config(
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
        self.status_dict = {
            'green': 0,
            'yellow': 1,
            'red': 2
        }
        self._doc_dict = {}

    def _get_metric(self):
        response = self.es_client.cluster.health(params=self.param_dict)
        cluster_name = response['cluster_name']
        del response['cluster_name']
        del response['timed_out']
        status = response['status']
        response['status'] = self.status_dict.get(status, 2)
        for key, value in response.items():
            metric = f'{self.key}_{key}'
            g = GaugeMetricFamily(
                metric,
                self._doc_dict.get('key', key),
                labels=['cluster_name']
            )
            g.add_metric([cluster_name], value)
            yield g
