from .base import BaseEsCollector
from prometheus_client.core import GaugeMetricFamily


class EsClusterCollector(BaseEsCollector):
    def __init__(self, es_client, config):
        self.es_client = es_client
        super().__init__(config, 'es_cluster')
        self.timeout = self.config.get('timeout', 10)
        self.level = self.config.get('level', 'info')
        self.status_dict = {
            'green': 0,
            'yellow': 1,
            'red': 2
        }
        self._doc_dict = {}

    def _get_metric(self):
        response = self.es_client.cluster.health(level=self.level, request_timeout=self.timeout)
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
