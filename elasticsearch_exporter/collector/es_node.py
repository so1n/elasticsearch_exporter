from .base import BaseEsCollector
from prometheus_client.core import GaugeMetricFamily


class EsNodeCollector(BaseEsCollector):
    key = 'es_node'

    def __init__(self, es_client, config):
        self.es_client = es_client
        super().__init__(config)
        self.timeout = self.config.get('timeout', 10)
        self._doc_dict = {}

    def _get_metric(self):
        response = self.es_client.nodes.stats(request_timeout=self.timeout)
        all_node_dict = response['nodes']
        for node_id in all_node_dict:
            node_dict = all_node_dict[node_id]
            node = node_dict['name']
            instance = node_dict['transport_address']
            labels_value_list = [node, node_id, instance]
            labels_key_list = ['node', 'node_id', 'instance']

            # node role
            node_role_list = node_dict['roles']
            g = GaugeMetricFamily(
                f'{self.key}_role',
                'node role',
                labels=labels_key_list
            )
            for role in ['data', 'ingest', 'master', 'ml']:
                g.add_metric(labels_value_list, float(role in node_role_list))
            yield g

            for es_system_metric in [
                'indices', 'os', 'process', 'jvm', 'thread_pool', 'fs', 'transport', 'http', 'breakers', 'script',
                'discovery', 'ingest'
            ]:
                for metric_name, metric_doc, value in self.auto_gen_metric(self.key + '_', node_dict[es_system_metric]):
                    g = GaugeMetricFamily(
                        metric_name,
                        metric_doc,
                        labels=labels_key_list
                    )
                    g.add_metric(labels_value_list, value)
                    yield g
