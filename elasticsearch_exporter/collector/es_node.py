from .base import BaseEsCollector
from prometheus_client.core import GaugeMetricFamily


class EsNodeCollector(BaseEsCollector):
    key = 'es_node'

    def __init__(self, es_client, config):
        self.es_client = es_client
        super().__init__(config)
        self.param_dict = self.get_request_param_from_config(
            (
                ('node_id', ..., None),
                ('metric', ('_all', 'breaker', 'fs', 'http', 'indices', 'jvm', 'os', 'process', 'thread_pool',
                            'transport', 'discovery'), ...),
                ('index_metric', ('_all', 'completion', 'docs', 'fielddata', 'query_cache', 'flush', 'get', 'indexing',
                                  'merge', 'request_cache', 'refresh', 'search', 'segments', 'store', 'warmer',
                                  'suggest'), ...),
                ('completion_fields', ..., ...),
                ('fielddata_fields', ..., ...),
                ('fields', ..., ...),
                ('groups', ..., ...),
                ('include_segment_file_sizes', ..., 'false'),
                ('level', ('node', 'indices', 'shards'), 'node'),
                ('timeout', ..., '30s'),
                ('type', ..., ...)

            )
        )
        self.node_id = self.param_dict.pop('node_id')
        if 'metric' in self.param_dict:
            self.metric = self.param_dict.pop('metric')
        else:
            self.metric = None
        if self.metric not in ('indices', '_all') or 'index_metric' not in self.param_dict:
            self.index_metric = None
        else:
            self.index_metric = self.param_dict.pop('index_metric')

    def _get_metric(self):
        response = self.es_client.nodes.stats(
            node_id=self.node_id,
            metric=self.metric,
            index_metric=self.index_metric,
            params=self.param_dict
        )
        all_node_dict = response['nodes']
        for node_id in all_node_dict:
            node_dict = all_node_dict[node_id]
            node = node_dict['name']
            instance = node_dict['transport_address']
            labels_value_list = [node, node_id, instance]
            labels_key_list = ['node', 'node_id', 'instance']

            # node role
            node_role_list = node_dict['roles']
            metric = f'{self.key}_role'
            if not self.is_block(metric):
                g = GaugeMetricFamily(
                    metric,
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
                    if self.is_block(metric_name):
                        continue
                    g = GaugeMetricFamily(
                        metric_name,
                        metric_doc,
                        labels=labels_key_list
                    )
                    g.add_metric(labels_value_list, value)
                    yield g
