from typing import Any, Dict, List, Optional
from elasticsearch import Elasticsearch
from prometheus_client.core import GaugeMetricFamily

from .base import BaseEsCollector


class EsNodeCollector(BaseEsCollector):
    key: str = 'es_node'

    def __init__(self, es_client: 'Elasticsearch', config: Dict[str, Any]):
        super().__init__(es_client, config)

        request_param: Dict[str, Any] = self.config.get('request_param', {})
        self.node_id: Optional[str] = request_param.get('node_id', None)
        self.metric: Optional[str] = request_param.get('metric', None)

        self.index_metric: Optional[str] = None
        if self.metric not in ('indices', '_all') or 'index_metric' not in request_param:
            self.index_metric = None
        else:
            self.index_metric = request_param.get('index_metric')

    def _get_metric(self):
        response: Dict[str, Any] = self.es_client.nodes.stats(
            node_id=self.node_id,
            metric=self.metric,
            index_metric=self.index_metric,
            params=self.config.get('request_param', None)
        )
        all_node_dict: Dict[str, Any] = response['nodes']
        for node_id in all_node_dict:
            node_dict: Dict[str, Any] = all_node_dict[node_id]
            node: str = node_dict['name']
            instance: str = node_dict['transport_address']
            labels_value_list: List[str] = [node, node_id, instance]
            labels_key_list: List[str] = ['node', 'node_id', 'instance']

            # node role
            node_role_list: List[str] = node_dict['roles']
            metric: str = f'{self.key}_role'
            if not self._is_block(metric):
                g: 'GaugeMetricFamily' = GaugeMetricFamily(
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
                    if self._is_block(metric_name):
                        continue
                    g: 'GaugeMetricFamily' = GaugeMetricFamily(
                        metric_name,
                        metric_doc,
                        labels=labels_key_list
                    )
                    g.add_metric(labels_value_list, value)
                    yield g
