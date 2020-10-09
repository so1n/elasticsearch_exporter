import logging

from elasticsearch.exceptions import ConnectionTimeout
from prometheus_client.core import GaugeMetricFamily


def collector_up_gauge(metric_name, description, succeeded=True):
    description = 'Did the {} fetch succeed.'.format(description)
    return GaugeMetricFamily(metric_name + '_up', description, value=int(succeeded))

class BaseCollector(object):
    def __init__(self, metric_name_list, description, timeout):
        self.metric_name = metric_name_list
        self.description = description
        self.timeout = timeout

    def get_metric(self):
        raise NotImplementedError

    def collect(self):
        try:
            yield from self.get_metric()
        except ConnectionTimeout:
            logging.warning(
                f'fetching{self.description} timeout{self.timeout}'
            )
            yield collector_up_gauge(self.metric_name, self.description, succeeded=False)
        except Exception:
            logging.warning(f'fetching error: {self.description}')
            yield collector_up_gauge(self.metric_name, self.description, succeeded=False)
        else:
            yield collector_up_gauge(self.metric_name, self.description)


class NodesStatsCollector(BaseCollector):

    def __init__(self, es_client, timeout, metrics=None):
        self.es_client = es_client
        self.metrics = metrics
        super().__init__(['es', 'nodes_stats'], 'Nodes Stats', timeout)

    def get_metric(self):
        response = self.es_client.nodes.stats(metric=self.metrics,
                                              request_timeout=self.timeout)
        # metrics = nodes_stats_parser.parse_response(response, self.metric_name_list)
        # metric_dict = group_metrics(metrics)


class IndicesStatsCollector(BaseCollector):

    def __init__(self, es_client, timeout, parse_indices=False, metrics=None, fields=None):
        self.es_client = es_client
        self.timeout = timeout
        self.parse_indices = parse_indices
        self.metrics = metrics
        self.fields = fields
        super().__init__(['es', 'indices_stats'], 'Indices Stats', timeout)

    def get_metric(self):
        response = self.es_client.indices.stats(
            metric=self.metrics,
            fields=self.fields,
            request_timeout=self.timeout
        )
        import json
        with open('tes.json', 'w') as file:
            file.write(json.dumps(response))
        # metrics = indices_stats_parser.parse_response(response, self.parse_indices, self.metric_name_list)
        # metric_dict = group_metrics(metrics)
