import logging
from collections import OrderedDict
from functools import partial

from elasticsearch.exceptions import ConnectionTimeout
from prometheus_client.core import GaugeMetricFamily


def collector_up_gauge(metric_name, description, succeeded=True):
    description = 'Did the {} fetch succeed.'.format(description)
    return GaugeMetricFamily(metric_name + '_up', description, value=int(succeeded))


class QueryMetricCollector(object):
    def __init__(self, es_client):
        self.es_client = es_client
        self.custom_metric_dict = {}

    def gen_job(self, config):
        global_c = config['global']
        for metric_config_dict in config['metrics']:
            _interval = metric_config_dict.get('interval', global_c['interval'])
            if 'timeout' not in metric_config_dict:
                metric_config_dict['timeout'] = global_c['timeout']

            try:
                interval = int(_interval)
            except Exception:
                interval = int(_interval[:-1])
                unit = _interval[-1]
                if unit == 's':
                    pass
                elif unit == 'm':
                    interval = interval * 60
                elif unit == 'h':
                    interval = interval * 60 * 60

            yield (
                partial(self.get_metric, metric_config_dict),
                interval,
                metric_config_dict['name']
            )

    def _aggregations_handle(self, aggregations_dict, label_dict=None):
        for metric_key, metric_dict in aggregations_dict.items():
            if metric_key == 'key' or metric_key == 'doc_count':
                continue
            if label_dict is None:
                label_dict = OrderedDict()
            bucket_dict_list = metric_dict["buckets"]
            for bucket_dict in bucket_dict_list:
                label_dict[metric_key] = bucket_dict['key']
                if len(bucket_dict) > 2:
                    yield from self._aggregations_handle(
                        bucket_dict,
                        label_dict=label_dict
                    )
                else:
                    metric_value = bucket_dict['doc_count']
                    yield label_dict, metric_value

    def get_metric(self, metric_config_dict):
        response = self.es_client.search(
            index=metric_config_dict['index'],
            body=metric_config_dict['query_json'],
            request_timeout=metric_config_dict['timeout']
        )
        index = metric_config_dict["index"].replace("*", "")
        metric = f'es_{index}_{metric_config_dict["name"]}'
        if response['timed_out']:
            return

        key = metric + '_total_milliseconds'
        g = GaugeMetricFamily(
            key,
            metric_config_dict['doc'] + ' total_milliseconds',
            value=response['took']
        )
        self.custom_metric_dict[key] = g

        total = response['hits']['total']
        if isinstance(total, dict):
            total = total['value']

        key = metric + '_hits_total'
        g = GaugeMetricFamily(
            key,
            metric_config_dict['doc'] + ' hits_total',
            total
        )
        self.custom_metric_dict[key] = g

        if 'aggregations' in response:
            key = metric + '_aggregations'
            g = None
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

    def collect(self):
        query_metrics = self.custom_metric_dict.copy()
        for metric_dict in query_metrics.values():
            yield metric_dict


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


class ClusterHealthCollector(BaseCollector):
    """
    relocating_shards
    active_shards
    delayed_unassigned_shards
    active_primary_shards
    active_shards_percent_as_number
    initializing_shards
    status
    unassigned_shards
    number_of_pending_tasks
    number_of_nodes
    number_of_in_flight_fetch
    number_of_data_nodes
    task_max_waiting_in_queue_millis
    """
    def __init__(self, es_client, timeout, level):
        self.es_client = es_client
        self.level = level
        super().__init__('es_cluster_health', 'Cluster Health', timeout)
        self.status_dict = {
            'green': 0,
            'yellow': 1,
            'red': 2
        }

    def get_metric(self):
        response = self.es_client.cluster.health(
            level=self.level, request_timeout=self.timeout
        )
        cluster_name = response['cluster_name']
        del response['cluster_name']
        del response['timed_out']
        status = response['status']
        response['status'] = self.status_dict.get(status, 2)
        for key, value in response.items():
            metric = f'{self.metric_name}_{key}'
            g = GaugeMetricFamily(
                metric,
                f'{self.description} {key}',
                labels=['cluster_name']
            )
            g.add_metric([cluster_name], value)
            yield g


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
