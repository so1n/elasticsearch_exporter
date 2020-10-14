import argparse
import logging
import os

import yaml
from typing import Any, Dict, List, Type

from apscheduler.schedulers.background import BaseScheduler, BackgroundScheduler
from elasticsearch import Elasticsearch
from prometheus_client.exposition import start_http_server
from prometheus_client.core import REGISTRY

from elasticsearch_exporter import collector
from elasticsearch_exporter.collector.base import BaseEsCollector

from elasticsearch_exporter.utils import shutdown


@shutdown()
def main():
    parser: 'argparse.ArgumentParser' = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", default='./config.yaml')
    parser.add_argument("--es_cluster")
    parser.add_argument("--listen_port", default=9206)
    parser.add_argument("--log_level", default='INFO')
    parser.add_argument("--apscheduler_log_level", default='WARNING')
    args, unknown = parser.parse_known_args()

    es_cluster_list: List[str] = args.es_cluster.split(',')
    listen_port: int = int(args.listen_port)
    config_filename_path: str = args.config
    log_level: str = args.log_level
    apscheduler_log_level: str = args.apscheduler_log_level

    logging.basicConfig(
        format='[%(asctime)s %(levelname)s %(process)d] %(message)s',
        datefmt='%y-%m-%d %H:%M:%S',
        level=getattr(logging, log_level.upper()))

    start_http_server(listen_port)
    logging.info(f'Server started on port {listen_port}')

    es_client: 'Elasticsearch' = Elasticsearch(es_cluster_list, verify_certs=False)
    if os.path.exists(config_filename_path):
        logging.info(f'reading custom metric from {config_filename_path}')
        with open(config_filename_path, 'r') as config_file:
            custom_metric_config: Dict[str, Any] = yaml.load(config_file, Loader=yaml.FullLoader)

        scheduler: 'BaseScheduler' = BackgroundScheduler()

        # register custom metric
        if 'metrics' in custom_metric_config:
            query_metric_collector: 'collector.QueryMetricCollector' = collector.QueryMetricCollector(es_client)
            REGISTRY.register(query_metric_collector)

            for job, config in query_metric_collector.gen_job(custom_metric_config):
                scheduler.add_job(
                    job, 'interval', seconds=config['interval'], name=config['name'], jitter=config['jitter']
                )

        # register es self metric
        for es_system_class_name in dir(collector):
            if not es_system_class_name.endswith('Collector') \
                    or es_system_class_name == collector.QueryMetricCollector.__name__:
                continue
            collector_class: 'Type[BaseEsCollector]' = getattr(collector, es_system_class_name, ...)
            if collector_class is ...:
                continue
            if collector_class.key not in custom_metric_config:
                continue
            collector_instance: BaseEsCollector = collector_class(es_client, custom_metric_config)
            logging.info(f'enable {es_system_class_name}. enable_scheduler: {collector_instance.enable_scheduler}')
            REGISTRY.register(collector_instance)
            if collector_instance.enable_scheduler:
                job, config = collector_instance.gen_job()
                scheduler.add_job(
                    job, 'interval', seconds=config['interval'], name=config['name'], jitter=config['jitter']
                )

        logging.getLogger('apscheduler.executors.default').setLevel(getattr(logging, apscheduler_log_level))
        scheduler.start()
    else:
        logging.error("Can't found config path")
