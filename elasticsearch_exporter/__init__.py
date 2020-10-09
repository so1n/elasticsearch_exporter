import argparse
import logging
import os

import yaml

from apscheduler.schedulers.background import BackgroundScheduler
from elasticsearch import Elasticsearch
from prometheus_client.exposition import start_http_server
from prometheus_client.core import REGISTRY

from elasticsearch_exporter.collector import (
    ClusterHealthCollector,
    QueryMetricCollector,
)
from elasticsearch_exporter.utils import shutdown


@shutdown()
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", default='')
    parser.add_argument("--es_cluster")
    parser.add_argument("--listen_port", default=9206)
    parser.add_argument("--log_level", default='INFO')
    parser.add_argument("--apscheduler_log_level", default='WARNING')
    args, unknown = parser.parse_known_args()

    es_cluster_list: list = args.es_cluster.split(',')
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

    es_client = Elasticsearch(es_cluster_list, verify_certs=False)

    if not config_filename_path:
        config_filename_path = './config.yaml'
    if os.path.exists(config_filename_path):
        logging.info(f'reading custom metric from {config_filename_path}')
        with open(config_filename_path, 'r') as config_file:
            custom_metric_config = yaml.load(config_file, Loader=yaml.FullLoader)

        scheduler = BackgroundScheduler()

        if 'metrics' in custom_metric_config:
            query_metric_collector = QueryMetricCollector(es_client)
            REGISTRY.register(query_metric_collector)

            for job, interval, name in query_metric_collector.gen_job(custom_metric_config):
                scheduler.add_job(job, 'interval', seconds=interval, name=name)
        if 'cluster_health' in custom_metric_config:
            logging.info('enable cluster_health')
            cluster_health_collector = ClusterHealthCollector(es_client, custom_metric_config)
            REGISTRY.register(cluster_health_collector)
            if cluster_health_collector.enable_scheduler:
                job, interval, name = cluster_health_collector.gen_job()
                scheduler.add_job(job, 'interval', seconds=interval, name=name)

        logging.getLogger('apscheduler.executors.default').setLevel(getattr(logging, apscheduler_log_level))
        scheduler.start()
    else:
        logging.error("Can't Found config path")
