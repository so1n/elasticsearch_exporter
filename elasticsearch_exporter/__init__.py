import argparse
import logging
import os

import yaml

from apscheduler.schedulers.background import BackgroundScheduler
from elasticsearch import Elasticsearch
from prometheus_client.exposition import start_http_server
from prometheus_client.core import REGISTRY

from elasticsearch_exporter import collector

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

        # register custom metric
        if 'metrics' in custom_metric_config:
            query_metric_collector = collector.QueryMetricCollector(es_client)
            REGISTRY.register(query_metric_collector)

            for job, config in query_metric_collector.gen_job(custom_metric_config):
                scheduler.add_job(
                    job, 'interval', seconds=config['interval'], name=config['name'], jitter=config['jitter']
                )

        # register es self metric
        for es_system_metric in ['es_cluster', 'es_node']:
            if es_system_metric not in custom_metric_config:
                continue
            es_system_class_name = ''.join([i.capitalize() for i in es_system_metric.split('_')]) + 'Collector'
            collector_class = getattr(collector, es_system_class_name, ...)
            if collector_class is ...:
                logging.error(f"Can't found {es_system_metric} class")
                continue
            collector_instance = collector_class(es_client, custom_metric_config)
            logging.info(f'enable {es_system_metric}. enable_scheduler: {collector_instance.enable_scheduler}')
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
