import argparse
import logging
import os
import sys

import yaml
from logging import Handler
from logging.handlers import SysLogHandler
from typing import Any, Dict, List, Optional, Type

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
    parser.add_argument("-c", "--config", default='./config.yaml', help='metric config; default use ./config.yml')
    parser.add_argument("--es_cluster", help='es node host. eg 127.0.0.1:9200 or 127.0.0.1:9200,127.0.0.2:9200')
    parser.add_argument("--listen_port", default=9206, help='server port that provide data to prometheus')
    parser.add_argument("--log_level", default='INFO', help='log level')
    parser.add_argument("--apscheduler_log_level", default='WARNING', help='scheduler log level(when scheduler enable)')
    parser.add_argument(
        "--syslog_address",
        help="syslog address, enable syslog handle when value is not empty, "
             "If you want to send to the local, the value is '/dev/log'"
    )
    parser.add_argument(
        "--syslog_facility",
        help="syslog facility, can only be used when syslog is enabled",
        choices=SysLogHandler.facility_names.keys()
    )
    args, unknown = parser.parse_known_args()

    log_level: str = args.log_level
    apscheduler_log_level: str = args.apscheduler_log_level
    syslog_address: str = args.syslog_address

    basicConfig = dict(
        format='[%(asctime)s %(levelname)s %(process)d] %(message)s',
        datefmt='%y-%m-%d %H:%M:%S',
        level=getattr(logging, log_level.upper(), 'INFO'),
    )

    if syslog_address:
        syslog_facility: int = SysLogHandler.facility_names.get(args.syslog_facility, 'user')
        basicConfig.update(
            dict(
                handlers=[SysLogHandler(address=syslog_address, facility=syslog_facility)],
                format='%(levelname)s elasticsearch_exporter %(message)s',
            )
        )
        del basicConfig['datefmt']

    logging.basicConfig(**basicConfig)

    if not args.es_cluster:
        logging.error('not found es cluster. exit....')
        sys.exit()

    es_cluster_list: List[str] = args.es_cluster.split(',')
    listen_port: int = int(args.listen_port)
    config_filename_path: str = args.config

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
        has_generator_metric: bool = False
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
            else:
                has_generator_metric = True

        logging.getLogger('apscheduler.executors.default').setLevel(getattr(logging, apscheduler_log_level))
        if not scheduler.get_jobs():
            if not has_generator_metric:
                logging.error("not found scheduler job and generator metric job")
                sys.exit()
            else:
                logging.info("not found scheduler job")
        else:
            scheduler.start()
    else:
        logging.error("Can't found config path. exit ...")
        sys.exit()
