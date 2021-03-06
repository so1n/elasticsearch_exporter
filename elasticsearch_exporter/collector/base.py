import logging
import re
from typing import Any, Callable, Dict, Generator, List, Tuple, Optional, Set, Union
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionTimeout
from prometheus_client.core import GaugeMetricFamily


def collector_up_gauge(metric_name: str, succeeded: bool = True) -> GaugeMetricFamily:
    description = 'Did the {} fetch succeed.'.format(metric_name)
    return GaugeMetricFamily(metric_name + '_up', description, value=int(succeeded))


def interval_handle(_interval: str) -> int:
    try:
        try:
            interval: int = int(_interval)
        except ValueError:
            interval: int = int(_interval[:-1])
            unit: str = _interval[-1]
            if unit == 's':
                pass
            elif unit == 'm':
                interval = interval * 60
            elif unit == 'h':
                interval = interval * 60 * 60
        return interval
    except Exception:
        raise RuntimeError('Not support interval:{}'.format(_interval))


class BaseEsCollector(object):
    key: Optional[str] = None

    def __init__(self, es_client, config: Dict[str, Any]):
        self.es_client: 'Elasticsearch' = es_client
        self.custom_metric_value: Optional[GaugeMetricFamily] = None
        self.config: Dict[str, Any] = config[self.key]
        self.global_config: Dict[str, Any] = config['global']

        self.config['name'] = self.key
        self.enable_scheduler: bool = False
        if 'interval' in self.config:
            _interval: str = self.config.get('interval', self.global_config['interval'])
            if _interval != 'disable':
                self.enable_scheduler = True
                _interval: int = interval_handle(_interval)
            self.config['interval'] = _interval

        if 'jitter' not in self.config:
            self.config['jitter'] = self.global_config.get('jitter', 0)

        global_config_black_re_list: List[str] = self.global_config.get('black_re', [])
        black_re_list: List[str] = self.config.get('black_re', [])
        for black_re in global_config_black_re_list:
            if black_re not in black_re_list:
                black_re_list.append(black_re)
        self._black_re_list: List[re.Pattern[str]] = [re.compile(i) for i in black_re_list]
        self._black_metric_set: Set[str] = set()

    def _is_block(self, metric: str) -> bool:
        is_block: bool = False
        if metric in self._black_metric_set:
            return True

        for pattern in self._black_re_list:
            if pattern.match(metric):
                is_block = True
                self._black_metric_set.add(metric)
                break
        return is_block

    def auto_gen_metric(
            self, metric_name: str, data_dict: Dict[str, Any], metric_doc: str = ''
    ) -> Generator[Tuple[str, str, Any], None, None]:
        for key, value in data_dict.items():
            _metric_name = metric_name + f'{key}'
            _metric_doc = metric_doc + f' {key}'
            value_type = type(value)
            if key == 'timestamp':
                continue
            if value_type in (int, float):
                yield _metric_name, _metric_doc.strip(), value
            elif value_type is list:
                for i in value:
                    yield from self.auto_gen_metric(metric_name, i)
            elif value_type is dict:
                yield from self.auto_gen_metric(metric_name, value)

    def _get_metric(self):
        raise NotImplementedError

    def get_metric(self) -> Generator[GaugeMetricFamily, None, None]:
        try:
            yield from self._get_metric()
        except ConnectionTimeout:
            logging.warning(f'fetching{self.key} timeout')
            yield collector_up_gauge(self.key, succeeded=False)
        except Exception as e:
            logging.warning(f'fetching error: {self.key} error:{e}')
            yield collector_up_gauge(self.key, succeeded=False)
        else:
            yield collector_up_gauge(self.key)

    def gen_job(self) -> Tuple[Callable, Dict[str, Any]]:
        def _job():
            self.custom_metric_value = self._get_metric()
        return _job, self.config

    def collect(self) -> Generator[GaugeMetricFamily, None, None]:
        if self.enable_scheduler:
            if self.custom_metric_value is not None:
                yield from self.custom_metric_value
        else:
            yield from self.get_metric()
