import logging
import re

from elasticsearch.exceptions import ConnectionTimeout
from prometheus_client.core import GaugeMetricFamily


def collector_up_gauge(metric_name, succeeded=True):
    description = 'Did the {} fetch succeed.'.format(metric_name)
    return GaugeMetricFamily(metric_name + '_up', description, value=int(succeeded))


def interval_handle(_interval):
    try:
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
        return interval
    except Exception:
        raise RuntimeError('Not support interval:{}'.format(_interval))


class BaseCollector(object):
    key = None

    def _get_metric(self):
        raise NotImplementedError

    def get_metric(self):
        raise NotImplementedError

    def gen_job(self):
        raise NotImplementedError

    def collect(self):
        raise NotImplementedError


class BaseEsCollector(BaseCollector):
    def __init__(self, config):
        self.custom_metric_value = None
        self.config = config[self.key]
        self.global_config = config['global']

        self.config['name'] = self.key
        self.enable_scheduler = False
        if 'interval' in self.config:
            self.enable_scheduler = True
            _interval = self.config.get('interval', self.global_config['interval'])
            self.config['interval'] = interval_handle(_interval)

        if 'timeout' not in self.config:
            self.config['timeout'] = self.global_config.get('timeout', None)

        if 'jitter' not in self.config:
            self.config['jitter'] = self.global_config.get('jitter', 0)

        global_config_black_re_list = self.global_config.get('black_re', [])
        black_re_list = self.config.get('black_re', [])
        for black_re in global_config_black_re_list:
            if black_re not in black_re_list:
                black_re_list.append(black_re)
        self.black_re_list = [re.compile(i) for i in black_re_list]

    def is_block(self, metric):
        is_block = False
        for pattern in self.black_re_list:
            if pattern.match(metric):
                is_block = True
                break
        return is_block

    def get_request_param_from_config(self, key_list):
        param_dict = {}
        request_param = self.config.get('request_param', {})
        for key, choice_list, default in key_list:
            value = request_param.get(key, default)
            if value is ...:
                continue
            if choice_list is not ... and key not in choice_list:
                continue
            param_dict[key] = value
        return param_dict

    def auto_gen_metric(self, metric_name, data_dict, metric_doc=''):
        for key, value in data_dict.items():
            _metric_name = metric_name + f'{key}'
            _metric_doc = metric_doc + f' {key}'
            if key == 'timestamp':
                continue
            if type(value) in (int, float):
                yield _metric_name, _metric_doc.strip(), value
            elif type(value) is dict:
                self.auto_gen_metric(metric_name, value)

    def _get_metric(self):
        raise NotImplementedError

    def get_metric(self):
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

    def gen_job(self):
        def _job():
            self.custom_metric_value = self._get_metric()
        return _job, self.config

    def collect(self):
        if self.enable_scheduler:
            if self.custom_metric_value is not None:
                yield from self.custom_metric_value
        else:
            yield from self.get_metric()
