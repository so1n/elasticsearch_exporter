import logging
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
    def _get_metric(self):
        raise NotImplementedError

    def get_metric(self):
        raise NotImplementedError

    def gen_job(self):
        raise NotImplementedError

    def collect(self):
        raise NotImplementedError


class BaseEsCollector(BaseCollector):
    def __init__(self, config, key):
        self.key = key
        self.custom_metric_value = None
        self.config = config[key]
        self.global_config = config['global']

        self.enable_scheduler = False
        if 'interval' in self.config:
            self.enable_scheduler = True
            _interval = self.config.get('interval', self.global_config['interval'])
            self.config['interval'] = interval_handle(_interval)

        if 'timeout' not in self.config:
            self.config['timeout'] = self.global_config['timeout']

    def _get_metric(self):
        raise NotImplementedError

    def get_metric(self):
        try:
            yield from self._get_metric()
        except ConnectionTimeout:
            logging.warning(f'fetching{self.key} timeout')
            yield collector_up_gauge(self.key, succeeded=False)
        except Exception:
            logging.warning(f'fetching error: {self.key}')
            yield collector_up_gauge(self.key, succeeded=False)
        else:
            yield collector_up_gauge(self.key)

    def gen_job(self):
        def _job():
            self.custom_metric_value = self._get_metric()
        return _job, self.config['interval'], self.key

    def collect(self):
        if self.enable_scheduler:
            if self.custom_metric_value is not None:
                yield from self.custom_metric_value
        else:
            yield from self.get_metric()
