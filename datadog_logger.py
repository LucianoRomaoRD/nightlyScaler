from logging.handlers import QueueHandler, QueueListener
import queue
import socket
from os import getenv
import logging
import time
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem
from datadog_api_client.v2.model.content_encoding import ContentEncoding

log_format = "%(asctime)s - %(levelname)s - %(message)s"

#https://docs.datadoghq.com/api/latest/logs/?site=us5
class RemoteLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        api_key = getenv("dd_api_key_rdsm")
        app_key = getenv("dd_app_key_rdsm")
        dd_site = getenv("dd_site", "datadoghq.com")

        cfg = Configuration()
        cfg.api_key["apiKeyAuth"] = api_key
        cfg.api_key["appKeyAuth"] = app_key
        cfg.host = "https://http-intake.logs.us5.datadoghq.com"

        self._api_client = ApiClient(configuration=cfg)
        self._api = LogsApi(self._api_client)

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        ts = int(time.time() * 1e9)
        item = HTTPLogItem(
            ddsource="python",
            ddtags="env:production,service:nightlyscaler",
            host=socket.gethostname(),
            message=msg,
            service="nightlyscaler",
            timestamp=ts,
            status=record.levelname.lower()
        )
        body = HTTPLog([item])
        try:
            # envia com DEFLATE explicitamente
            a= self._api.submit_log(body=body, content_encoding=ContentEncoding.DEFLATE)
        except Exception:
            self.handleError(record)

class DataDogLogger(logging.Logger):
    def __init__(self, name, level=logging.NOTSET):
        super(DataDogLogger, self).__init__(name, level)

        formatter = logging.Formatter(log_format)

        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.addHandler(handler)

        log_queue = queue.Queue(-1)
        queue_handler = QueueHandler(log_queue)
        self.addHandler(queue_handler)

        remote_handler = RemoteLogHandler()
        self.queue_listener = QueueListener(log_queue, remote_handler)
        self.queue_listener.start()
    
    def stop(self):
        self.queue_listener.stop()

    def debug(self, msg, *args, **kwargs):
        super().debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        super().info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        super().warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        super().error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        super().critical(msg, *args, **kwargs)