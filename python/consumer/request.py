import requests

from consumer_config import endpoint
from model import Validation


def send_json(validation: Validation):
    requests.post(url=endpoint, data=validation.to_json())
