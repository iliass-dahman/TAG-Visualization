import requests

from producer_config import endpoint
from model import Validation


def send_json(validation: Validation):
    requests.post(url=endpoint, data=validation.to_json())
