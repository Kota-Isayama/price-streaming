from ctypes import ARRAY
from datetime import datetime

from sqlalchemy import JSON, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from request_for_quote.application.port.pricing_request_loader import IPricingRequestLoader
from request_for_quote.infrastructure.postgres.base import Base


class SqlAlchemyPricingRequestLoader(IPricingRequestLoader):
    def __init__(self, )