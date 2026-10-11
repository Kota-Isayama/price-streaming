from dataclasses import dataclass

from .common import Money


@dataclass(frozen=True, slots=True)
class NotionalSchedule:
    """
    現時点ではconstant notionalのみ。

    将来的には、
      - amortizing
      - accreting
      - step schedule
    をここへ拡張できる。
    """

    initial_value: Money
    