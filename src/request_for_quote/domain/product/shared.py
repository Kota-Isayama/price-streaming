import enum


class Currency(enum.StrEnum):
    JPY = "JPY"

    def to_str(self) -> str:
        return str(self.value)
    