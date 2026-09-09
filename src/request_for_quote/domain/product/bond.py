import dataclasses


@dataclasses.dataclass(frozen=True)
class Bond:
    isin_code: str
    