from typing import ParamSpec, Protocol, TypeVar

P = ParamSpec("P")
R = TypeVar("R", covariant=True)


class TargetFunc(Protocol[P, R]):
    """A generic protocol that dynamically matches the signature of the passed callable."""

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...
