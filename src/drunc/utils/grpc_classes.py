from druncschema.token_pb2 import Token


class DecodedResponse:
    """Decoded response object.

    Warning: This should be kept in sync with
    druncschema/request_response.proto/Response class
    """

    name = None
    token = None
    data = None
    flag = None
    children: list["DecodedResponse"] = []

    def __init__(
        self,
        name: str,
        token: Token,
        flag: object,
        data: object | None = None,
        children: list["DecodedResponse"] | None = None,
    ) -> None:
        """Initialize a DecodedResponse.

        Args:
            name: The name of the response.
            token: The token associated with the response.
            flag: The response flag.
            data: The response data. Defaults to None.
            children: Child responses. Defaults to None.
        """
        self.name = name
        self.token = token
        self.flag = flag
        self.data = data
        if children is None:
            self.children = []
        else:
            self.children = children

    @staticmethod
    def to_string(obj: "DecodedResponse", prefix: str = "") -> str:
        """Convert a DecodedResponse to a string representation.

        Args:
            obj: The DecodedResponse to convert.
            prefix: A prefix to add to the string. Defaults to empty string.

        Returns:
            str: The string representation of the response.
        """
        text = (
            f"{prefix} {obj.name} -> response flag={obj.flag} type={type(obj.data)}\n"
        )
        for v in obj.children:
            if v is None:
                continue
            text += DecodedResponse.to_string(v, prefix + "  ")
        return text

    def __str__(self) -> str:
        """Return string representation of the DecodedResponse.

        Returns:
            str: The string representation.
        """
        return DecodedResponse.to_string(self)
