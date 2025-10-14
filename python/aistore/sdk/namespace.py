from pydantic import BaseModel


class Namespace(BaseModel):
    """
    A bucket namespace defined by the uuid of the cluster and a name
    """

    uuid: str = ""
    name: str = ""

    def get_path(self) -> str:
        """
        Get the AIS-style path representation of the string -- @uuid#name

        Returns:
            Formatted namespace string
        """
        return f"@{self.uuid}#{self.name}"
