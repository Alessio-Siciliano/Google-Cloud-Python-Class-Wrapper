""" This module represents a generic object. """


class Generic:
    """This class is the core of the Generic object."""

    def set_attribute(self, attribute_name: str, attribute_value) -> None:
        """Set an attribute by its name and value

        Parameters
        ----------
        attribute_name : str
            Name of the attribute

        attribute_value:
            Value of the attribute

        Returns
        -------
        None
        """
        if hasattr(self, attribute_name):
            setattr(self, attribute_name, attribute_value)

    def get_attribute(self, attribute_name: str):
        """Get an attribute by its name

        Parameters
        ----------
        attribute_name : str
            Name of the attribute

        Returns
        -------
        attribute_value:
            If the attribute exists the function returns its value
            otherwise None
        """
        return getattr(self, attribute_name, None)
