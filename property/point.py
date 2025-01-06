class Point:
    """
    A class to represent a point in a 2D space.

    Attributes:
        x (float or int): The x-coordinate of the point. Must be a number.
        y (float or int): The y-coordinate of the point. Must be a number.
    """
    
    def __init__(self, x=0, y=0):
        """
        Initialize the point with x and y coordinates.

        Args:
            x (float or int): The x-coordinate (default is 0).
            y (float or int): The y-coordinate (default is 0).

        Raises:
            TypeError: If x or y are not numbers.
        """
        self._x = None
        self._y = None
        self.set_x(x)
        self.set_y(y)

    def get_x(self):
        """Get the x-coordinate of the point."""
        return self._x

    def set_x(self, value):
        """
        Set the x-coordinate of the point.

        Args:
            value (float or int): The new x-coordinate.

        Raises:
            TypeError: If the value is not a number.
        """
        if not isinstance(value, (int, float)):
            raise TypeError("x-coordinate must be a number.")
        self._x = value

    def get_y(self):
        """Get the y-coordinate of the point."""
        return self._y

    def set_y(self, value):
        """
        Set the y-coordinate of the point.

        Args:
            value (float or int): The new y-coordinate.

        Raises:
            TypeError: If the value is not a number.
        """
        if not isinstance(value, (int, float)):
            raise TypeError("y-coordinate must be a number.")
        self._y = value

    # Define properties using the property() function
    x = property(get_x, set_x, doc="Get or set the x-coordinate. Must be a number.")
    y = property(get_y, set_y, doc="Get or set the y-coordinate. Must be a number.")

    def __repr__(self):
        """
        Return a string representation of the Point instance.

        Returns:
            str: A string in the format 'Point(x=<value>, y=<value>)'.
        """
        return f"Point(x={self.x}, y={self.y})"


if __name__ == "__main__":
    # Example usage
    p = Point(1.5, 2)
    print(p)  # Output: Point(x=1.5, y=2)

    p.x = 3
    print(p.x)  # Output: 3

    try:
        p.x = "invalid"  # Raises TypeError
    except TypeError as e:
        print(e)  # Output: x-coordinate must be a number.

    try:
        del p.x  # Raises AttributeError
    except AttributeError as e:
        print(e)  # Output: AttributeError: cannot delete attribute

    help(Point)
