import math

def circle_area(radius):
    return 3.14159 * math.pow(radius, 2)

def triangle_area(base, height):
    return 0.5 * base * height

def rectangle_area(length, width):
    return length * width

if __name__ == '__main__':
    print(circle_area(5))
    print(triangle_area(10, 5))
    print(rectangle_area(10, 5))