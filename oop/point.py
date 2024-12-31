import math
class Point:
    def __init__(self, a=0.0, b=0.0):
        if not isinstance(a, (int, float) or not isinstance(a, (int, float))):
            print("Error: Invalid input. Please provide numbers.")
            self.x = 0.0
            self.y = 0.0
        else:
            self.x = a
            self.y = b
        
    def distance_from_origin(self):
        return math.sqrt(self.x**2 + self.y**2)
    
if __name__ == "__main__":
    p = Point(1, 2.5)
    print(p.x)
    print(p.y)
    print(p.distance_from_origin())
    p = Point("a", 2.5)