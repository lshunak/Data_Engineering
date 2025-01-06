class Property:
    def __init__(self, getter): # getter is passed from @Property
        self.getter = getter
        self.setter = None
        self.deleter = None
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        if self.getter is None:
            raise AttributeError("unreadable attribute")  # case of missing getter
        return self.getter(instance) #call to the getter method
    
    def __set__(self, instance, value):
        if self.setter is None:
            raise AttributeError("can't set attribute")
        self.setter(instance, value)
    
    def __delete__(self, instance):
        if self.deleter is None:
            raise AttributeError("can't delete attribute")
        self.deleter(instance)

    def Setter(self, setter):
        self.setter = setter
        return self
    
    def Deleter(self, deleter):
        self.deleter = deleter
        return self

if __name__ == "__main__":
    class X:     
        def __init__(self, val):    
            self.__x = int(val)     
        @Property     
        def x(self):     
            return self.__x

        @x.Setter     
        def x(self, val):     
            self.__x = int(val)    

        @x.Deleter
        def x(self):
            del self.__x

    print(X(0).x) # 0
    a = X(0)
    print(a.x) # 0
    a.x = 1
    print(a.x) # 1
    del a.x
    print(a.x) # 1
