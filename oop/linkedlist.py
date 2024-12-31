class Node():
    def __init__(self, data):
        self.data = data
        self.next = None

class LinkedList():
    def __init__(self):
        self.head = None
        self.size = 0

    def push(self, data):
        new_node = Node(data)
        if self.head is None:
            self.head = new_node
        else:
            current = self.head
            while current.next is not None:
                current = current.next
            current.next = new_node
        self.size += 1
    
    def pop(self):
        if self.is_empty():
            print("Error: The list is empty.")
            return None
        removed = self.head
        self.head = self.head.next
        self.size -= 1
        return removed
    
    def __len__(self):
        return self.size
    
    def head(self):
        if self.is_empty():
            return None
        return self.head.data

    def is_empty(self):
        return self.head is None

    def __str__(self):
        nodes_values = []
        current = self.head
        while current is not None:
            nodes_values.append(str(current.data))
            current = current.next
        return " -> ".join(nodes_values)

if __name__ == "__main__":
    ll = LinkedList()

    # Test: Check if the list is initially empty
    print("Initial list:")
    print(ll)
    print("Is the list empty?", ll.is_empty())  # True

    # Test: Add elements to the list
    ll.push(10)
    ll.push(20)
    ll.push(30)
    print("\nList after adding elements:")
    print(ll)  # Expected: 10 -> 20 -> 30
    print("Is the list empty?", ll.is_empty())  # False

    # Test: Check the length of the list
    print("\nLength of the list:", len(ll))  

    # Test: Pop elements from the list
    print("\nPopped element:", ll.pop().data)  # Expected: 10
    print("List after popping:")
    print(ll)  # Expected: 20 -> 30
    print("Length of the list:", len(ll))  

    print("\nPopped element:", ll.pop().data)  # Expected: 20
    print("List after popping:")
    print(ll)  # Expected: 30

    # Test: Pop the last element
    print("\nPopped element:", ll.pop().data)  # Expected: 30
    print("List after popping the last element:")
    print(ll)  # Expected: (empty list)

    # Test: Attempt to pop from an empty list
    print("\nAttempting to pop from an empty list:")
    print("Popped element:", ll.pop())  # Expected: Error message and None

    # Test: Add elements after popping everything
    ll.push(40)
    ll.push(50)
    print("\nList after adding elements to an empty list:")
    print(ll)  # Expected: 40 -> 50