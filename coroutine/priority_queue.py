class PriorityQueue:
    def __init__(self):
        self.items = []

    def push(self, priority, item):
        self.items.append((priority, item))

    def pop(self):
        try:
            min_priority, min_value = next(self._highest_priority_item())
            self.items.remove((min_priority, min_value))
            return min_priority, min_value
        except StopIteration:
            raise IndexError("pop on an empty priority queue")

    def _highest_priority_item(self):
        if self.is_empty():
            raise IndexError("Operation on an empty priority queue")
        min_priority, min_value = min(self.items, key=lambda item: item[0])  # Find the tuple with the lowest priority
        yield min_priority, min_value
    
    def peek(self):
        try:
            return next(self._highest_priority_item())
        except StopIteration:
            return IndexError("peek on an empty priority queue")

    def is_empty(self):
        return not self.items
    
    def __len__(self):
        return len(self.items)
    
def main():
    print("Priority Queue Test Suite")
    
    # Initialize the priority queue
    pq = PriorityQueue()
    
    print("\nTest 1: Pushing elements")
    pq.push(2, "Task 2")
    pq.push(1, "Task 1")
    pq.push(3, "Task 3")
    print("Elements pushed successfully")
    print("Current queue length:", len(pq))  # Expected: 3
    
    # Test 2: Peek at the highest priority element
    print("\nTest 2: Peeking at the highest priority element")
    peeked = pq.peek()
    print("Peeked element:", peeked)  # Expected: (1, "Task 1")
    
    # Test 3: Pop elements and verify order
    print("\nTest 3: Popping elements")
    print("Pop 1:", pq.pop())  # Expected: (1, "Task 1")
    print("Pop 2:", pq.pop())  # Expected: (2, "Task 2")
    print("Pop 3:", pq.pop())  # Expected: (3, "Task 3")
    
    # Test 4: Check for emptiness
    print("\nTest 4: Checking if the queue is empty")
    print("Is empty:", pq.is_empty())  # Expected: True
    
    # Test 5: Push more elements and check the queue length
    print("\nTest 5: Push more elements and check the queue length")
    pq.push(5, "Task 5")
    pq.push(4, "Task 4")
    print("Ensure the queue is not empty. is_empty:", pq.is_empty())  # Expected: False
    print("Length after pushing:", len(pq))  # Expected: 2
    
    # Test 6: Peek and pop with new elements
    print("\nTest 6: Peek and pop with new elements")
    print("Peek:", pq.peek())  # Expected: (4, "Task 4")
    print("Pop 1:", pq.pop())  # Expected: (4, "Task 4")
    print("Pop 2:", pq.pop())  # Expected: (5, "Task 5")
    
    # Final Check: Ensure the queue is empty
    print("\nFinal Check: Ensure the queue is empty")
    print("Is empty:", pq.is_empty())  # Expected: True


if __name__ == "__main__":
    main()
    
