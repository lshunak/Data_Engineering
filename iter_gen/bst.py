from dataclasses import dataclass
from typing import Optional, Generator

@dataclass
class Node:
    data: int
    left: Optional["Node"] = None
    right: Optional["Node"] = None

class BST:
    def __init__(self):
        self.root = None

    def insert(self, data: int):
        if self.root is None:
            self.root = Node(data)
        else:
            self._insert(self.root, data)

    def _insert(self, node: Node, data: int):
        if data < node.data:
            if node.left is None:
                node.left = Node(data)
            else:
                self._insert(node.left, data)
        else:
            if node.right is None:
                node.right = Node(data)
            else:
                self._insert(node.right, data)

    def remove(self, data: int):
        self.root = self._remove(self.root, data)

    def _remove(self, node: Optional[Node], data: int):
        if node is None:
            return None
        if data < node.data:
            node.left = self._remove(node.left, data)
        elif data > node.data:
            node.right = self._remove(node.right, data)
        else:
            if node.left is None:
                return node.right
            elif node.right is None:
                return node.left
            
            min_node = self._get_min(node.right)
            node.data = min_node.data
            node.right = self._remove(node.right, min_node.data)
        
        return node
    
    def _get_min(self, node: Node):
        while node.left is not None:
            node = node.left
        return node

    def find(self, data: int):
        return self._find(self.root, data)
        
    def _find(self, node: Optional[Node], data: int):
        if node is None or data == node.data:
            return node
        elif data < node.data:
            return self._find(node.left, data)
        else:
            return self._find(node.right, data)
    
    def inorder(self):
        yield from self._inorder(self.root)

    def _inorder(self, node: Optional[Node]):
        if node is not None:
            yield from self._inorder(node.left)
            yield node.data
            yield from self._inorder(node.right)

    def preorder(self):
        yield from self._preorder(self.root)

    def _preorder(self, node: Optional[Node]):
        if node is not None:
            yield node.data
            yield from self._preorder(node.left)
            yield from self._preorder(node.right)

    def postorder(self):
        yield from self._postorder(self.root)

    def _postorder(self, node: Optional[Node]):
        if node is not None:
            yield from self._postorder(node.left)
            yield from self._postorder(node.right)
            yield node.data

    def __len__(self):
        return sum(1 for _ in self)

    def __contains__(self, data: int):
        return self.find(data)
    
    def __iter__(self):
        yield from self.inorder()
def main():
    # Create a new BST
    bst = BST()
    print("Created an empty BST.")

    # Test: Check if the tree is empty
    print("Is the tree empty? (Should be True):", len(bst) == 0)

    # Test: Insert elements
    print("\nInserting elements: 5, 3, 7, 2, 4")
    bst.insert(5)
    bst.insert(3)
    bst.insert(7)
    bst.insert(2)
    bst.insert(4)

    # Test: Traversals
    print("\nIn-order traversal (Should be [2, 3, 4, 5, 7]):", list(bst))
    print("Pre-order traversal (Should be [5, 3, 2, 4, 7]):", list(bst.preorder()))
    print("Post-order traversal (Should be [2, 4, 3, 7, 5]):", list(bst.postorder()))

    # Test: Check the length of the tree
    print("\nNumber of elements in BST (Should be 5):", len(bst))

    # Test: Check if specific elements exist
    print("Does 5 exist in BST? (Should be True):", 5 in bst)
    print("Does 8 exist in BST? (Should be False):", 8 in bst)

    # Test: Remove an element
    print("\nRemoving element 3.")
    bst.remove(3)

    # Verify after removal
    print("In-order traversal after removal (Should be [2, 4, 5, 7]):", list(bst))
    print("Does 3 exist in BST? (Should be False):", 3 in bst)
    print("Number of elements in BST (Should be 4):", len(bst))

    # Test: Accessing elements lazily using iteration
    print("\nIterating over elements:")
    for i in bst:
        print(i, end=" ")
    print()  # Newline after iteration

    # Test: Edge case - Removing non-existent element
    print("\nAttempting to remove a non-existent element (8).")
    bst.remove(8)
    print("In-order traversal after no-op removal (Should still be [2, 4, 5, 7]):", list(bst))

    # Test: Edge case - Removing root
    print("\nRemoving root element (5).")
    bst.remove(5)
    print("In-order traversal after removing root (Should be [2, 4, 7]):", list(bst))

    # Test: Attempting to remove elements until empty
    print("\nRemoving all elements one by one.")
    bst.remove(7)
    bst.remove(2)
    bst.remove(4)
    print("Is the tree empty? (Should be True):", len(bst) == 0)
    print("In-order traversal of empty tree (Should be []):", list(bst))


if __name__ == "__main__":
    main()
