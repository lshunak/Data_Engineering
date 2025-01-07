class TreeNode: 
	def __init__(self, value=0, left=None, right=None): 
		self.value = value 
		self.left = left 
		self.right = right 

def pruneTree(root: TreeNode) -> TreeNode: 
    if not root: 
        return None
    
    root.left = pruneTree(root.left)
    root.right = pruneTree(root.right)

    if not root.left and not root.right and root.value == 0: 
        return None
    
    return root

def printTree(root, prefix="", is_left=True):
    if not root:
        return

    print(prefix + ("└── " if is_left else "├── ") + str(root.value))
    
    new_prefix = prefix + ("    " if is_left else "│   ")
    
    if root.right:
        printTree(root.right, new_prefix, False)
    if root.left:
        printTree(root.left, new_prefix, True)

def print2DTree(root, space=0, LEVEL_SPACE = 5):
    if (root == None): return
    space += LEVEL_SPACE
    print2DTree(root.right, space)
    #print() # neighbor space
    for i in range(LEVEL_SPACE, space): print(end = " ")  
    print("|" + str(root.value) + "|<")
    print2DTree(root.left, space)

if __name__ == "__main__":
    root = TreeNode(0)
    root.left = TreeNode(1)
    root.right = TreeNode(0)
    root.right.left = TreeNode(1)
    root.right.right = TreeNode(0)
    root.right.left.left = TreeNode(0)
    root.right.left.right = TreeNode(0)

    print("Original Tree:")
    print2DTree(root)  

    pruned_root = pruneTree(root)

    print("\nPruned Tree:")
    print2DTree(pruned_root) 