"""
-*- coding: utf-8 -*-
@File  : Component.py
@author: caoqinghua
@Time  : 2023/12/19 13:13
"""
import xxhash
import random
import math
from Set_parameter import *


class Node:
    """Doubly linked list node class, storing the value of the node, pointers to previous and next nodes, and additional metadata."""

    def __init__(self, val):
        self.val = val            # The value of the node
        self.pre = None           # The previous node
        self.next = None          # The next node
        self.gap = 0              # Custom field, can store the gap between data
        self.src = 0              # Source identifier


class DoubleLinkedList:
    """Doubly linked list class providing operations like insertion, deletion, and traversal."""

    def __init__(self):
        """Initialize the doubly linked list with a head node."""
        self.head = Node(0)        # Head node
        self.tail = self.head      # Tail node, initially the same as the head node

    def is_empty(self):
        """Check if the list is empty."""
        return self.head.next is None

    def get_length(self):
        """Calculate the length of the list."""
        length = 0
        cur_node = self.head.next
        while cur_node:
            length += 1
            cur_node = cur_node.next
        return length

    def add_last(self, node):
        """Add a node at the end of the list."""
        if self.is_empty():
            self.head.next = node
            node.pre = self.head
            self.tail = node
        else:
            last_node = self.tail
            last_node.next = node
            node.pre = last_node
            node.next = None
            self.tail = node

    def shift_node(self, node):
        """Move the node to the tail of the list."""
        if node == self.tail:
            return  # Skip if the node is already at the tail
        else:
            # Update pointers of the previous and next nodes
            node.pre.next = node.next
            if node.next:
                node.next.pre = node.pre
            node.pre = self.tail
            self.tail.next = node
            node.next = None
            self.tail = node

    def remove_old_node(self):
        """Remove the head node of the list."""
        if self.is_empty():
            print("Failed to remove, the list is empty.")
            return False
        else:
            # Remove the head node and update the pointers
            first_node = self.head.next
            self.head.next = first_node.next
            if first_node.next:
                first_node.next.pre = self.head
            first_node.pre = None
            first_node.next = None
            return True

    def traversal(self):
        """Traverse the list and return a list of all nodes."""
        nodes = []
        cur_node = self.head.next
        if self.is_empty():
            print("The list is empty!")
            return nodes
        while cur_node:
            nodes.append(cur_node)
            cur_node = cur_node.next
        return nodes


class CountMin:
    """CountMin Sketch structure, used for approximate frequency estimation."""

    def __init__(self, d, w):
        """
        Initialize the CountMin structure.
        :param d: Number of hash functions
        :param w: Width of each hash table row
        """
        self.d = d  # Number of hash functions
        self.w = w  # Width of hash tables
        self.CM = [[0] * w for _ in range(d)]  # Initialize a d x w 2D array

    def CM_update(self, pos):
        """Update the CountMin table by incrementing the frequency at the given position."""
        pos = str(pos)
        global bias
        for i in range(self.d):
            # Use xxhash to generate a hash value and map it to the table range
            hash_value = xxhash.xxh64_intdigest(pos, seed=2024 + bias[i]) % self.w
            self.CM[i][hash_value] += 1

    def CM_decrease(self, pos):
        """Decrease the frequency at the specified position."""
        pos = str(pos)
        global bias
        D_val = []
        for i in range(self.d):
            hash_value = xxhash.xxh64_intdigest(pos, seed=2024 + bias[i]) % self.w
            self.CM[i][hash_value] -= 1
            D_val.append(self.CM[i][hash_value])
        return D_val

    def get_CM_value(self, pos):
        """Get the frequency estimate at the given position."""
        pos = str(pos)
        global bias
        frequency = []
        for i in range(self.d):
            hash_value = xxhash.xxh64_intdigest(pos, seed=2024 + bias[i]) % self.w
            frequency.append(self.CM[i][hash_value])
        return frequency


# Enhanced functionality and complexity
class AdvancedCountMin(CountMin):
    """Extended CountMin Sketch structure, with custom hashing strategy and optimization features."""

    def __init__(self, d, w, hash_function=None):
        """
        Initialize the extended CountMin structure with support for custom hash functions.
        :param d: Number of hash functions
        :param w: Width of hash tables
        :param hash_function: Optional custom hash function
        """
        super().__init__(d, w)
        self.hash_function = hash_function if hash_function else xxhash.xxh64_intdigest

    def CM_update(self, pos, custom_seed=None):
        """Update the CountMin table using a custom hash function."""
        pos = str(pos)
        global bias
        seed = custom_seed if custom_seed else 2024
        for i in range(self.d):
            # Use the custom hash function to generate the hash value
            hash_value = self.hash_function(pos, seed=seed + bias[i]) % self.w
            self.CM[i][hash_value] += 1

    def CM_decrease(self, pos, custom_seed=None):
        """Decrease the frequency using a custom hash function."""
        pos = str(pos)
        global bias
        D_val = []
        seed = custom_seed if custom_seed else 2024
        for i in range(self.d):
            hash_value = self.hash_function(pos, seed=seed + bias[i]) % self.w
            self.CM[i][hash_value] -= 1
            D_val.append(self.CM[i][hash_value])
        return D_val

    def get_custom_hash_value(self, pos, custom_seed=None):
        """Get the frequency estimate using the custom hash function."""
        pos = str(pos)
        global bias
        frequency = []
        seed = custom_seed if custom_seed else 2024
        for i in range(self.d):
            hash_value = self.hash_function(pos, seed=seed + bias[i]) % self.w
            frequency.append(self.CM[i][hash_value])
        return frequency
