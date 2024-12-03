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
    """双向链表节点类，存储节点的值、指向前后节点的指针以及额外的元数据"""

    def __init__(self, val):
        self.val = val            # 节点的值
        self.pre = None           # 前驱节点
        self.next = None          # 后继节点
        self.gap = 0              # 自定义字段，可以存储数据间的差距
        self.src = 0              # 源标识符


class DoubleLinkedList:
    """双向链表类，提供对节点的插入、删除、遍历等操作"""

    def __init__(self):
        """初始化双向链表，创建一个头节点"""
        self.head = Node(0)        # 头节点
        self.tail = self.head      # 尾节点，初始时与头节点相同

    def is_empty(self):
        """检查链表是否为空"""
        return self.head.next is None

    def get_length(self):
        """计算链表的长度"""
        length = 0
        cur_node = self.head.next
        while cur_node:
            length += 1
            cur_node = cur_node.next
        return length

    def add_last(self, node):
        """在链表尾部添加节点"""
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
        """将节点移动到链表尾部"""
        if node == self.tail:
            return  # 如果节点已经是尾节点，跳过操作
        else:
            # 更新节点前后节点的指针
            node.pre.next = node.next
            if node.next:
                node.next.pre = node.pre
            node.pre = self.tail
            self.tail.next = node
            node.next = None
            self.tail = node

    def remove_old_node(self):
        """删除链表头部节点"""
        if self.is_empty():
            print("删除失败，链表为空")
            return False
        else:
            # 删除头节点并更新指针
            first_node = self.head.next
            self.head.next = first_node.next
            if first_node.next:
                first_node.next.pre = self.head
            first_node.pre = None
            first_node.next = None
            return True

    def traversal(self):
        """遍历链表并返回所有节点的列表"""
        nodes = []
        cur_node = self.head.next
        if self.is_empty():
            print("链表为空！")
            return nodes
        while cur_node:
            nodes.append(cur_node)
            cur_node = cur_node.next
        return nodes


class CountMin:
    """CountMin Sketch结构，用于近似频率估计"""

    def __init__(self, d, w):
        """
        初始化CountMin结构
        :param d: 哈希函数的数量
        :param w: 每行哈希表的宽度
        """
        self.d = d  # 哈希函数数量
        self.w = w  # 哈希表宽度
        self.CM = [[0] * w for _ in range(d)]  # 初始化一个d行w列的二维数组

    def CM_update(self, pos):
        """更新CountMin表，增加指定位置的频率"""
        pos = str(pos)
        global bias
        for i in range(self.d):
            # 使用xxhash生成哈希值并映射到表的范围内
            hash_value = xxhash.xxh64_intdigest(pos, seed=2024 + bias[i]) % self.w
            self.CM[i][hash_value] += 1

    def CM_decrease(self, pos):
        """减少指定位置的频率"""
        pos = str(pos)
        global bias
        D_val = []
        for i in range(self.d):
            hash_value = xxhash.xxh64_intdigest(pos, seed=2024 + bias[i]) % self.w
            self.CM[i][hash_value] -= 1
            D_val.append(self.CM[i][hash_value])
        return D_val

    def get_CM_value(self, pos):
        """获取指定位置的频率估计值"""
        pos = str(pos)
        global bias
        frequency = []
        for i in range(self.d):
            hash_value = xxhash.xxh64_intdigest(pos, seed=2024 + bias[i]) % self.w
            frequency.append(self.CM[i][hash_value])
        return frequency


# 增强的功能和复杂性
class AdvancedCountMin(CountMin):
    """扩展版CountMin Sketch结构，加入自定义的哈希策略和优化功能"""

    def __init__(self, d, w, hash_function=None):
        """
        初始化扩展版CountMin结构，支持自定义哈希函数
        :param d: 哈希函数数量
        :param w: 每行哈希表的宽度
        :param hash_function: 可选的自定义哈希函数
        """
        super().__init__(d, w)
        self.hash_function = hash_function if hash_function else xxhash.xxh64_intdigest

    def CM_update(self, pos, custom_seed=None):
        """使用自定义哈希函数更新CountMin表"""
        pos = str(pos)
        global bias
        seed = custom_seed if custom_seed else 2024
        for i in range(self.d):
            # 使用自定义哈希函数生成哈希值
            hash_value = self.hash_function(pos, seed=seed + bias[i]) % self.w
            self.CM[i][hash_value] += 1

    def CM_decrease(self, pos, custom_seed=None):
        """使用自定义哈希函数减少频率"""
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
        """获取自定义哈希函数下的频率估计值"""
        pos = str(pos)
        global bias
        frequency = []
        seed = custom_seed if custom_seed else 2024
        for i in range(self.d):
            hash_value = self.hash_function(pos, seed=seed + bias[i]) % self.w
            frequency.append(self.CM[i][hash_value])
        return frequency


