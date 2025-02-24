import math
import random
import time
import struct
import hashlib
import mmh3
import xxhash
import pandas as pd
import numpy as np
import random
from Component import *
from Set_parameter import *
import time


class PackedVector:
    def __init__(self, register_size, sketch_size):
        self.bits_per_entry = register_size
        self.data = [0] * sketch_size

    def get(self, index):
        return self.data[index]

    def set(self, index, value):
        self.data[index] = value


def argmin(array, m):
    min_val = array.get(0)
    min_idx = 0
    for i in range(1, m):
        if array.get(i) < min_val:
            min_val = array.get(i)
            min_idx = i
    return min_idx


def initial_value(sketch, m):
    tmp_sum = 0.0
    for i in range(m):
        tmp_sum += 2 ** (-sketch.get(i))
    return (m - 1) / tmp_sum


def f_func(sketch, k, w):
    res = 0.0
    e = math.e
    for i in range(k):
        x = 2 ** (-sketch.get(i) - 1)
        ex = e ** (w * x)
        res += x * (2 - ex) / (ex - 1)
    return res

def df_func(sketch, k, w):
    res = 0
    for i in range(k):
        x = 2 ** (-sketch.get(i) - 1)
        exponent = w * x

        # 数值稳定处理
        if exponent > 500:  # 当w*x超过700时，近似视为0
            continue
        elif exponent < -500:  # 处理负指数溢出
            ex = 0.0
        else:
            ex = math.exp(exponent)

        # 处理分母接近0的情况
        denominator = (ex - 1) ** 2
        if denominator < 1e-20:  # 极小时用泰勒展开近似
            term = -x ** 2 * ex / (x ** 2 * w ** 2 + 1e-20)
        else:
            term = -x ** 2 * ex / denominator

        res += term
    return res


def newton(sketch, k, c0):
    err = 1e-5
    max_iterations = 100  # 新增最大迭代限制
    c1 = c0 - f_func(sketch, k, c0) / df_func(sketch, k, c0)
    iterations = 0
    while abs(c1 - c0) > err and iterations < max_iterations:
        c0 = c1
        c1 = c0 - f_func(sketch, k, c0) / df_func(sketch, k, c0)
        iterations += 1
    return c1


class QSketch:
    def __init__(self, sketch_size, register_size):
        self.register_size = register_size
        self.sketch_size = sketch_size
        self.range = 2 ** register_size
        self.r_max = self.range - 1
        self.r_min = 0
        self.pii = list(range(sketch_size))
        self.seed = [random.getrandbits(64) for _ in range(sketch_size)]
        self.estimated_card = 0.0
        self.update_time = 0.0
        self.estimation_time = 0.0
        self.qs = PackedVector(register_size, sketch_size)
        self.win = window_size
        self.back = [0] * self.win

    def get_index(self, dst, j):
        hash_int = xxhash.xxh64_intdigest(dst, seed=2025224+j)
        return hash_int / (2 ** 64)

    def update(self, data):
        start_time = time.time()
        global row_length, print_LC_gap
        for t in range(window_size):
            pi = self.pii.copy()
            r = 0.0
            j_min = 0
            for i in range(self.sketch_size):
                u = self.get_index(data[t], i)
                r -= math.log(u) / (1*(self.sketch_size - i+1))
                y = int(math.floor(-math.log2(r)))
                if y <= self.qs.get(j_min):
                    break
                jj = random.randint(i, self.sketch_size - 1)
                pi[i], pi[jj] = pi[jj], pi[i]

                if y > self.qs.get(pi[i]):
                    if self.r_min < y < self.r_max:
                        self.qs.set(pi[i], y)
                    elif y >= self.r_max:
                        self.qs.set(pi[i], self.r_max)
                    else:
                        continue
                    if pi[i] == j_min:
                        j_min = argmin(self.qs, self.sketch_size)
        self.update_time = time.time() - start_time

    def estimate_card(self):
        start_time = time.time()
        c0 = initial_value(self.qs, self.sketch_size)
        if not (0 < c0 < 1e6):
            c0 = 1.0  # 重置为安全值
        self.estimated_card = newton(self.qs, self.sketch_size, c0)
        res = self.estimated_card
        self.estimation_time = time.time() - start_time
        return res

def Prepare(file_csv, file_real):
    df_source = pd.read_csv(file_csv, usecols=['src'])
    source = df_source['src'].values
    df_real = pd.read_csv(file_real, usecols=['real-cardinality'])
    real_num = df_real['real-cardinality'].values
    return source, real_num

if __name__ == '__main__':
    global where_datastream, where_stream_realcar, window_size
    # 数据准备初始化
    file_path = where_datastream
    file_realnum = where_stream_realcar
    source, real_num = Prepare(file_csv=file_path, file_real=file_realnum)
    step = int(0.5*window_size)
    for i in range(10):
        qsketch = QSketch(sketch_size=512, register_size=8)
        source1 = source[0+step*i:window_size+step*i]
        qsketch.update(source1)
        # 估计基数
        qsketch.estimate_card()
        print(f"Estimated cardinality: {qsketch.estimated_card}")