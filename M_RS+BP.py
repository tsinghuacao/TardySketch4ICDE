"""
-*- coding: utf-8 -*-
@File  : M_RS+BP.py
@author: caoqinghua
@Time  : 2024/03/06 16:24
"""

import mmh3
import xxhash
import pandas as pd
import numpy as np
import random
import time
from Component import *
from Set_parameter import *

class LinearCounting:
    """线性计数算法，用于基数估计"""

    def __init__(self, m, win):
        """
        初始化线性计数算法
        :param m: 计数表大小
        :param win: 窗口大小
        """
        self.m = m
        self.win = win
        self.LC = []  # 计数表
        self._initialize_lc()

    def _initialize_lc(self):
        """初始化计数表"""
        self.LC = [Node(0) for _ in range(self.m)]

    def _get_index(self, dst):
        """获取数据源dst的哈希索引"""
        res = xxhash.xxh64_intdigest(dst, seed=20240417)
        return res

    def get_estimation(self):
        """基于线性计数公式估算基数"""
        res = -self.m * np.log((self.m - self.LC[0].val) / self.m)
        return res

    def _calculate_average_gap(self):
        """计算LC表中所有节点的平均间隔"""
        total_gap = sum(node.gap for node in self.LC)
        return total_gap / self.m

    def update(self, lru, CM, source, real_num):
        """更新计数表，并根据窗口滑动调整"""
        LC_estimates = []
        cnt = 0
        cnt_out = 0
        es_out = []
        for i in range(len(source)):
            # 获取数据源的哈希索引并计算bit index
            hash_val = self._get_index(source[i])
            bit_index = hash_val % self.m

            # LC表和LRU更新
            bit_val = self.LC[bit_index].val
            CM.CM_update(bit_index)
            if bit_val == 0:
                self.LC[bit_index].val = 1
                lru.head.val += 1
                lru.add_last(self.LC[bit_index])
            else:
                self.LC[bit_index].pre.gap += (self.LC[bit_index].gap + 1)
                self.LC[bit_index].gap = 0
                lru.shift_node(self.LC[bit_index])

            # 窗口滑动机制
            if cnt >= self.win:
                first_node_index = self.LC.index(lru.head.next)
                e_mode = lru.head.gap

                if e_mode == 0:
                    temp_flag1 = min(CM.CM_decrease(str(first_node_index)))
                    if temp_flag1 <= 0:
                        lru.head.gap = self.LC[first_node_index].gap
                        self.LC[first_node_index].gap = 0
                        self.LC[first_node_index].val = 0
                        lru.remove_old_node()
                        lru.head.val -= 1
                else:
                    while True:
                        hpos = random.randint(0, self.m - 1)
                        hf = min(CM.get_CM_value(hpos))
                        if hf > 1:
                            break
                    CM.CM_decrease(hpos)
                    lru.head.gap -= 1

                # 打印校验结果
                if (cnt - self.win) % print_LC_gap == 0:
                    estimate_num = self.get_estimation()
                    LC_estimates.append(estimate_num)
                    print(f"当前处理至第 {cnt}, {cnt_out} 项，真实基数是 {real_num[cnt_out]}，估计基数是 {estimate_num}")
                    es_out.append([real_num[cnt_out], estimate_num])

                if cnt_out >= 11:
                    print(es_out)
                    quit()

            cnt += 1
        return LC_estimates


class DataPreparation:
    """数据准备类，负责从文件中加载数据"""

    @staticmethod
    def load_data(file_csv, file_real):
        """加载CSV文件并提取源数据和真实基数"""
        df_source = pd.read_csv(file_csv, usecols=['src'])
        source = df_source['src']
        df_real = pd.read_csv(file_real, usecols=['real-cardinality'])
        real_num = df_real['real-cardinality']
        return source, real_num


class FileSaver:
    """文件保存类，负责保存估计结果"""

    @staticmethod
    def save_results(results, file_realnum):
        """保存估计结果到文件"""
        data = {'LC_estimate': results}
        df = pd.DataFrame(data)
        save_path = file_realnum[:-12]
        df.to_csv(save_path + '_LC_estimate.csv')
        print("估计结果保存完毕")


def main():
    """主函数，控制整个数据处理流程"""
    global CM_para_d, CM_para_w, LC_para_m, where_datastream, where_stream_realcar, window_size

    # 初始化LRU和CountMin辅助结构
    lru = DoubleLinkedList()
    CM = CountMin(d=CM_para_d, w=CM_para_w)
    CM.generate_countmin()

    # 数据准备
    file_path = where_datastream
    file_realnum = where_stream_realcar
    source, real_num = DataPreparation.load_data(file_csv=file_path, file_real=file_realnum)

    # 初始化线性计数算法
    LC = LinearCounting(m=LC_para_m, win=window_size)
    LC_estimates = LC.update(lru=lru, CM=CM, source=source, real_num=real_num)

    # 保存估计结果
    FileSaver.save_results(LC_estimates, file_realnum)


if __name__ == '__main__':
    main()