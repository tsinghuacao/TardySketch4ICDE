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
    """Linear Counting algorithm for cardinality estimation"""

    def __init__(self, m, win):
        """
        Initialize the Linear Counting algorithm
        :param m: Size of the counting table
        :param win: Window size
        """
        self.m = m
        self.win = win
        self.LC = []  # Counting table
        self._initialize_lc()

    def _initialize_lc(self):
        """Initialize the counting table"""
        self.LC = [Node(0) for _ in range(self.m)]

    def _get_index(self, dst):
        """Get hash index for the data source 'dst'"""
        res = xxhash.xxh64_intdigest(dst, seed=20240417)
        return res

    def get_estimation(self):
        """Estimate the cardinality based on the linear counting formula"""
        res = -self.m * np.log((self.m - self.LC[0].val) / self.m)
        return res

    def _calculate_average_gap(self):
        """Calculate the average gap between nodes in the LC table"""
        total_gap = sum(node.gap for node in self.LC)
        return total_gap / self.m

    def update(self, lru, CM, source, real_num):
        """Update the counting table and adjust based on the sliding window"""
        LC_estimates = []
        cnt = 0
        cnt_out = 0
        es_out = []
        for i in range(len(source)):
            # Get the hash index for the data source and calculate the bit index
            hash_val = self._get_index(source[i])
            bit_index = hash_val % self.m

            # Update the LC table and LRU
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

            # Window sliding mechanism
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

                # Print verification results
                if (cnt - self.win) % print_LC_gap == 0:
                    estimate_num = self.get_estimation()
                    LC_estimates.append(estimate_num)
                    print(f"Currently processing {cnt}, {cnt_out} items, real cardinality is {real_num[cnt_out]}, estimated cardinality is {estimate_num}")
                    es_out.append([real_num[cnt_out], estimate_num])

                if cnt_out >= 11:
                    print(es_out)
                    quit()

            cnt += 1
        return LC_estimates


class DataPreparation:
    """Data preparation class responsible for loading data from files"""

    @staticmethod
    def load_data(file_csv, file_real):
        """Load CSV files and extract source data and real cardinality"""
        df_source = pd.read_csv(file_csv, usecols=['src'])
        source = df_source['src']
        df_real = pd.read_csv(file_real, usecols=['real-cardinality'])
        real_num = df_real['real-cardinality']
        return source, real_num


class FileSaver:
    """File saving class responsible for saving estimation results"""

    @staticmethod
    def save_results(results, file_realnum):
        """Save the estimation results to a file"""
        data = {'LC_estimate': results}
        df = pd.DataFrame(data)
        save_path = file_realnum[:-12]
        df.to_csv(save_path + '_LC_estimate.csv')
        print("Estimation results have been saved successfully")


def main():
    """Main function to control the entire data processing flow"""
    global CM_para_d, CM_para_w, LC_para_m, where_datastream, where_stream_realcar, window_size

    # Initialize LRU and CountMin auxiliary structures
    lru = DoubleLinkedList()
    CM = CountMin(d=CM_para_d, w=CM_para_w)
    CM.generate_countmin()

    # Data preparation
    file_path = where_datastream
    file_realnum = where_stream_realcar
    source, real_num = DataPreparation.load_data(file_csv=file_path, file_real=file_realnum)

    # Initialize the Linear Counting algorithm
    LC = LinearCounting(m=LC_para_m, win=window_size)
    LC_estimates = LC.update(lru=lru, CM=CM, source=source, real_num=real_num)

    # Save estimation results
    FileSaver.save_results(LC_estimates, file_realnum)


if __name__ == '__main__':
    main()
