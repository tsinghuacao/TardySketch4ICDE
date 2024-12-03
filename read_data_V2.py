import pandas as pd
import numpy as np
import os
import logging
from multiprocessing import Pool, Manager

# 配置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])


class CardinalityEstimator:
    """基数估计器类，负责分块处理和并行计算基数"""

    def __init__(self, win, step_size):
        """
        初始化基数估计器
        :param win: 窗口大小
        :param step_size: 步长
        """
        self.win = win
        self.step_size = step_size
        logging.info(f"初始化基数估计器，窗口大小：{win}，步长：{step_size}")

    def get_real_num_parallel(self, args):
        """并行处理单个数据块，计算唯一元素个数"""
        chunk, win, shared_space = args
        unique_elements = set(chunk)
        return len(unique_elements)

    def split_dataframe(self, df):
        """将数据框分割为多个滑动窗口"""
        rounds = int((len(df) - self.win) / self.step_size)
        chunks = []
        for i in range(rounds + 1):
            start_point = i * self.step_size
            end_point = self.win + i * self.step_size
            chunks.append(df.iloc[start_point:end_point])
        logging.info(f"数据被分割为 {len(chunks)} 个块")
        return chunks

    def parallel_processing(self, chunks):
        """使用多进程并行计算所有块的基数"""
        with Manager() as manager:
            shared_space = manager.list([0] * self.win)
            args_list = [(chunk, self.win, shared_space) for chunk in chunks]

            with Pool() as pool:
                logging.info(f"开始并行处理 {len(chunks)} 个数据块")
                results = pool.map(self.get_real_num_parallel, args_list)

        return results

    def save_results(self, results, file_path):
        """保存统计结果到文件"""
        statistics = {'real-cardinality': results}
        df = pd.DataFrame(statistics)
        output_file = file_path[:-4] + f"-win-{self.win}-step_size-{self.step_size}_real_num.csv"
        df.to_csv(output_file, index=False)
        logging.info(f"统计结果保存到文件：{output_file}")


def load_data(file_path, win):
    """加载数据并截取需要的部分"""
    try:
        df_data = pd.read_csv(file_path, usecols=['src'])
        df_data = df_data[:win * 50]  # 限制数据量避免内存溢出
        logging.info(f"成功加载数据，数据行数：{len(df_data)}")
        return df_data['src']
    except Exception as e:
        logging.error(f"加载数据时出错: {e}")
        raise


def main(file_path, win, step_size):
    """主函数，控制数据处理流"""
    try:
        # 加载数据
        data = load_data(file_path, win)

        # 创建基数估计器
        estimator = CardinalityEstimator(win, step_size)

        # 分割数据
        chunks = estimator.split_dataframe(data)

        # 并行处理
        results = estimator.parallel_processing(chunks)

        # 保存结果
        estimator.save_results(results, file_path)

        logging.info("基数统计完毕！")
    except Exception as e:
        logging.error(f"程序执行过程中发生错误: {e}")


if __name__ == '__main__':
    # 配置文件路径及参数
    file_path = 'Data/mini-test/_4000004.csv'
    win = 65536*4  # 窗口大小
    step_size = int(0.5 * win)  # 步长设置为窗口的一半

    # 调用主函数
    main(file_path, win, step_size)
