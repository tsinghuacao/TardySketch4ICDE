import pandas as pd
import numpy as np
import os
import logging
from multiprocessing import Pool, Manager

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])


class CardinalityEstimator:
    """Cardinality Estimator class responsible for chunk processing and parallel cardinality computation"""

    def __init__(self, win, step_size):
        """
        Initialize the cardinality estimator
        :param win: Window size
        :param step_size: Step size
        """
        self.win = win
        self.step_size = step_size
        logging.info(f"Initialized cardinality estimator with window size: {win} and step size: {step_size}")

    def get_real_num_parallel(self, args):
        """Parallel processing for a single chunk, computing the number of unique elements"""
        chunk, win, shared_space = args
        unique_elements = set(chunk)
        return len(unique_elements)

    def split_dataframe(self, df):
        """Split the dataframe into multiple sliding windows"""
        rounds = int((len(df) - self.win) / self.step_size)
        chunks = []
        for i in range(rounds + 1):
            start_point = i * self.step_size
            end_point = self.win + i * self.step_size
            chunks.append(df.iloc[start_point:end_point])
        logging.info(f"Data split into {len(chunks)} chunks")
        return chunks

    def parallel_processing(self, chunks):
        """Use multiprocessing to parallelize the computation of cardinality for all chunks"""
        with Manager() as manager:
            shared_space = manager.list([0] * self.win)
            args_list = [(chunk, self.win, shared_space) for chunk in chunks]

            with Pool() as pool:
                logging.info(f"Starting parallel processing of {len(chunks)} chunks")
                results = pool.map(self.get_real_num_parallel, args_list)

        return results

    def save_results(self, results, file_path):
        """Save the statistics results to a file"""
        statistics = {'real-cardinality': results}
        df = pd.DataFrame(statistics)
        output_file = file_path[:-4] + f"-win-{self.win}-step_size-{self.step_size}_real_num.csv"
        df.to_csv(output_file, index=False)
        logging.info(f"Statistics saved to file: {output_file}")


def load_data(file_path, win):
    """Load data and extract the required portion"""
    try:
        df_data = pd.read_csv(file_path, usecols=['src'])
        df_data = df_data[:win * 50]  # Limit data size to avoid memory overflow
        logging.info(f"Data loaded successfully, number of rows: {len(df_data)}")
        return df_data['src']
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


def main(file_path, win, step_size):
    """Main function to control the data processing flow"""
    try:
        # Load data
        data = load_data(file_path, win)

        # Create the cardinality estimator
        estimator = CardinalityEstimator(win, step_size)

        # Split the data into chunks
        chunks = estimator.split_dataframe(data)

        # Parallel processing
        results = estimator.parallel_processing(chunks)

        # Save the results
        estimator.save_results(results, file_path)

        logging.info("Cardinality estimation completed!")
    except Exception as e:
        logging.error(f"Error during program execution: {e}")


if __name__ == '__main__':
    # Configuration of file path and parameters
    file_path = 'Data/mini-test/_4000004.csv'
    win = 65536 * 4  # Window size
    step_size = int(0.5 * win)  # Step size is set to half of the window size

    # Call the main function
    main(file_path, win, step_size)
