# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Dict, Any
import pandas as pd
import logging
from multiprocessing import Pool
from collections import Counter
from operator import itemgetter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

@dataclass(frozen=True)
class ProcessingConfig:
    window_size: int = 65536
    step_ratio: float = 0.5
    top_k: int = 20
    max_workers: int = 8

class FrequencyAnalyzer:
    def __init__(self, config: ProcessingConfig) -> None:
        self.config = config
        self.step_size = int(config.window_size * config.step_ratio)

    def _generate_windows(self, series: pd.Series) -> List[pd.Series]:
        n = len(series)
        return [
            series.iloc[start:end]
            for start in range(0, n - self.config.window_size + 1, self.step_size)
            if (end := start + self.config.window_size) <= n
        ]

    @staticmethod
    def _compute_frequencies(series: pd.Series) -> List[Tuple[Any, int]]:
        try:
            counter = Counter(series.dropna())
            return sorted(
                counter.items(),
                key=itemgetter(1, 0),
                reverse=True
            )[:self.config.top_k]
        except Exception as e:
            logging.error(f"Frequency computation failed: {str(e)}")
            return []

    def analyze(self, file_path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(file_path, usecols=['src'], dtype={'src': 'category'})
            windows = self._generate_windows(df['src'])
            
            with Pool(processes=self.config.max_workers) as pool:
                results = pool.map(self._compute_frequencies, windows)
            
            return pd.DataFrame({'F_val': results})
            
        except FileNotFoundError:
            logging.error(f"Data file not found: {file_path}")
            raise
        except KeyError:
            logging.error("Required column 'src' missing in dataset")
            raise

if __name__ == '__main__':
    config = ProcessingConfig(max_workers=4)
    analyzer = FrequencyAnalyzer(config)
    
    try:
        hhts = analyzer.analyze('/home/cao/code-master/Data/mini-test/_4000002.csv')
        print(hhts.iloc[0]['F_val'])
        logging.info(f"Result type: {type(hhts.iloc[0]['F_val'])}")
        
    except Exception as e:
        logging.critical(f"Analysis pipeline failed: {str(e)}")
