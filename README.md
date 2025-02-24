## TardySketch

This is the source code of QSketch in ICDE2025.  

## Preparation

First, you need to install some dependencies to meet the runtime requirements. You can use `pip` to install the required packages:  

```python
pip install package==version
```



### File Structure Introduction  
- **data**: Due to copyright restrictions of the CAIDA dataset, only example data formats are provided here, including preprocessed CSV files, cardinality information statistics files, and frequency information statistics files.  
- **`read_data_V2`**: Data preprocessing file. You can run this program to process your data files and obtain flow cardinality information. This program supports multi-threading operations.  
- **`read_data_V2_F`**: Data preprocessing file. You can run this program to process your data files and obtain flow frequency information. This program supports multi-threading operations.  
- **`M_RS+BP`:** Core component code, including BP-bitmap implementation, which serves as the main entry of the program.  
- **`Component`:** Core component code, including implementations of the GAP mechanism and SD mechanism. This program provides feature-rich interfaces, supporting custom hash functions and other functionalities.  
- **`mmh3_utils`**: Hash utility functions, supporting custom random seeds and hash types.  
- **`xxhash_utils`**: Hash utility functions, supporting custom random seeds and hash types.  
- **`M_QSketch`**: QSketch baseline solution. More baseline solutions will be continuously updated in the future.  

## Running the Program  
1. Run `M_RS+BP.py` and specify the following parameters:  
```python
   global CM_para_d, CM_para_w, LC_para_m, where_datastream, where_stream_realcar, window_size  
   # Data preparation  
   file_path = where_datastream  
   file_realnum = where_stream_realcar  
   source, real_num = DataPreparation.load_data(file_csv=file_path, file_real=file_realnum)  
```

2. Initialize TardySketch:

```python
   # Initialize the Linear Counting algorithm  
   LC = LinearCounting(m=LC_para_m, win=window_size)  
   LC_estimates = LC.update(lru=lru, CM=CM, source=source, real_num=real_num)  
```

3. Save experimental results:

```python
   # Save estimation results  
   FileSaver.save_results(LC_estimates, file_realnum)  
```
