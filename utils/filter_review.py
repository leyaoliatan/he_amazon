#This script is to filter the review df based on a list of product id
def batch_filter_reviews(review_path, label_product_ids, 
                         batch_size=100000, output_path=None):
    """
    Filter a large review dataframe in batches to find reviews for products with labels.
    
    Parameters:
    -----------
    review_path : str
        Path to the review CSV or parquet file
    label_product_ids : list
        List of product IDs that have labels
    batch_size : int, default=100000
        Number of rows to process in each batch
    output_path : str, optional
        Path to save filtered results. If None, returns the filtered dataframe
        
    Returns:
    --------
    pd.DataFrame or None
        If output_path is None, returns the filtered dataframe
        Otherwise, saves to file and returns None
    """
    import pandas as pd
    import os
    
    # Determine file type and appropriate reader
    file_ext = os.path.splitext(review_path)[1].lower()
    
    # Convert label_product_ids to a set for faster lookups
    label_product_ids_set = set(label_product_ids)
    
    # Initialize an empty list to store filtered chunks
    filtered_chunks = []
    
    if file_ext == '.csv':
        # Get total rows to estimate progress
        total_rows = sum(1 for _ in open(review_path, 'r')) - 1  # subtract header
        
        # Process CSV in chunks
        chunk_iter = pd.read_csv(review_path, chunksize=batch_size)
        
        processed_rows = 0
        for i, chunk in enumerate(chunk_iter):
            filtered_chunk = chunk[chunk['product_ID'].isin(label_product_ids_set)]
            
            if not filtered_chunk.empty:
                if output_path:
                    # Append to file if output_path provided
                    mode = 'w' if i == 0 else 'a'
                    header = i == 0
                    filtered_chunk.to_csv(output_path, mode=mode, header=header, index=False)
                else:
                    # Store in memory if no output_path
                    filtered_chunks.append(filtered_chunk)
            
            processed_rows += len(chunk)
            print(f"Processed batch {i+1}: {processed_rows}/{total_rows} rows ({(processed_rows/total_rows)*100:.1f}%)", end='\r', flush=True)
            
    elif file_ext == '.parquet':
        # Process parquet file
        import pyarrow.parquet as pq
        
        # Get total number of row groups
        parquet_file = pq.ParquetFile(review_path)
        num_row_groups = parquet_file.num_row_groups
        
        for i in range(0, num_row_groups):
            # Read one row group at a time
            chunk = parquet_file.read_row_group(i).to_pandas()
            filtered_chunk = chunk[chunk['product_ID'].isin(label_product_ids_set)]
            
            if not filtered_chunk.empty:
                if output_path:
                    mode = 'w' if i == 0 else 'a'
                    header = i == 0
                    filtered_chunk.to_csv(output_path, mode=mode, header=header, index=False)
                else:
                    filtered_chunks.append(filtered_chunk)
            
            print(f"Processed row group {i+1}/{num_row_groups} ({(i+1)/num_row_groups*100:.1f}%)", end='\r', flush=True)
    
    else:
        raise ValueError(f"Unsupported file extension: {file_ext}. Use .csv or .parquet")
    
    # Return combined filtered data if not saving to file
    if not output_path and filtered_chunks:
        return pd.concat(filtered_chunks, ignore_index=True)
    elif output_path:
        print(f"Filtered data saved to {output_path}")
    else:
        return pd.DataFrame()  # Return empty DataFrame if no matches found