import lance
import pandas as pd
import pathlib

def loading_into_pandas():
	uri = "s3://lance/fluss/images_minio.lance"
	ds = lance.dataset(uri, storage_options={"access_key_id": "minio", "secret_access_key": "minioadmin", "endpoint": "http://localhost:9000", "allow_http": "true",})

	# Accumulate data from batches into a list
	data = []
	for batch in ds.to_batches(columns=["image"], batch_size=10):
		tbl = batch.to_pandas()
		data.append(tbl)

	# Concatenate all DataFrames into a single DataFrame
	df = pd.concat(data, ignore_index=True)
	print("Pandas DataFrame is ready")
	print("Total Rows: ", df.shape[0])
	return df

def save_images_from_dataframe(df):
    output_dir = pathlib.Path("/Users/jim/Documents/minio_images")
    output_dir.mkdir(exist_ok=True)
    
    print(f"\n--- Saving {len(df)} images to {output_dir} ---")
    
    for idx, row in df.iterrows():
        image_data = row["image"]
        filename = f"image_{idx:04d}.png"
        file_path = output_dir / filename
        
        with open(file_path, 'wb') as f:
            f.write(image_data)
        
        print(f"Saved: {filename}")
    
    print(f"All images saved to {output_dir}")

if __name__ == "__main__":
	df = loading_into_pandas()
	save_images_from_dataframe(df)