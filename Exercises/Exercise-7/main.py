from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_spark_session():
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName("Exercise 7 - Hard Drive Analysis") \
        .getOrCreate()

# ... existing code ...

def main():
    # Tạo Spark session
    spark = create_spark_session()
    
    # Đọc dữ liệu
    df = read_hard_drive_data(spark)
    
    # Áp dụng tất cả các biến đổi
    df = (df
        .transform(add_source_file_column)
        .transform(extract_file_date)
        .transform(determine_brand)
        .transform(add_storage_ranking)
        .transform(create_primary_key)
    )
    
    # Hiển thị kết quả
    df.select(
        "source_file", "file_date", "brand", 
        "storage_ranking", "primary_key"
    ).show(5)

if __name__ == "__main__":
    main()
