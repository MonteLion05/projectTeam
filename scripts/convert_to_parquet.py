"""Convert selected TSV tables and read back every column before success."""
import argparse
from pathlib import Path
from src.loaders import parquet_path, read_tsv
from src.schemas import TABLE_COLUMNS
from src.spark_session import get_spark
from scripts.rebuild_episode import table_signature

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--tables', nargs='+', choices=list(TABLE_COLUMNS), default=list(TABLE_COLUMNS))
    p.add_argument('--overwrite', action='store_true')
    args=p.parse_args()
    spark=get_spark('convert-to-parquet')
    try:
        for table in args.tables:
            target=parquet_path(table)
            if Path(target).exists() and not args.overwrite:
                raise RuntimeError(f'{target} exists; verify it or explicitly pass --overwrite')
            source=read_tsv(spark, table)
            before=table_signature(source)
            source.write.mode('overwrite' if args.overwrite else 'error').parquet(target)
            after=table_signature(spark.read.parquet(target))
            if before != after or before['total_rows'] <= 0:
                Path(target, '_SUCCESS').unlink(missing_ok=True)
                raise RuntimeError(f'Full-column readback failed: {table}')
            print('VERIFIED PARQUET',table,after['total_rows'],flush=True)
    finally:
        spark.stop()
if __name__=='__main__': main()
