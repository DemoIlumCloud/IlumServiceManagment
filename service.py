from ilum.api import IlumJob
from pyspark.sql.functions import col, sum as spark_sum


class SparkInteractiveExample(IlumJob):
    def run(self, spark, config) -> str:
        table_name = config.get('table')
        database_name = config.get('database')
        report_lines = []

        if not table_name:
            raise ValueError("Config must provide a 'table' key")

        if database_name:
            spark.catalog.setCurrentDatabase(database_name)
            report_lines.append(f"Using database: {database_name}")

        if table_name not in [t.name for t in spark.catalog.listTables()]:
            raise ValueError(f"Table '{table_name}' not found in catalog")

        df = spark.table(table_name)

        report_lines.append(f"=== Details for table: {table_name} ===")

        total_rows = df.count()
        report_lines.append(f"Total rows: {total_rows}")

        total_columns = len(df.columns)
        report_lines.append(f"Total columns: {total_columns}")

        report_lines.append("Distinct values per column:")
        for c in df.columns:
            distinct_count = df.select(c).distinct().count()
            report_lines.append(f"  {c}: {distinct_count}")

        report_lines.append("Schema:")
        for f in df.schema.fields:
            report_lines.append(f"  {f.name}: {f.dataType}")

        report_lines.append("Sample data (first 5 rows):")
        sample_rows = df.take(5)
        for row in sample_rows:
            report_lines.append(str(row.asDict()))

        report_lines.append("Null counts per column:")
        null_counts_df = df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
        null_counts = null_counts_df.collect()[0].asDict()
        for c, v in null_counts.items():
            report_lines.append(f"  {c}: {v}")

        return "\n".join(report_lines)
  
