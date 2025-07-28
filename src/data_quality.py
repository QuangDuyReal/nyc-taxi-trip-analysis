from pyspark.sql.functions import col

class DataQualityChecker:
    def __init__(self, spark_session):
        self.spark = spark_session

    def check_data_quality(self, df, layer_name=None):
        """Comprehensive data quality assessment"""
        quality_metrics = {}
        total_rows = df.count()

        # 1. Completeness Check
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            quality_metrics[f"{col_name}_completeness"] = (total_rows - null_count) / total_rows

        # 2. Validity Check — only apply when schema matches
        expected_cols = ["trip_distance", "fare_amount", "trip_duration_minutes", "passenger_count"]
        if all(c in df.columns for c in expected_cols):
            valid_trips = df.filter(
                (col("trip_distance") > 0) &
                (col("fare_amount") > 0) &
                (col("trip_duration_minutes") > 0) &
                (col("passenger_count").between(1, 8))
            ).count()
            quality_metrics["business_rule_validity"] = valid_trips / total_rows

        # 3. Consistency Check
        if all(c in df.columns for c in ["total_amount", "fare_amount", "extra", "mta_tax"]):
            consistency = df.filter(
                col("total_amount") >= (col("fare_amount") + col("extra") + col("mta_tax"))
            ).count()
            quality_metrics["amount_consistency"] = consistency / total_rows

        # 4. Uniqueness Check
        distinct_rows = df.distinct().count()
        quality_metrics["uniqueness"] = distinct_rows / total_rows

        return quality_metrics

    def generate_quality_report(self, quality_metrics):
        """Generate quality report"""
        report = []
        for metric, score in quality_metrics.items():
            status = "PASS" if score >= 0.95 else "WARN" if score >= 0.8 else "FAIL"
            report.append(f"{metric}: {score:.2%} - {status}")
        return "\n".join(report)
