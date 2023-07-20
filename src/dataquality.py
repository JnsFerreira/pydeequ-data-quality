import os
from typing import List
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame

from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.analyzers import AnalysisRunner, _AnalyzerObject
from pydeequ.repository import FileSystemMetricsRepository, ResultKey


@dataclass
class DataQuality:
    """
    Class that runs Data Quality reports

    Args:
        run_id (str): Unique report identifier
        metrics_bucket (str): Metrics bucket name
        spark (SparkSession): Spark session
        spark_version (str): Running spark version
        tags (dict): Tags to be applyied to reports
    """
    run_id: str
    metrics_bucket: str
    spark: SparkSession
    spark_version: str
    tags: dict


    def __post__init__(self) -> None:
        """
        Post initializes `DataQuality` class
        """
        os.environ["SPARK_VERSION"] = self.spark_version

    def _setup_repository(self, filename: str = "metrics.json") -> FileSystemMetricsRepository:
        """
        Setup metrics repository

        Args:
            filename (str): Metrics file name

        Returns:
            FileSystemMetricsRepository: High level metrics repository
        """
        repository_path = f"s3://{self.metrics_bucket}/{self.run_id}/{filename}"

        metrics_file = FileSystemMetricsRepository.helper_metrics_file(
            self.spark, repository_path
        )

        return FileSystemMetricsRepository(self.spark, metrics_file)

    def _generate_result_key(self) -> ResultKey:
        """
        Generates a unique identifier for an analysis or profile

        Returns:
            ResultKey: Information that uniquely identifies a AnalysisResult
        """
        result_key_date = ResultKey.current_milli_time()

        return ResultKey(
            spark_session=self.spark,
            dataSetDate=result_key_date,
            tags=self.tags,
        )

    def run_analysis(
        self, dataframe: DataFrame, analyzers: List[_AnalyzerObject]
    ) -> None:
        """
        Runs a data quality analysis

        Args:
            dataframe (DataFrame): Data to perform data quality analysis
            analyzers (List[_AnalyzerObject]): List of analyzers to apply on data

        Returns:
            None
        """
        repository = self._setup_repository()
        result_key = self._generate_result_key()

        analysis_results = AnalysisRunner(spark_session=self.spark).onData(df=dataframe)

        for analyzer in analyzers:
            analysis_results = analysis_results.addAnalyzer(analyzer=analyzer)

        analysis_results \
            .useRepository(repository) \
            .saveOrAppendResult(result_key) \
            .run()

    def run_data_profiling(self, dataframe: DataFrame) -> None:
        """
        Runs a data profile report on a given data

        Args:
            dataframe (DataFrame): Data to perform data profile report

        Returns:
            None
        """
        repository = self._setup_repository(filename="profiling.json")
        result_key = self._generate_result_key()

        ColumnProfilerRunner(self.spark) \
            .onData(dataframe) \
            .useRepository(repository) \
            .saveOrAppendResult(result_key) \
            .run()
