import os
from typing import List

from pyspark.sql import SparkSession, DataFrame

from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.analyzers import AnalysisRunner, _AnalyzerObject
from pydeequ.repository import FileSystemMetricsRepository, ResultKey


# TODO: Setup repository root path
class DataQuality:
    spark: SparkSession
    spark_version: str
    tags: dict

    def __post__init__(self) -> None:
        os.environ["SPARK_VERSION"] = self.spark_version

    def _setup_repository(self, filename: str = "metrics.json"):
        metrics_file = FileSystemMetricsRepository.helper_metrics_file(
            self.spark, filename
        )

        return FileSystemMetricsRepository(self.spark, metrics_file)

    def _get_result_key(self) -> ResultKey:
        result_key_date = ResultKey.current_milli_time()

        return ResultKey(
            spark_session=self.spark,
            dataSetDate=result_key_date,
            tags=self.tags,
        )

    def run_analysis(
        self, dataframe: DataFrame, analyzers: List[_AnalyzerObject]
    ) -> None:
        repository = self._setup_repository()
        result_key = self._get_result_key(filename="analyzer.json")

        analysis_results = AnalysisRunner(spark_session=self.spark).onData(df=dataframe)

        for analyzer in analyzers:
            analysis_results = analysis_results.addAnalyzer(analyzer=analyzer)

        analysis_results.useRepository(repository).saveOrAppendResult(result_key).run()

    def run_data_profiling(self, dataframe: DataFrame):
        repository = self._setup_repository()
        result_key = self._get_result_key(filename="profiling.json")

        ColumnProfilerRunner(self.spark) \
            .onData(dataframe) \
            .useRepository(repository) \
            .saveOrAppendResult(result_key) \
            .run()
    