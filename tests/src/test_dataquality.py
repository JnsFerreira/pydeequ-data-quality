import mock
import pytest

from src.dataquality import DataQuality


class TestDataQuality:
    @pytest.fixture(scope="session")
    def spark(self) -> mock.MagicMock:
        spark = mock.MagicMock()

        return spark

    @pytest.fixture
    def data_quality(self, spark) -> DataQuality:
        return DataQuality(
            run_id="unit-test-run",
            metrics_bucket="my-dq-metrics",
            spark=spark,
            spark_version="3.3",
            tags={"testing": True},
        )

    @pytest.mark.parametrize("filename", ["foo.json", "bar.json"])
    def test_setup_repository_should_works_as_expected(
        self, data_quality: DataQuality, filename: str
    ) -> None:
        """
        Asserts that `_setup_repository` works as expected
        """
        with mock.patch(
            "src.dataquality.FileSystemMetricsRepository"
        ) as file_system_mock:
            data_quality._setup_repository(filename=filename)

        repository_path = (
            f"s3://{data_quality.metrics_bucket}/{data_quality.run_id}/{filename}"
        )

        file_system_mock.helper_metrics_file.assert_called_once_with(
            data_quality.spark, repository_path
        )

        file_system_mock.assert_called_once()

    def test_generate_result_key_should_works_as_expected(
        self, data_quality: DataQuality
    ) -> None:
        """
        Asserts `_generate_result_key` works as expected
        """
        with mock.patch("src.dataquality.ResultKey") as result_key_mock:
            data_quality._generate_result_key()

        result_key_mock.current_milli_time.assert_called_once()

        result_key_mock.assert_called_once_with(
            spark_session=data_quality.spark,
            dataSetDate=result_key_mock.current_milli_time.return_value,
            tags=data_quality.tags,
        )

    @mock.patch.object(target=DataQuality, attribute="_setup_repository")
    @mock.patch.object(target=DataQuality, attribute="_generate_result_key")
    def test_run_analysis_should_works_as_expected(
        self,
        generate_result_key_mock: mock.MagicMock,
        setup_repository_mock: mock.MagicMock,
        data_quality: DataQuality,
    ) -> None:
        """
        Asserts `run_analysis` works as expected
        """
        dataframe = mock.Mock()

        with mock.patch("src.dataquality.AnalysisRunner") as analysis_runner_mock:
            data_quality.run_analysis(dataframe=dataframe, analyzers=[])

            generate_result_key_mock.assert_called_once()
            setup_repository_mock.assert_called_once()

            analysis_runner_mock.return_value.onData.assert_called_once_with(
                df=dataframe
            )

    @mock.patch.object(target=DataQuality, attribute="_setup_repository")
    @mock.patch.object(target=DataQuality, attribute="_generate_result_key")
    def test_run_data_profiling_should_works_as_expected(
        self,
        generate_result_key_mock: mock.MagicMock,
        setup_repository_mock: mock.MagicMock,
        data_quality: DataQuality,
    ) -> None:
        """
        Asserts `run_data_profiling` works as expected
        """
        dataframe = mock.Mock()

        with mock.patch(
            "src.dataquality.ColumnProfilerRunner"
        ) as column_profiler_runner_mock:
            data_quality.run_data_profiling(dataframe=dataframe)

            generate_result_key_mock.assert_called_once()
            setup_repository_mock.assert_called_once()

            column_profiler_runner_mock.return_value.onData.assert_called_once_with(
                dataframe
            )
