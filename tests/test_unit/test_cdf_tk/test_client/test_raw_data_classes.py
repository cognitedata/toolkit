from typing import Any, ClassVar

import pytest

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawProfileResults


class TestRawProfileResults:
    LARGE_CASE: ClassVar[dict[str, Any]] = {
        "rowCount": 1000,
        "columns": {
            "MyUnknownType": {"count": 0, "nullCount": 1000},
            "EquipmentType": {
                "count": 985,
                "nullCount": 15,
                "string": {
                    "lengthRange": [3, 20],
                    "distinctCount": 47,
                    "lengthHistogram": [
                        [3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33],
                        [8, 231, 103, 9, 102, 86, 152, 19, 79, 196, 0, 0, 0, 0, 0, 0],
                    ],
                    "valueCounts": [
                        [
                            "<other>",
                            "GEN. - MISCELLANEOUS",
                            "PROCESS SENSOR",
                            "VALVE",
                            "STRUCTURAL",
                            "PUMP",
                            "PIPING",
                            "MOTOR",
                            "ARCHITECTURAL",
                            "VESSEL",
                            "LOSS CONTROL",
                        ],
                        [303, 169, 105, 81, 69, 66, 47, 40, 36, 35, 34],
                    ],
                    "count": 985,
                },
            },
            "my_string_column": {
                "count": 1000,
                "nullCount": 0,
                "string": {
                    "lengthRange": [8, 8],
                    "distinctCount": 2,
                    "lengthHistogram": [[8], [1000]],
                    "valueCounts": [["00000000", "20220719"], [999, 1]],
                    "count": 1000,
                },
            },
            "my_number_column": {
                "count": 1000,
                "nullCount": 0,
                "number": {
                    "valueRange": [0.0, 5017001.1],
                    "distinctCount": 287,
                    "valueCounts": {
                        "0.0": 341,
                        "0.01": 130,
                        "0.02": 92,
                        "0.03": 33,
                        "0.04": 28,
                        "0.06": 22,
                        "0.12": 9,
                        "0.08": 8,
                        "0.05": 7,
                        "64.3": 6,
                    },
                    "histogram": [
                        [
                            0.0,
                            313562.56875,
                            627125.1375,
                            940687.70625,
                            1254250.275,
                            1567812.84375,
                            1881375.4125,
                            2194937.98125,
                            2508500.55,
                            2822063.11875,
                            3135625.6875,
                            3449188.25625,
                            3762750.825,
                            4076313.39375,
                            4389875.9625,
                            4703438.53125,
                        ],
                        [341, 656, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                    ],
                    "count": 1000,
                    "mean": 8485.551200000002,
                    "std": 162257.82025455945,
                    "median": 0.02,
                },
            },
        },
        "isComplete": True,
    }

    EXHAUSTIVE_CASE: ClassVar[dict[str, Any]] = {
        "columns": {
            "ArrayCol": {
                "count": 4,
                "nullCount": 1,
                "vector": {"count": 4, "lengthHistogram": [[3], [4]], "lengthRange": [3, 3]},
            },
            "BooleanCol": {"boolean": {"count": 5, "trueCount": 4}, "count": 5, "nullCount": 0},
            "EmptyCol": {"count": 0, "nullCount": 5},
            "FloatCol": {
                "count": 5,
                "nullCount": 0,
                "number": {
                    "count": 5,
                    "distinctCount": 5,
                    "histogram": [
                        [
                            0.2,
                            0.24375,
                            0.2875,
                            0.33125,
                            0.375,
                            0.41875,
                            0.4625,
                            0.50625,
                            0.55,
                            0.59375,
                            0.6375,
                            0.68125,
                            0.725,
                            0.76875,
                            0.8125,
                            0.85625,
                        ],
                        [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1],
                    ],
                    "mean": 0.58,
                    "median": 0.4,
                    "std": 0.25612496949731395,
                    "valueCounts": {"0.2": 1, "0.4": 1, "0.6000000000000001": 1, "0.8": 1, "0.9": 1},
                    "valueRange": [0.2, 0.9],
                },
            },
            "IntegerCol": {
                "count": 5,
                "nullCount": 0,
                "number": {
                    "count": 5,
                    "distinctCount": 4,
                    "histogram": [
                        [
                            1.0,
                            1.1875,
                            1.375,
                            1.5625,
                            1.75,
                            1.9375,
                            2.125,
                            2.3125,
                            2.5,
                            2.6875,
                            2.875,
                            3.0625,
                            3.25,
                            3.4375,
                            3.625,
                            3.8125,
                        ],
                        [1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 2],
                    ],
                    "mean": 2.8,
                    "median": 2.0,
                    "std": 1.1661903789690602,
                    "valueCounts": {"1.0": 1, "2.0": 1, "3.0": 1, "4.0": 2},
                    "valueRange": [1.0, 4.0],
                },
            },
            "ObjectCol": {
                "count": 5,
                "nullCount": 0,
                "object": {"count": 5, "keyCountHistogram": [[1], [5]], "keyCountRange": [1, 1]},
            },
            "StringCol": {
                "count": 5,
                "nullCount": 0,
                "string": {
                    "count": 5,
                    "distinctCount": 3,
                    "lengthHistogram": [[6], [5]],
                    "lengthRange": [6, 6],
                    "valueCounts": [["value0", "value2", "value1"], [2, 2, 1]],
                },
            },
        },
        "isComplete": True,
        "rowCount": 5,
    }

    @pytest.mark.parametrize("data", [EXHAUSTIVE_CASE, LARGE_CASE])
    def test_load_dump(self, data: dict[str, Any]) -> None:
        results = RawProfileResults._load(data)
        dumped = results.dump()
        assert dumped == data, "Dumped data does not match original data"
        assert isinstance(results, RawProfileResults), "Loaded object is not of type RawProfileResults"
