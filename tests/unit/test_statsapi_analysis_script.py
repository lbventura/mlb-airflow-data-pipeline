import matplotlib.pyplot as plt
from matplotlib.text import Annotation
import numpy as np
import pandas as pd
import pytest

from mlb_airflow_data_pipeline.statsapi_analysis_script import scatter_plot


@pytest.mark.parametrize("empty", [False, True], ids=["valid-points", "all-missing"])
def test_scatter_plot_filters_missing_values_and_preserves_input(empty: bool) -> None:
    frame = pd.DataFrame(
        {
            "playername": ["First Hitter", "Missing X", "Missing Y", "Other Pitcher"],
            "x": [1.0, None, 3.0, 4.0],
            "y": [2.0, 3.0, None, 5.0],
        },
        index=[101, 202, 303, 404],
    )
    if empty:
        frame["x"] = float("nan")
    original = frame.copy()

    figure = scatter_plot(frame, ("x", "y"))
    try:
        axes = figure.axes[0]
        assert axes.get_xlabel() == "x"
        assert axes.get_ylabel() == "y"
        assert np.asarray(axes.collections[0].get_offsets()).tolist() == (
            [] if empty else [[1.0, 2.0], [4.0, 5.0]]
        )
        assert [text.get_text() for text in axes.texts] == (
            [] if empty else ["Hitter", "Pitcher"]
        )
        positions = []
        for text in axes.texts:
            assert isinstance(text, Annotation)
            positions.append(text.xy)
        assert positions == ([] if empty else [(1.0, 2.0), (4.0, 5.0)])
        pd.testing.assert_frame_equal(frame, original)
    finally:
        plt.close(figure)
