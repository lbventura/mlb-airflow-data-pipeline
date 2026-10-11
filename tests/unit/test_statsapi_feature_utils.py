import pandas as pd
import pytest

from mlb_airflow_data_pipeline.statsapi_feature_utils import (
    create_innings_pitched_normalization,
)


@pytest.mark.parametrize(("innings", "outs"), [(20.0, 60), (20.1, 61), (20.2, 62)])
def test_pitching_normalization_uses_baseball_innings(
    innings: float, outs: int
) -> None:
    data = pd.DataFrame(
        {"inningsPitched": [innings], "outs": [outs], "intentionalWalks": [3]}
    )
    result = create_innings_pitched_normalization(data, ["intentionalWalks"])
    assert result.loc[0, "intentionalWalksinningsPitched"] == pytest.approx(
        3 / (outs / 3)
    )
