import pytest

from determined.common.api import bindings
from tests import api_utils
from tests import config as conf
from tests import experiment as exp


@pytest.mark.port_registry
def test_multi_trial_exp_port_registry() -> None:
    admin = api_utils.admin_session()
    experiment_id = exp.create_experiment(
        admin,
        conf.tutorials_path("mnist_pytorch/dist_random.yaml"),
        conf.tutorials_path("mnist_pytorch"),
    )

    exp.wait_for_experiment_state(
        admin, experiment_id=experiment_id, target_state=bindings.experimentv1State.COMPLETED
    )
