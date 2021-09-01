"""
This example shows how to interact with the Determined PyTorch Lightning Adapter
interface to build a basic MNIST network. LightningAdapter utilizes the provided
LightningModule with Determined's PyTorch control loop.
"""

from determined.pytorch import PyTorchTrialContext, DataLoader
from determined.pytorch.lightning import LightningAdapter

import data
import mnist


class MNISTTrial(LightningAdapter):
    def __init__(self, context: PyTorchTrialContext, *args, **kwargs) -> None:
        lm = mnist.LitMNIST(
            hidden_size=context.get_hparam('hidden_size'),
            learning_rate=context.get_hparam('learning_rate'),
        )
        self.dm = data.MNISTDataModule(
            data_url=context.get_data_config()["url"],
            data_dir="/tmp/datasets/determined-mnist-pytorch",
            batch_size=context.get_per_slot_batch_size(),
        )

        super().__init__(context, lightning_module=lm, *args, **kwargs)
        self.dm.prepare_data()

    def build_training_data_loader(self) -> DataLoader:
        self.dm.setup()
        dl = self.dm.train_dataloader()
        return DataLoader(dl.dataset, batch_size=dl.batch_size, num_workers=dl.num_workers)

    def build_validation_data_loader(self) -> DataLoader:
        self.dm.setup()
        dl = self.dm.val_dataloader()
        return DataLoader(dl.dataset, batch_size=dl.batch_size, num_workers=dl.num_workers)
