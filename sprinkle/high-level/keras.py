    import numpy as np
    from tensorflow import keras
    from tensorflow.keras import layers

+   import determined as det
+   ctx = det.keras.init()  # also initialized random seeds
+   hparams = ctx.training.hparams

    # model code is straight from keras mnist example
    num_classes = 10
    input_shape = (28, 28, 1)

    (x_train, y_train), (x_test, y_test) = keras.datasets.mnist.load_data()

    x_train = x_train.astype("float32") / 255
    x_test = x_test.astype("float32") / 255
    x_train = np.expand_dims(x_train, -1)
    x_test = np.expand_dims(x_test, -1)

    y_train = keras.utils.to_categorical(y_train, num_classes)
    y_test = keras.utils.to_categorical(y_test, num_classes)

    model = keras.Sequential(
        [
            keras.Input(shape=input_shape),
            layers.Conv2D(32, kernel_size=(3, 3), activation="relu"),
            layers.MaxPooling2D(pool_size=(2, 2)),
            layers.Conv2D(64, kernel_size=(3, 3), activation="relu"),
            layers.MaxPooling2D(pool_size=(2, 2)),
            layers.Flatten(),
            layers.Dropout(0.5),
            layers.Dense(num_classes, activation="softmax"),
        ]
    )

    model.summary()

    batch_size = 128
    epochs = 15

+   # these steps only actually required for distributed training:
+   x_train, y_train = ctx.wrap_data(x_train, y_train)
+   x_test, y_test = ctx.wrap_data(x_test, y_test)
+   optimizer = ctx.wrap_optimizer(...)
    model.compile(loss="categorical_crossentropy", optimizer=optimizer, metrics=["accuracy"])

    batch_size = 128
+   # this step is actually optional for dtrain, based on the behavior you want
+   batch_size = ctx.distributed.shard_batch_size(batch_size)  # rename shard_batch_size()

+   # our keras support requires that you pass validation data fit() rather than using separate
+   # fit() and evaluate() calls.
+   hist = context.fit(
+       model,
        x=x_train,
        y=y_train,
        batch_size=batch_size,

        epochs=epochs,         # ignored for cluster training
        initial_epoch=...,     # ignored for cluster training

        validation_split=...,  # during a training job,
        validation_data=...,   #   some form of validation data
        x_val=...,             #   must be set
        y_val=...,             #   for context.fit()
    )

    print("Test loss:", hist.history['val_loss'][-1])
    print("Test accuracy:", hist.score['val_acc'][-1])
