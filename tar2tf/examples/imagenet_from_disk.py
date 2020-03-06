import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

from tar2tf import AisDataset, default_record_parser

EPOCHS = 10
BATCH_SIZE = 20

# ADJUST AisDataset PARAMETERS BELOW

BUCKET_NAME = "lb"
PROXY_URL = "http://localhost:8080"

ais = AisDataset(BUCKET_NAME, PROXY_URL)

# prepare your bucket, for example from `gsutil ls gs://lpr-gtc2020`
# save TFRecord file to train.record and test.record
ais.load_from_tar("train-{0..3}.tar.xz", path="train.record")
ais.load_from_tar("train-{4..7}.tar.xz", path="test.record")

train_dataset = tf.data.TFRecordDataset(filenames=["train.record"])
train_dataset = train_dataset.map(default_record_parser)
train_dataset = train_dataset.shuffle(buffer_size=1024).batch(BATCH_SIZE)

test_dataset = tf.data.TFRecordDataset(filenames=["test.record"])
test_dataset = test_dataset.map(default_record_parser).batch(BATCH_SIZE)

# TRAINING PART BELOW

inputs = keras.Input(shape=(224, 224, 3), name="images")
x = layers.Flatten()(inputs)
x = layers.Dense(64, activation="relu", name="dense_1")(x)
x = layers.Dense(64, activation="relu", name="dense_2")(x)
outputs = layers.Dense(10, name="predictions")(x)
model = keras.Model(inputs=inputs, outputs=outputs)

model.compile(optimizer=keras.optimizers.Adam(1e-4), loss=keras.losses.mean_squared_error, metrics=["acc"])

model.summary()

model.fit(train_dataset, epochs=EPOCHS)
result = model.evaluate(test_dataset)
print(dict(zip(model.metrics_names, result)))
