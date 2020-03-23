import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

from tar2tf import AisDataset
from tar2tf.ops import Select, Decode, Convert, Resize

EPOCHS = 5
BATCH_SIZE = 20

# ADJUST AisDataset PARAMETERS BELOW

BUCKET_NAME = "lb"
PROXY_URL = "http://localhost:8080"

# Create AisDataset.
# Values will be extracted from tar-records according to Resize(Convert(Decode("jpg"), tf.float32), (224, 224)) operation,
# meaning that bytes under "jpg" in tar-record will be decoded as an image, converted to tf.float32 type and then Resized to (224, 224)
# Labels will be extracted from tar-records according to Select("cls") operation, meaning that bytes under "cls" will be treated as label.
ais = AisDataset(BUCKET_NAME, PROXY_URL, Resize(Convert(Decode("jpg"), tf.float32), (224, 224)), Select("cls"))

# prepare your bucket first with Gavin's tars (gsutil ls gs://lpr-gtc2020)
train_dataset = ais.load_from_tar("train-{0..3}.tar.xz").shuffle(buffer_size=1024).batch(BATCH_SIZE)
test_dataset = ais.load_from_tar("train-{4..7}.tar.xz").batch(BATCH_SIZE)

# TRAINING PART BELOW

inputs = keras.Input(shape=(224, 224, 3), name="images")
x = layers.Flatten()(inputs)
x = layers.Dense(64, activation="relu", name="dense_1")(x)
x = layers.Dense(64, activation="relu", name="dense_2")(x)
outputs = layers.Dense(10, name="predictions")(x)
model = keras.Model(inputs=inputs, outputs=outputs)

model.compile(optimizer=keras.optimizers.Adam(1e-4), loss=keras.losses.mean_squared_error, metrics=["acc"])
model.summary()

# the results are probably useless
model.fit(train_dataset, epochs=EPOCHS)
result = model.evaluate(test_dataset)
print(dict(zip(model.metrics_names, result)))
