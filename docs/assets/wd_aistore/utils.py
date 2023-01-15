import matplotlib.pyplot as plt

import numpy as np
from torchvision import transforms

import webdataset as wds


# Utility that displays even number of images based on loader
# pylint: disable=unused-variable
def display_loader_images(data_loader, objects=2):
    test_iter = iter(data_loader)
    printed = 0
    row = 0
    _, axarr = plt.subplots((objects // 2), 2, figsize=(12, 12))
    while printed != objects:
        img_tensors, _ = next(test_iter)
        for img_tensor in img_tensors:
            column = printed % 2
            img = np.transpose(np.asarray(img_tensor.squeeze()), (1, 2, 0))
            img = np.clip(img, 0, 1)
            axarr[row, column].set_yticks([])
            axarr[row, column].set_xticks([])
            axarr[row, column].imshow(img, interpolation="nearest")
            printed += 1
            if column == 1:
                row += 1
            if printed == objects:
                plt.show()
                return
    plt.show()


# Utility for displaying images from shard
# pylint: disable=unused-variable
def display_shard_images(client, bucket, tar_name, objects=2, etl_name=""):
    to_tensor = transforms.Compose([transforms.ToTensor()])
    test_object = (
        wds.WebDataset(
            client.object_url(bucket, tar_name, transform_id=etl_name),
            handler=wds.handlers.warn_and_continue,
        )
        .decode("rgb")
        .to_tuple("jpg;png;jpeg;npy cls", handler=wds.handlers.warn_and_continue)
        .map_tuple(to_tensor, lambda x: x)
    )

    test_loader = wds.WebLoader(
        test_object,
        batch_size=None,
        shuffle=False,
        num_workers=1,
    )
    test_iter = iter(test_loader)
    row = 0
    _, axarr = plt.subplots((objects // 2), 2, figsize=(12, 12))
    for i in range(objects):
        column = i % 2
        img_tensor, _ = next(test_iter)
        plt.figure()
        img = np.transpose(np.asarray(img_tensor.squeeze()), (1, 2, 0))
        img = np.clip(img, 0, 1)
        axarr[row, column].set_yticks([])
        axarr[row, column].set_xticks([])
        axarr[row, column].imshow(img, interpolation="nearest")
        if column == 1:
            row += 1
    plt.show()
