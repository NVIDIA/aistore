# Download all training data from ImageNet official website
# This takes approximately 200 GB disk space
nohup wget https://image-net.org/data/ILSVRC/2012/ILSVRC2012_img_train.tar
tar xf $IMAGENET_HOME/ILSVRC2012_img_train.tar -C $IMAGENET_HOME/train
cd $IMAGENET_HOME/train
for f in *.tar; do
 d=`basename $f .tar`
 mkdir $d
 tar xf $f -C $d
 rm $f
done

# Prepare validation data
wget https://image-net.org/data/ILSVRC/2012/ILSVRC2012_img_val.tar
tar xf $IMAGENET_HOME/ILSVRC2012_img_val.tar -C $IMAGENET_HOME/validation

# Prepare features
wget https://image-net.org/data/ILSVRC/2010/ILSVRC2010_feature_sbow_train.tar
tar xf $IMAGENET_HOME/ILSVRC2010_feature_sbow_train.tar -C $IMAGENET_HOME/label

# Prepare Annotations
wget https://image-net.org/data/ILSVRC/2012/ILSVRC2012_bbox_train_v2.tar.gz
tar xf $IMAGENET_HOME/ILSVRC2012_bbox_train_v2.tar.gz -C $IMAGENET_HOME/train_annotation

wget https://image-net.org/data/ILSVRC/2012/ILSVRC2012_bbox_val_v3.tgz
tar xf $IMAGENET_HOME/ILSVRC2012_bbox_train_v2.tar.gz -C $IMAGENET_HOME/validation_annotation

cd $IMAGENET_HOME/annotation
for f in *.tar.gz; do
 d=`basename $f .tar.gz`
 mkdir $d
 tar xf $f -C $d
 rm $f
done

# Remove original tar files to free up disk space
rm ILSVRC2012_bbox_train_v2  ILSVRC2012_bbox_val_v3.tgz  ILSVRC2010_feature_sbow_train.tar  ILSVRC2012_img_val.tar  ILSVRC2012_img_train.tar  nohup.out
