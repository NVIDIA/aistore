#/bin/bash
set -e

volcount=$1
if [ -z "$volcount" ]; then
    echo Using default volcount of 2
    volcount=2
fi

echo '' > attachvolumes.sh
echo '' > detachvolumes.sh
echo '' > deletevolumes.sh

declare -a devices=(l m n o p q r s t u v w x y z)
for i in `cat inventory/targets.txt; cat inventory/new_targets.txt`; do
        instanceId=$(aws ec2 describe-instances --filters "Name=private-ip-address,Values='$i'" | grep InstanceId | cut -d\" -f4)
        echo Target IP $i the instanceId is $instanceId
        for j in $(seq 1 $1); do
                volumeId=$(aws ec2 create-volume --availability-zone us-east-2b --volume-type standard --size 500 | grep VolumeId | cut -d\" -f4)
                echo New volume created $volumeId
                device=/dev/xvd${devices[$((j-1))]}
                echo aws ec2 attach-volume --volume-id ${volumeId} --instance-id ${instanceId} --device ${device} | tee -a attachvolumes.sh
                echo aws ec2 detach-volume --volume-id ${volumeId} --force | tee -a detachvolumes.sh
                echo aws ec2 delete-volume --volume-id ${volumeId} | tee -a deletevolumes.sh
        done
done

chmod 744 attachvolumes.sh
chmod 744 detachvolumes.sh
chmod 744 deletevolumes.sh

echo Give some time for volume create to finish, sleep for 30 sec
sleep 30
echo Running attachvolumes.sh

./attachvolumes.sh
