#/bin/bash
set -e
export AWS_ACCESS_KEY_ID=AK
export AWS_SECRET_ACCESS_KEY=SK
export AWS_DEFAULT_REGION=us-east-2

declare -a devices=(l m n o p q r s t u v w x y z)
for i in `cat inventory/targets.txt`; do
	instanceId=$(aws ec2 describe-instances --filters "Name=private-ip-address,Values='$i'" | grep InstanceId | cut -d\" -f4)
	echo Target IP $i the instanceId is $instanceId
	for j in $(seq 1 $1); do 
		volumeId=$(aws ec2 create-volume --availability-zone us-east-2b --volume-type standard --size 1000 | grep VolumeId | cut -d\" -f4)
		echo New volume created $volumeId
		device=/dev/xvd${devices[$((j-1))]}
		echo aws ec2 attach-volume --volume-id ${volumeId} --instance-id ${instanceId} --device ${device} | tee -a attachvolumes.sh
	done	
done

chmod 777 attachvolumes.sh

echo Created attachvolumes.sh file in current dir, run it when volumes are ready to attach

