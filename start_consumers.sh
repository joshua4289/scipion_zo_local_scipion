#!/bin/bash

module load global/cluster

check_username=`whoami`
echo username is $check_username
#if [[ $check_username != "gda2" ]]
#then
:
#echo "Script must be run as gda2..exiting "
#exit


#HACK:
#FIXME:HARDCODE path:needs to be a cluster path but when it's a trusted service it will be dlstbx.service instead

#DIALS_PYTHON=


qsub -N scip.cons <<EOF 2>&1
#!/bin/bash
#$ -q high.q ### Queue name
#$ -P em
#$ -j y ### Merge stdin and stdout
###=======================================================#
#$ -l gpu=1
#$ -l gpu_arch=Pascal
#$ -l exclusive
###=====================================================###
. /etc/profile.d/modules.sh
module load dials
# export DIALS_START_SERVICE="/home/jtq89441/workspace/zocalo_test/motioncor2_launcher/testing/share_local/start_services.py"

dials.python /home/jtq89441/workspace/zocalo_test/motioncor2_launcher/testing/share_local/start_services.py --live -s ScipionRunner
EOF
