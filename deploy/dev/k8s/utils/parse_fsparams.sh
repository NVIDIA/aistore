
###################################
#
# fspaths config is used if and only if test_fspath_cnt == 0
# existence of each fspath is checked at runtime
#
###################################

test_fspath_cnt=0
fspath="\"\":{}"

echo "Select"
echo " 1: Local cache directories"
echo " 2: Filesystems"
echo "Enter your cache choice:"
read -r cache_source
if [[ $cache_source -eq 1 ]]; then
   echo "Enter number of local cache directories:"
   read -r test_fspath_cnt
   is_number ${test_fspath_cnt}
elif [[ $cache_source -eq 2 ]]; then
   echo "Enter filesystem info in comma separated format ex: /tmp/ais1,/tmp/ais:"
   read -r fsinfo
   fspath=""
   IFS=',' read -r -a array <<< "$fsinfo"
   for element in "${array[@]}"; do
      fspath="$fspath,\"$element\" : {} "
   done
   fspath=${fspath#","}
fi

export AIS_FS_PATHS=${fspath}
export TEST_FSPATH_COUNT=${test_fspath_cnt}

