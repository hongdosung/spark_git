#!/bin/bash
###########################################################################################
# - 개요: 하둡 파일 삭제 기능
#
###########################################################################################

to_day_to_sec= `date '+%s'`

### HDFS /home/spark/last_rdd_info/ 정보를 읽어 파일 시스템에 내용 저장
last_rdd_id_dir=/usr/spark/last_rdd_id

function get_rdd_range(){
    app_name=$1

    match_yn='N'
    rdd_save_all_app_name=`hdfs dfs -ls /home/spark/last_rdd_info | awk '{print $8}' | cut -d '/' -fs`
    for rdd_save_app_name in $rdd_save_all_app_name; do 
        if ["$rdd_save_app_name" == "$app_name"]; then
            match_yn='Y'
            break
        fi
    done

    if [ "$match_yn" == "Y" ]; then
        echo "$app_name: last_rdd_info 데이터 존재, 범위 확인"
        # 로컬 디렉토리 생성
        if [ ! -d "$last_rdd_id_dir/$app_name" ]; then
             mkdir $last_rdd_id_dir/$app_name
        fi

        cnt=1

        dataset_paths=`hdfs dfs -ls /home/spark/last_rdd_info/$app_name | awk '{print $8}'`
        for dataset_path in $dataset_paths; do
            hdfs dfs -getmerge $dataset_path $last_rdd_id_dir/$app_name/$cnt
            each_rdd_range=`cat $last_rdd_id_dir/$app_name/$cnt`
            aggr_rdd_range="$aggr_rdd_range $each_rdd_range"
            cnt=`expr $cnt + 1`
        done

        echo "$app_name: last_rdd_info 범우: $aggr_rdd_range"    
    else
        echo "$app_name: last_rdd_info  데이터 없음"
    fi 
}

# Define Delete Function
function del_old_rdd(){
    tgt_path=$1
    del_older_than_minute=$2

    if [ "$tgt_path" == "" ]; then
        echo "tgt_path should be set"
        return
    if

    # $6: date column
    # $7: time column
    # $8: path column
    # concat date and column without character -> sorting describe -> all lines execpt last 2 line
    hdfs dfs -ls ${tgt_path} | grep "^d" | awk '{print $6" "$7" "$8}' | sort -k1,1 -k2,2 | head -n -2 | while read line;do
        rdd_id=`echo $line | cut -d '/' -f7 | cut -d '-' -f2`

        # change expression of hdfs dir time to %Y%m%d%H%M EX) 2024112711125
        dir_date_str=$(echo ${line} | awk '{print $1" "$2}')KST
        dir_date_add_minute=`date -d "${dir_date_str} +${del_older_than_minute} minute"`
        dir_date_add_minute_to_sec=$(date -d "${dir_date_add_minute}" '+%s')
        difference=$(( ${to_day_to_sec} - ${dir_date_add_minute_to_sec} ))
        RDD_PATH=$(echo ${line} | awk 'print $3')
        echo "today_sec: $to_day_to_sec, dir_date_str: $dir_date_str, dir_date_add_minute: $dir_date_add_minute, dir_date_add_minute_to_sec: $dir_date_add_minute_to_sec, difference: $difference, RDD_PATH: $RDD_PATH"
        
        delete_yn=Y
        if [ $difference -gt 0 ]; then
            if [ ! "$aggr_rdd_range" == "" ]; then
                for rdd_range in $aggr_rdd_range; do
                    ahead_rdd_id=`echo $rdd_range | cut -d ',' -f1`
                    behind_rdd_id=`echo $rdd_range | cut -d ',' -f2`
                    if [ $rdd_id -gt $ahead_rdd_id ] && [ $rdd_id -lt $behind_rdd_id ]; then
                       delete_yn=N
                       echo "Delete skip($RDD_PATH) --> this is last checkpointed rdd, ahead: $ahead_rdd_id, behind: $behind_rdd_id"
                    fi
                done
        fi

        if [ $delete_yn == 'Y' ]; then
            echo "hdfs dfs -rm -r -skipTrash $RDD_PATH"
            hdfs dfs -rm -r -skipTrash $RDD_PATH
        fi
    done
}


app_name_paths=`hdfs dfs -ls /home/spark/chk_init_tbl | awk '{print $8}'`
for app_name_path in $app_name_paths ;do
    # app_name_path EX) /home/spark/chk_init_tbl/파일명
    # app_name_path 아래 job 디렉토리 삭제
    echo "app directory: $app_name_path"
    del_old_rdd $app_name_path
done


echo 'app/job_id 별 삭제 완료'
for app_name_path in $app_name_paths ;do
    job_paths=`hdfs dfs -ls $app_name_path | awk '{print $8}'`
    
    # job_path EX) /home/spark/chk_init_tbl/파일명/0000000
    # job_path 아래 실제 데이터가 저장된 rdd 디렉토리 삭제
    if [ "$job_paths" == "" ]; then
        echo "app directory: $app_name_path"
        continue
    if

    app_name=`echo $job_paths | cut -d ',' -f5`
    echo "$app_name: function get_rdd_range() start"
    aggr_rdd_range=""
    get_rdd_range $app_name

    # Run del_old_rdd funtion
    # 그룹별 older than 시간 기준을 다르게 실행
    echo "job directory: $job_paths"
    for job_path in job_paths; do
        del_old_rdd $job_path 20
    done
done

echo "app/job_id/checkpoint 별 삭제 완료"