#!/bin/sh

#change permission of generated file by Nifi
chmod_nifi_gen_file(){
    cat /dev/null > $1
    file_owner=$(ls -l $1 | awk -F' ' '{print $3}')
    if [ "${file_owner}" = "${nifi_user}" ]; then
        chmod 664 $1
    fi
}

# Re-kinit ticket cache file
re_kinit_ticket(){
    tmp_ticket_cache_file=$1
    tmp_kerberos_principal=$2
    tmp_logPath=$3
    tmp_zone=$4
    tmp_main_env_config=$5
    tmp_env_config=$6
    tmp_run_mode=$7
    tmp_business_date=$8
    tmp_logName=$9
    tmp_execution_id=$10
    tmp_processType=$11
    tmp_basePath=$12
    tmp_database_name=$13
    tmp_workflow_name=$14

    klistInfo=$(klist -c -f ${tmp_ticket_cache_file})
    result=""
    result+=$(printf 'Abort: Kinit ticket information before re-kinit.')$'\n'
    result+=$(printf 'Statement: %s' "$klistInfo")$'\n'
    python ${tmp_logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${tmp_zone}" -m "${tmp_main_env_config}" -e "${tmp_env_config}" -r "${tmp_run_mode}" -b "${tmp_business_date}" -sn "${tmp_logName}" -xi "${tmp_execution_id}" -t "${tmp_processType}" -p "${tmp_basePath}" -db "${tmp_database_name}" -w "${tmp_workflow_name}" -d "${result}"

    klistInfo=$(kinit -R -c ${tmp_ticket_cache_file} ${tmp_kerberos_principal})
    code=$?
    if [ $code -ne 0 ]; then
        result=""
        result+=$(printf 'Abort: Re-kinit ticket cache file failed.')$'\n'
        result+=$(printf 'Statement: %s' "$klistInfo")$'\n'
        python ${tmp_logPath}/log.py -o "failed" -s "false" -ec "0" -z "${tmp_zone}" -m "${tmp_main_env_config}" -e "${tmp_env_config}" -r "${tmp_run_mode}" -b "${tmp_business_date}" -sn "${tmp_logName}" -xi "${tmp_execution_id}" -t "${tmp_processType}" -p "${tmp_basePath}" -db "${tmp_database_name}" -w "${tmp_workflow_name}" -d "${result}"
    else
        result=""
        result+=$(printf 'Abort: Re-kinit ticket cache file succeeded.')$'\n'
        result+=$(printf 'Statement: %s' "$klistInfo")$'\n'
        python ${tmp_logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${tmp_zone}" -m "${tmp_main_env_config}" -e "${tmp_env_config}" -r "${tmp_run_mode}" -b "${tmp_business_date}" -sn "${tmp_logName}" -xi "${tmp_execution_id}" -t "${tmp_processType}" -p "${tmp_basePath}" -db "${tmp_database_name}" -w "${tmp_workflow_name}" -d "${result}"
    fi

    klistInfo=$(klist -c -f ${tmp_ticket_cache_file})
    result=""
    result+=$(printf 'Abort: Kinit ticket information after re-kinit.')$'\n'
    result+=$(printf 'Statement: %s' "$klistInfo")$'\n'
    python ${tmp_logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${tmp_zone}" -m "${tmp_main_env_config}" -e "${tmp_env_config}" -r "${tmp_run_mode}" -b "${tmp_business_date}" -sn "${tmp_logName}" -xi "${tmp_execution_id}" -t "${tmp_processType}" -p "${tmp_basePath}" -db "${tmp_database_name}" -w "${tmp_workflow_name}" -d "${result}"
}

#set hive parameter from optimization parameter or workflow config file
set_param_config_file(){
    optimization_parameter_config_file_param=$1
    workflow_config_file_param=$2
    ingestion_step_param=$3
    ingestion_command_file_param=$4
    
    begin_param_config="##begin ${ingestion_step_param}"
    end_param_config="##end ${ingestion_step_param}"
    
    optimization_parameter_config_file_value=`sed -n "/${begin_param_config}/,/${end_param_config}/{/${begin_param_config}/b;/${end_param_config}/b;p}" ${optimization_parameter_config_file_param}`
    
    #check workflow config file exists
    if [ -s "${workflow_config_file_param}" ]; then
        workflow_config_file_value=`sed -n "/${begin_param_config}/,/${end_param_config}/{/${begin_param_config}/b;/${end_param_config}/b;p}" ${workflow_config_file_param}`
        #check workflow config file empty
        if [ -z "${workflow_config_file_value}" ]; then
            #check optimization parameter config file empty
            if [ ! -z "${optimization_parameter_config_file_value}" ]; then
                sed -n "/${begin_param_config}/,/${end_param_config}/{/${begin_param_config}/b;/${end_param_config}/b;p}" ${optimization_parameter_config_file_param} | awk '{print "set "$0";"}' > ${ingestion_command_file_param}
            fi
        else
            sed -n "/${begin_param_config}/,/${end_param_config}/{/${begin_param_config}/b;/${end_param_config}/b;p}" ${workflow_config_file_param} | awk '{print "set "$0";"}' > ${ingestion_command_file_param}
        fi
    else
        #check optimization parameter config file empty
        if [ ! -z "${optimization_parameter_config_file_value}" ]; then
            sed -n "/${begin_param_config}/,/${end_param_config}/{/${begin_param_config}/b;/${end_param_config}/b;p}" ${optimization_parameter_config_file_param} | awk '{print "set "$0";"}' > ${ingestion_command_file_param}
        fi
    fi
}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Main Program                                                      
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#read opts
while getopts ":m:e:s:t:u:r:b:g:" opt
do
    case $opt in
        m)
        main_env_config=`echo $OPTARG | sed 's/ //g'` ;;
        e)
        env_config=`echo $OPTARG | sed 's/ //g'` ;;
        s)
        src_system=`echo $OPTARG | sed 's/ //g'` ;;
        t)
        table_name_option=`echo $OPTARG | sed 's/ //g'` ;;
        u)
        uuid=`echo $OPTARG | sed 's/ //g'` ;;
        r)
        run_mode=`echo $OPTARG | sed 's/ //g'` ;;
        b)
        rerun_business_date=`echo $OPTARG | sed 's/ //g'` ;;
        g)
        gen_skey_flag=`echo $OPTARG | sed 's/ //g'` ;;
    esac
done

. $main_env_config
. $env_config

source ${main_conda_env}

conda activate ${main_conda_env_name}

src_system=${src_system,,}
gen_table=${table_name_option,,}

basePath=${dapscripts_pln_igt}
logPath=${main_dapscripts_fwk_log}

processType="workflow"
zone="stgpst"

baseFolder="${dapscripts_fwk_register_oozie_param_stgpst}/${src_system}"
sqlScriptPath=${dapscripts_pln_igt_stgpst_script}

alias beelinecli=${alias}
pid=$(echo $$)
script_name=$(basename "$0")
ticket_cache_file=/tmp/krb5cc_${script_name}_${pid}
kinit -kt ~/${kerberos_user}.keytab ${kerberos_principal} -c ${ticket_cache_file}

export KRB5CCNAME=${ticket_cache_file}

if [ "$run_mode" == "" ]; then
    run_mode="N"
fi

#define parameter file
staging_persist_parameter_file="${baseFolder}/${gen_table}.cfg"

if [ ! -s "${staging_persist_parameter_file}" ]; then
    echo "The parameter file (${staging_persist_parameter_file}) does not exist or is the empty file!!"
    rm ${ticket_cache_file}
    exit 1
fi

#define optimization parameter config file
optimization_parameter_config_file=${ingestion_optimization_parameter_config_file}

if [ ! -s "${optimization_parameter_config_file}" ]; then
    echo "The optimization parameter config file (${optimization_parameter_config_file}) does not exist or is the empty file!!"
    rm ${ticket_cache_file}
    exit 1
fi

#declare variable
source_system=""
database_name=""
database_name_raw=""
database_name_stgpst=""
database_name_pst=""
table_name=""
column_name_stgpst=""
column_name_raw=""
business_date=""
workflow_id=""
key_column=""
workflow_name=""
check_null_data_column=""
check_key_null_data_column=""
table_name_error=""
column_name_stgpst_all=""
data_type_mismatch_condition=""
warning_column=""
column_name_raw_mismatch=""
data_type_mismatch_condition_check=""
data_type_mismatch_column=""
skey_column=""
remove_dup_column=""

state="STGPST"
n=1

#assign variable from parameter file
while read -r line; do
    if [ $n == "1" ]; then
        source_system=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "2" ]; then
        database_name_pst=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "3" ]; then
        database_name_stgpst=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "4" ]; then
        database_name_raw=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "5" ]; then
        table_name=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "6" ]; then
        column_name_stgpst=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "7" ]; then
        column_name_raw=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "8" ]; then
        workflow_id=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "9" ]; then
        key_column=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "10" ]; then
        workflow_name=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "11" ]; then
        check_null_data_column=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "12" ]; then
        check_key_null_data_column=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "13" ]; then
        table_name_error=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "14" ]; then
        column_name_stgpst_all=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "15" ]; then
        data_type_mismatch_condition=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "16" ]; then
        warning_column=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "17" ]; then
        column_name_raw_mismatch=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "18" ]; then
        data_type_mismatch_condition_check=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "19" ]; then
        data_type_mismatch_column=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "20" ]; then
        skey_column=$(cut -d'=' -f2- <<<"$line")
    elif [ $n == "21" ]; then
        remove_dup_column=$(cut -d'=' -f2- <<<"$line")
    fi

    n=$((n + 1))
done < ${staging_persist_parameter_file}

database_name="${database_name_stgpst}"
real_table_name_error="${table_name_error//\`/}"
table_name_error_mismatch_temp="${real_table_name_error}_mismatch_temp"
table_name_error_mismatch_temp="\`${table_name_error_mismatch_temp}\`"

logName=$(python ${logPath}/log.py -o "generate" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -sn "" -xi "" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "")

processStart=$(date)

################################################################################
#							Get business date
################################################################################

if [ "$run_mode" == "R" ]; then
    business_date=${rerun_business_date}
else
    business_date=$(python ${main_dapscripts_fwk_control}/get_business_date.py -m "${main_env_config}" -e "${env_config}" -w "${workflow_id}" 2>&1)
    code=$?
    if [ $code -ne 0 ]; then
        err_message=$business_date
        business_date="None"
        result=""
        result+=$(printf 'Abort: Database (FAILED)')$'\n'
        result+=$(printf 'Execution_ID: %s' "None")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "None")$'\n'
        result+=$(printf 'Statement: %s' "$err_message")$'\n'
        python ${logPath}/log.py -o "failed" -s "false" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "None" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
fi

if [ "$business_date" == "None" ]; then
    result=""
    result+=$(printf 'Abort: Business Date cannot be found')$'\n'
    result+=$(printf 'Workflow_ID: %s' "$workflow_id")$'\n'
    result+=$(printf 'Run_Mode: %s' "$run_mode")$'\n'
    result+=$(printf 'State: %s' "$state")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    python ${logPath}/log.py -o "failed" -s "false" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    rm ${ticket_cache_file}
    exit 1
fi

column_name_raw=`echo "${column_name_raw}"|sed "s/{business_date}/'${business_date}'/g"|sed "s/{BUSINESS_DATE}/'${business_date}'/g"`
data_type_mismatch_condition_check=`echo "${data_type_mismatch_condition_check}"|sed "s/{business_date}/'${business_date}'/g"|sed "s/{BUSINESS_DATE}/'${business_date}'/g"`

real_database_name_raw="${database_name_raw//\`/}"
real_database_name_stgpst="${database_name_stgpst//\`/}"
real_database_name_pst="${database_name_pst//\`/}"
real_table_name="${table_name//\`/}"
real_temp_sky="${real_table_name}_temp_skey"
real_temp_sky="\`${real_temp_sky}\`"

mkdir -p ${dapscripts_pln_tfm_stgpst}/${src_system}

resultFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_insert_to_stgpst.hql"
PreresultFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_prereconcile.hql"
RecresultFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_reconcile.hql"
KeyNullresultFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_check_key_null_data.hql"
WarningresultFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_check_warning_flag.hql"
RemoveDupresultFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_remove_duplicate_data.hql"

PreoutputFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_prereconcile.out"
RecoutputFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_reconcile.out"
KeyNullputFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_check_key_null_data.out"
WarningputFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_check_warning_flag.out"

genSkeyPath="${dapscripts_pln_tfm_stgpst}/gen_skey"
genGetSkeyFiles="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_gen_get_skey.hql"

MisMatchrejFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_reject_mis_match_data_type.hql"
NullrejFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_reject_null_data.hql"
DuprejFile="${dapscripts_pln_tfm_stgpst}/${src_system}/${real_database_name_pst}_${real_table_name}_reject_dup_data.hql"

detail_log_path=`echo ${logName} | awk -F',' '{print $3}'`
detail_log_file=`echo ${logName} | awk -F',' '{print $1}' | sed 's/^rerun_prc_/rerun_detail_/g'| sed 's/^prc_/detail_/g'`
detail_log=${detail_log_path}/${detail_log_file}
ErrorFile="${dapscripts_pln_igt}/log/errorlog/${workflow_name}.err"

cat /dev/null > ${detail_log}

# setup dir permission for dapscripts_pln_tfm_stgpst

dir_owner=$(ls -ld ${dapscripts_pln_tfm_stgpst}/${src_system} | awk -F' ' '{print $3}')

if [ "$(whoami)" == "${nifi_user}" ]; then
    if [ "${dir_owner}" == "${nifi_user}" ]; then
        chmod 775 ${dapscripts_pln_tfm_stgpst}/${src_system}
    fi
    chmod_nifi_gen_file ${resultFile}
    chmod_nifi_gen_file ${PreresultFile}
    chmod_nifi_gen_file ${RecresultFile}
    chmod_nifi_gen_file ${KeyNullresultFile}
    chmod_nifi_gen_file ${WarningresultFile}
    chmod_nifi_gen_file ${RemoveDupresultFile}
    chmod_nifi_gen_file ${PreoutputFile}
    chmod_nifi_gen_file ${RecoutputFile}
    chmod_nifi_gen_file ${KeyNullputFile}
    chmod_nifi_gen_file ${WarningputFile}
    chmod_nifi_gen_file ${genGetSkeyFiles}
    chmod_nifi_gen_file ${MisMatchrejFile}
    chmod_nifi_gen_file ${NullrejFile}
    chmod_nifi_gen_file ${DuprejFile}
else
    cat /dev/null > ${resultFile}
    cat /dev/null > ${PreresultFile}
    cat /dev/null > ${RecresultFile}
    cat /dev/null > ${KeyNullresultFile}
    cat /dev/null > ${WarningresultFile}
    cat /dev/null > ${RemoveDupresultFile}
    cat /dev/null > ${PreoutputFile}
    cat /dev/null > ${RecoutputFile}
    cat /dev/null > ${KeyNullputFile}
    cat /dev/null > ${WarningputFile}
    cat /dev/null > ${genGetSkeyFiles}
    cat /dev/null > ${MisMatchrejFile}
    cat /dev/null > ${NullrejFile}
    cat /dev/null > ${DuprejFile}
fi

#define workflow config file
workflow_config_file=${baseFolder}/${real_database_name_stgpst}.${real_table_name}.optimization_parameter.cfg

################################################################################
#							Get Execution ID
################################################################################

execution_id=$(python ${main_dapscripts_fwk_control}/get_execution_id_table.py -m "${main_env_config}" -e "${env_config}" -wi "${workflow_id}" -b "${business_date}" -r "${run_mode}" -s "${state}" -wn "${workflow_name}" -u "${uuid}" -z "STGPST" 2>&1)
code=$?
if [ $code -ne 0 ]; then
    err_message=$execution_id
    execution_id="executionId:None"
    result=""
    result+=$(printf 'Abort: Database (FAILED)')$'\n'
    result+=$(printf 'Execution_ID: %s' "None")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
    result+=$(printf 'Statement: %s' "$err_message")$'\n'
    python ${logPath}/log.py -o "failed" -s "false" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
fi
execution_id="$(cut -d':' -f2 <<<$execution_id)"

if [ "$execution_id" == "None" ]; then
    result=""
    result+=$(printf 'Abort: Execution ID cannot be found')$'\n'
    result+=$(printf 'Workflow_ID: %s' "$workflow_id")$'\n'
    result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
    result+=$(printf 'Run_Mode: %s' "$run_mode")$'\n'
    result+=$(printf 'State: %s' "$state")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    python ${logPath}/log.py -o "failed" -s "false" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    rm ${ticket_cache_file}
    exit 1
fi

################################################################################
#							Create reject record data type mismatch
################################################################################

if [ "${data_type_mismatch_condition}" != "" ]; then

    set_param_config_file "${optimization_parameter_config_file}" "${workflow_config_file}" "create reject record data type mismatch staging persist" "${MisMatchrejFile}"
    echo "create temporary table ${database_name_stgpst}.${table_name_error_mismatch_temp} as " >>$MisMatchrejFile
    echo "select ${data_type_mismatch_condition_check}, *" >>$MisMatchrejFile
    echo "from ${database_name_raw}.${table_name};" >>$MisMatchrejFile
    echo "" >>$MisMatchrejFile
    echo "select ${data_type_mismatch_column} as data_type_mismatch_column, ${column_name_raw_mismatch}" >>$MisMatchrejFile
    echo "from ${database_name_stgpst}.${table_name_error_mismatch_temp}" >>$MisMatchrejFile
    echo "where ${data_type_mismatch_condition};" >>$MisMatchrejFile

    ################################################################################
    #							Re-kinit
    ################################################################################
    re_kinit_ticket "${ticket_cache_file}" "${kerberos_principal}" "${logPath}" "${zone}" "${main_env_config}" "${env_config}" "${run_mode}" "${business_date}" "${logName}" "${execution_id}" "${processType}" "${basePath}" "${database_name}" "${workflow_name}"

    start=$(date)
    echo "################# RAW TO STAGING PERSIST (Create reject record Data Type Mismatch) : `date +'%F %T'` #################" >> ${detail_log}
    beelinecli -n ${beeline_user} --showHeader=true --outputformat=dsv --delimiterForDSV="|" -f "$MisMatchrejFile" >${ErrorFile} 2>> ${detail_log}
    code=$?
    echo "" >> ${detail_log}
    end=$(date)

    if [ $code -ne 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "QUERY FAILED" -erl "RAW TO STAGING PERSIST (Create reject record Data Type Mismatch)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        data=$(cat ${MisMatchrejFile})
        result=""
        result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Create reject record Data Type Mismatch)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    else
        startTime=$(date -u -d "$start" +"%s")
        endTime=$(date -u -d "$end" +"%s")
        DIFF=$(($endTime - $startTime))
        duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
        data=$(cat ${MisMatchrejFile})
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Create reject record Data Type Mismatch)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Start: %s' "$start")$'\n'
        result+=$(printf 'End: %s' "$end")$'\n'
        result+=$(printf 'Duration: %s' "$duration")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
fi

################################################################################
#							Check data type mismatch
################################################################################

if [ "${data_type_mismatch_condition}" != "" ]; then
    
    start=$(date)
    check_mismatch_count=$(awk 'END { print NR - 1 }' ${ErrorFile})
    end=$(date)
    
    if [ ${check_mismatch_count} -gt 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "CHECK DATA TYPE MISMATCH FAILED" -erl "RAW TO STAGING PERSIST (Check Data Type Mismatch)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Check Data Type Mismatch)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Table_Name: %s' "$table_name")$'\n'
        result+=$(printf 'Check_Mismatch_Count: %s' "$check_mismatch_count")$'\n'
        result+=$(printf 'Statement: %s' "awk 'END { print NR - 1 }' ${ErrorFile}")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "5" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    else
        startTime=$(date -u -d "$start" +"%s")
        endTime=$(date -u -d "$end" +"%s")
        DIFF=$(($endTime - $startTime))
        duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Check Data Type Mismatch)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Start: %s' "$start")$'\n'
        result+=$(printf 'End: %s' "$end")$'\n'
        result+=$(printf 'Duration: %s' "$duration")$'\n'
        result+=$(printf 'Statement: %s' "awk 'END { print NR - 1 }' ${ErrorFile}")$'\n'
        python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm -f ${ErrorFile}
    fi
fi

# Check warning flag column
if [ "${warning_column}" != "" ]; then

    set_param_config_file "${optimization_parameter_config_file}" "${workflow_config_file}" "check warning column staging persist" "${WarningresultFile}"
    echo "select ${warning_column} as warning_column_data" >>$WarningresultFile
    echo "from ${database_name_raw}.${table_name};" >>$WarningresultFile

    ################################################################################
    #							Re-kinit
    ################################################################################
    re_kinit_ticket "${ticket_cache_file}" "${kerberos_principal}" "${logPath}" "${zone}" "${main_env_config}" "${env_config}" "${run_mode}" "${business_date}" "${logName}" "${execution_id}" "${processType}" "${basePath}" "${database_name}" "${workflow_name}"

    start=$(date)
    echo "################# RAW TO STAGING PERSIST (Check Warning Column) : `date +'%F %T'` #################" >> ${detail_log}
    beelinecli -n ${beeline_user} --showHeader=false --outputformat=dsv --delimiterForDSV="|" -f "$WarningresultFile" >${WarningputFile} 2>> ${detail_log}
    code=$?
    echo "" >> ${detail_log}
    end=$(date)

    if [ $code -ne 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "CHECK WARNING COLUMN FAILED" -erl "RAW TO STAGING PERSIST (Check Warning Column)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        data=$(cat ${WarningresultFile})
        result=""
        result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Check Warning Flag Column)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "5" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    fi
    
    # if found columns that wrong value length
    if [ "$(cat ${WarningputFile})" != "" ]; then
        warning_column_value=$(cat ${WarningputFile} | sed 's/  */, /g')

        startTime=$(date -u -d "$start" +"%s")
        endTime=$(date -u -d "$end" +"%s")
        DIFF=$(($endTime - $startTime))
        duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
        data=$(cat ${WarningresultFile})
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Check Warning Flag Column)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Start: %s' "$start")$'\n'
        result+=$(printf 'End: %s' "$end")$'\n'
        result+=$(printf 'Duration: %s' "$duration")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        result+=$(printf 'Warning message: the data has been truncated in column %s' "$warning_column_value")$'\n'
        python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
fi


################################################################################
#							Prereconcile
################################################################################

set_param_config_file "${optimization_parameter_config_file}" "${workflow_config_file}" "prereconcile staging persist" "${PreresultFile}"
echo "select concat('total record (${table_name})=',count(*)) as count_record" >>$PreresultFile
echo "from ${database_name_raw}.${table_name};" >>$PreresultFile

################################################################################
#							Re-kinit
################################################################################
re_kinit_ticket "${ticket_cache_file}" "${kerberos_principal}" "${logPath}" "${zone}" "${main_env_config}" "${env_config}" "${run_mode}" "${business_date}" "${logName}" "${execution_id}" "${processType}" "${basePath}" "${database_name}" "${workflow_name}"

start=$(date)
echo "################# RAW TO STAGING PERSIST (Prereconcile) : `date +'%F %T'` #################" >> ${detail_log}
beelinecli -n ${beeline_user} --showHeader=false --outputformat=dsv --delimiterForDSV="|" -f "$PreresultFile" >${PreoutputFile} 2>> ${detail_log}
code=$?
echo "" >> ${detail_log}
end=$(date)

if [ $code -ne 0 ]; then
    err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "QUERY FAILED" -erl "RAW TO STAGING PERSIST (Prereconcile)" 2>&1)
    code=$?
    if [ $code -ne 0 ]; then
        result=""
        result+=$(printf 'Abort: Database (FAILED)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$err_message")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
    data=$(cat ${PreresultFile})
    result=""
    result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Prereconcile)')$'\n'
    result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
    result+=$(printf 'Statement: %s' "$data")$'\n'
    python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    rm ${ticket_cache_file}
    exit 1
else
    startTime=$(date -u -d "$start" +"%s")
    endTime=$(date -u -d "$end" +"%s")
    DIFF=$(($endTime - $startTime))
    duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
    data=$(cat ${PreresultFile})
    result=""
    result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Prereconcile)')$'\n'
    result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
    result+=$(printf 'Start: %s' "$start")$'\n'
    result+=$(printf 'End: %s' "$end")$'\n'
    result+=$(printf 'Duration: %s' "$duration")$'\n'
    result+=$(printf 'Statement: %s' "$data")$'\n'
    python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
fi

total_record_source=$(cut -d'=' -f2 ${PreoutputFile})

################################################################################
#							Loading
################################################################################

set_param_config_file "${optimization_parameter_config_file}" "${workflow_config_file}" "loading staging persist" "${resultFile}"
echo "truncate table ${database_name_stgpst}.${table_name};" >>$resultFile
echo "insert into ${database_name_stgpst}.${table_name} (${column_name_stgpst})" >>$resultFile
echo "select ${column_name_raw}" >>$resultFile
echo "from ${database_name_raw}.${table_name};" >>$resultFile

################################################################################
#							Re-kinit
################################################################################
re_kinit_ticket "${ticket_cache_file}" "${kerberos_principal}" "${logPath}" "${zone}" "${main_env_config}" "${env_config}" "${run_mode}" "${business_date}" "${logName}" "${execution_id}" "${processType}" "${basePath}" "${database_name}" "${workflow_name}"

start=$(date)
echo "################# RAW TO STAGING PERSIST (Loading) : `date +'%F %T'` #################" >> ${detail_log}
beelinecli -n ${beeline_user} -f "$resultFile" 2>> ${detail_log}
code=$?
echo "" >> ${detail_log}
end=$(date)

if [ $code -ne 0 ]; then
    err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "QUERY FAILED" -erl "RAW TO STAGING PERSIST (Loading)" 2>&1)
    code=$?
    if [ $code -ne 0 ]; then
        result=""
        result+=$(printf 'Abort: Database (FAILED)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$err_message")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
    data=$(cat ${resultFile})
    result=""
    result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Loading)')$'\n'
    result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
    result+=$(printf 'Statement: %s' "$data")$'\n'
    python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    rm ${ticket_cache_file}
    exit 1
else
    startTime=$(date -u -d "$start" +"%s")
    endTime=$(date -u -d "$end" +"%s")
    DIFF=$(($endTime - $startTime))
    duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
    data=$(cat ${resultFile})
    result=""
    result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Loading)')$'\n'
    result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
    result+=$(printf 'Start: %s' "$start")$'\n'
    result+=$(printf 'End: %s' "$end")$'\n'
    result+=$(printf 'Duration: %s' "$duration")$'\n'
    result+=$(printf 'Statement: %s' "$data")$'\n'
    python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
fi

################################################################################
#							Generate and Get Skey
################################################################################

#Generate Skey
if [ "${skey_column}" != "" ] && [ "${gen_skey_flag}" = "Y" ]; then

    start=$(date)
    code=0
    
    rm -f ${genSkeyPath}/loaded/gen-${real_database_name_stgpst,,}.${real_table_name,,}-*-skey-*.hql*
    
    current_date=`date +'%Y%m'`
    current_date_short=`date +'%y%m'`
    
    #Replace value in generate skey sql command file
    for gen_file in ${sqlScriptPath}/gen-${real_database_name_stgpst,,}.${real_table_name,,}-*-skey-*.hql
    do
        if [ -e "${gen_file}" ]; then
            gen_filename=`basename ${gen_file}`
            cat ${gen_file} | sed "s/{workflow_name}/${workflow_name}/g" | sed "s/{business_date}/${business_date}/g" | sed "s/{current_date}/${current_date}/g" | sed "s/{current_date_short}/${current_date_short}/g" > ${genSkeyPath}/loaded/${gen_filename}
        else
            err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "Generate skey file (${gen_file}) does not exist." -erl "script ${main_dapscripts_fwk_igt_raw_pst}/gen_script_insert_to_stgpst.sh" 2>&1)
            code=$?
            if [ $code -ne 0 ]; then
                result=""
                result+=$(printf 'Abort: Database (FAILED)')$'\n'
                result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
                result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
                result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
                result+=$(printf 'Statement: %s' "$err_message")$'\n'
                python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
            fi
            result=""
            result+=$(printf 'Abort: %s' "Shell script ${main_dapscripts_fwk_igt_raw_pst}/gen_script_insert_to_stgpst.sh")$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "Generate skey file (${gen_file}) does not exist.")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
            echo "Generate skey file (${gen_file}) does not exist!"
            rm ${ticket_cache_file}
            exit 1
        fi
    done
    
    #Call script for generate skey
    for prep_file in ${sqlScriptPath}/gen-${real_database_name_stgpst,,}.${real_table_name,,}-*-skey-*.hql.prepare
    do
        if [ -e "${prep_file}" ]; then
            sh ${main_dapscripts_fwk_igt_raw_pst}/generate_skey_prepare.sh -m ${main_env_config_file} -e ${env_config_file} -f ${prep_file} -w ${workflow_name} &
            PIDS+=($!)
        else
            err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "Prepare skey file (${prep_file}) does not exist." -erl "script ${main_dapscripts_fwk_igt_raw_pst}/gen_script_insert_to_stgpst.sh" 2>&1)
            code=$?
            if [ $code -ne 0 ]; then
                result=""
                result+=$(printf 'Abort: Database (FAILED)')$'\n'
                result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
                result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
                result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
                result+=$(printf 'Statement: %s' "$err_message")$'\n'
                python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
            fi
            result=""
            result+=$(printf 'Abort: %s' "Shell script ${main_dapscripts_fwk_igt_raw_pst}/gen_script_insert_to_stgpst.sh")$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "Prepare skey file (${prep_file}) does not exist.")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
            echo "Prepare skey file (${prep_file}) does not exist!"
            rm ${ticket_cache_file}
            exit 1
        fi
    done
    
    for procid in ${PIDS[@]}; do
        wait ${procid}
        STATUS+=($?)
    done
    
    #Check result of generate skey from exit code
    for stcode in ${STATUS[@]}; do
        if [[ ${stcode} -ne 0 ]]; then
            code=1
        fi
    done
    
    cat ${genSkeyPath}/loaded/gen-${real_database_name_stgpst,,}.${real_table_name,,}-*-skey-*.hql.prepare_execute > ${genSkeyPath}/loaded/gen-${real_database_name_stgpst,,}.${real_table_name,,}-group_domain_id-skey-table.hql.execute
    cat ${genSkeyPath}/loaded/gen-${real_database_name_stgpst,,}.${real_table_name,,}-*-skey-*.hql.generate_execute >> ${genSkeyPath}/loaded/gen-${real_database_name_stgpst,,}.${real_table_name,,}-group_domain_id-skey-table.hql.execute
    data=`cat ${genSkeyPath}/loaded/gen-${real_database_name_stgpst,,}.${real_table_name,,}-group_domain_id-skey-table.hql.execute`
    
    rm -f ${genSkeyPath}/loaded/gen-${real_database_name_stgpst,,}.${real_table_name,,}-*-skey-*.hql*
    
    end=$(date)
    
    if [ $code -ne 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "QUERY FAILED" -erl "RAW TO STAGING PERSIST (Generate Skey)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        result=""
        result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Generate Skey)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    else
        startTime=$(date -u -d "$start" +"%s")
        endTime=$(date -u -d "$end" +"%s")
        DIFF=$(($endTime - $startTime))
        duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Generate Skey)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Start: %s' "$start")$'\n'
        result+=$(printf 'End: %s' "$end")$'\n'
        result+=$(printf 'Duration: %s' "$duration")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi

fi

#Get Skey
if [ "${skey_column}" != "" ]; then
    
    if [ -e "${sqlScriptPath}/get-${real_database_name_stgpst,,}.${real_table_name,,}.hql" ]; then
        set_param_config_file "${optimization_parameter_config_file}" "${workflow_config_file}" "get skey staging persist" "${genGetSkeyFiles}"
        echo "truncate table ${database_name_stgpst}.${real_temp_sky};" >>${genGetSkeyFiles}
        echo "" >>${genGetSkeyFiles}
        echo "insert into ${database_name_stgpst}.${real_temp_sky} (${column_name_stgpst})" >>${genGetSkeyFiles}
        echo "select ${column_name_stgpst}" >>${genGetSkeyFiles}
        echo "from ${database_name_stgpst}.${table_name};" >>${genGetSkeyFiles}
        echo "" >>${genGetSkeyFiles}
        echo "truncate table ${database_name_stgpst}.${table_name};" >>${genGetSkeyFiles}
        echo "" >>${genGetSkeyFiles}
        
        cat ${sqlScriptPath}/get-${real_database_name_stgpst,,}.${real_table_name,,}.hql >>${genGetSkeyFiles}
        echo "" >>${genGetSkeyFiles}
    else
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "GET SKEY FAILED" -erl "RAW TO STAGING PERSIST (Get Skey)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        data="Sql command file for get skey (${sqlScriptPath}/get-${real_database_name_stgpst,,}.${real_table_name,,}.hql) does not exists."
        result=""
        result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Get Skey)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    fi
  
    ################################################################################
    #							Re-kinit
    ################################################################################
    re_kinit_ticket "${ticket_cache_file}" "${kerberos_principal}" "${logPath}" "${zone}" "${main_env_config}" "${env_config}" "${run_mode}" "${business_date}" "${logName}" "${execution_id}" "${processType}" "${basePath}" "${database_name}" "${workflow_name}"
  
    start=$(date)
    echo "################# RAW TO STAGING PERSIST (Get Skey) : `date +'%F %T'` #################" >> ${detail_log}
    beelinecli -n ${beeline_user} -f "$genGetSkeyFiles" 2>> ${detail_log}
    code=$?
    echo "" >> ${detail_log}
    end=$(date)
    
    if [ $code -ne 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "QUERY FAILED" -erl "RAW TO STAGING PERSIST (Get Skey)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        data=$(cat ${genGetSkeyFiles})
        result=""
        result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Get Skey)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    else
        startTime=$(date -u -d "$start" +"%s")
        endTime=$(date -u -d "$end" +"%s")
        DIFF=$(($endTime - $startTime))
        duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
        data=$(cat ${genGetSkeyFiles})
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Get Skey)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Start: %s' "$start")$'\n'
        result+=$(printf 'End: %s' "$end")$'\n'
        result+=$(printf 'Duration: %s' "$duration")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi

fi

################################################################################
#							Reconcile
################################################################################

set_param_config_file "${optimization_parameter_config_file}" "${workflow_config_file}" "reconcile staging persist" "${RecresultFile}"
echo "select concat('total record (${table_name})=',count(*)) as count_record" >>$RecresultFile
echo "from ${database_name_stgpst}.${table_name};" >>$RecresultFile

################################################################################
#							Re-kinit
################################################################################
re_kinit_ticket "${ticket_cache_file}" "${kerberos_principal}" "${logPath}" "${zone}" "${main_env_config}" "${env_config}" "${run_mode}" "${business_date}" "${logName}" "${execution_id}" "${processType}" "${basePath}" "${database_name}" "${workflow_name}"

start=$(date)
echo "################# RAW TO STAGING PERSIST (Reconcile) : `date +'%F %T'` #################" >> ${detail_log}
beelinecli -n ${beeline_user} --showHeader=false --outputformat=dsv --delimiterForDSV="|" -f "$RecresultFile" >${RecoutputFile} 2>> ${detail_log}
code=$?
echo "" >> ${detail_log}
end=$(date)

if [ $code -ne 0 ]; then
    err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "QUERY FAILED" -erl "RAW TO STAGING PERSIST (Reconcile)" 2>&1)
    code=$?
    if [ $code -ne 0 ]; then
        result=""
        result+=$(printf 'Abort: Database (FAILED)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$err_message")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
    data=$(cat ${RecresultFile})
    result=""
    result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Reconcile)')$'\n'
    result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
    result+=$(printf 'Statement: %s' "$data")$'\n'
    python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    rm ${ticket_cache_file}
    exit 1
fi

total_record_target=$(cut -d'=' -f2 ${RecoutputFile})

err_message=$(python ${main_dapscripts_fwk_control}/write_reconcile_log.py -m "${main_env_config}" -e "${env_config}" -ei ${execution_id} -dbns "${real_database_name_raw}" -dbnt "${real_database_name_stgpst}" -tb "${real_table_name}" -trs ${total_record_source} -trt ${total_record_target} -rp "total" -bd "${business_date}" 2>&1)
code=$?
if [ $code -ne 0 ]; then
    result=""
    result+=$(printf 'Abort: Database (FAILED)')$'\n'
    result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
    result+=$(printf 'Statement: %s' "$err_message")$'\n'
    python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    rm ${ticket_cache_file}
    exit 1    
fi
err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log_reconcile.py -m "${main_env_config}" -e "${env_config}" -ei ${execution_id} -dbns "${real_database_name_raw}" -dbnt "${real_database_name_stgpst}" -tb "${real_table_name}" -trs ${total_record_source} -trt ${total_record_target} -crt "" -nrt "" -drt "" 2>&1)
if [ $code -ne 0 ]; then
    result=""
    result+=$(printf 'Abort: Database (FAILED)')$'\n'
    result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
    result+=$(printf 'Statement: %s' "$err_message")$'\n'
    python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    rm ${ticket_cache_file}
    exit 1    
fi

if [ ${total_record_source} -ne ${total_record_target} ]; then
    err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "RECONCILE FAILED" -erl "RAW TO STAGING PERSIST (Reconcile)" 2>&1)
    code=$?
    if [ $code -ne 0 ]; then
        result=""
        result+=$(printf 'Abort: Database (FAILED)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$err_message")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
    data=$(cat ${RecresultFile})
    result=""
    result+=$(printf 'Abort: RAW TO STAGING PERSIST (Reconcile)')$'\n'
    result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
    result+=$(printf 'Table_Name: %s' "$table_name")$'\n'
    result+=$(printf 'Source: %s' "$database_name_raw")$'\n'
    result+=$(printf 'Target: %s' "$database_name_stgpst")$'\n'
    result+=$(printf 'Source_Total_Record: %s' "$total_record_source")$'\n'
    result+=$(printf 'Target_Total_Record: %s' "$total_record_target")$'\n'
    result+=$(printf 'Statement: %s' "$data")$'\n'
    python ${logPath}/log.py -o "failed" -s "true" -ec "3" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    rm ${ticket_cache_file}
    exit 1
else
    startTime=$(date -u -d "$start" +"%s")
    endTime=$(date -u -d "$end" +"%s")
    DIFF=$(($endTime - $startTime))
    duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
    data=$(cat ${RecresultFile})
    result=""
    result+=$(printf 'Abort: RAW TO STAGING PERSIST (Reconcile)')$'\n'
    result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
    result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
    result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
    result+=$(printf 'Source_Total_Record: %s' "$total_record_source")$'\n'
    result+=$(printf 'Target_Total_Record: %s' "$total_record_target")$'\n'
    result+=$(printf 'Start: %s' "$start")$'\n'
    result+=$(printf 'End: %s' "$end")$'\n'
    result+=$(printf 'Duration: %s' "$duration")$'\n'
    result+=$(printf 'Statement: %s' "$data")$'\n'
    python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
fi

################################################################################
#							Remove duplicate data
################################################################################

if [ "${remove_dup_column}" != "" ]; then
    set_param_config_file "${optimization_parameter_config_file}" "${workflow_config_file}" "remove duplicate data staging persist" "${RemoveDupresultFile}"
    echo "insert overwrite table ${database_name_stgpst}.${table_name}" >>$RemoveDupresultFile
    echo "select ${column_name_stgpst}" >>$RemoveDupresultFile
    echo "from (" >>$RemoveDupresultFile
    echo "select *, ROW_NUMBER() OVER(PARTITION BY ${remove_dup_column} ORDER BY ${remove_dup_column}) rk" >>$RemoveDupresultFile
    echo "from ${database_name_stgpst}.${table_name}" >>$RemoveDupresultFile
    echo ") sub" >>$RemoveDupresultFile
    echo "where rk = 1" >>$RemoveDupresultFile

    ################################################################################
    #							Re-kinit
    ################################################################################
    re_kinit_ticket "${ticket_cache_file}" "${kerberos_principal}" "${logPath}" "${zone}" "${main_env_config}" "${env_config}" "${run_mode}" "${business_date}" "${logName}" "${execution_id}" "${processType}" "${basePath}" "${database_name}" "${workflow_name}"

    start=$(date)
    echo "################# RAW TO STAGING PERSIST (Remove duplicate data) : `date +'%F %T'` #################" >> ${detail_log}
    beelinecli -n ${beeline_user} -f "$RemoveDupresultFile" 2>> ${detail_log}
    code=$?
    echo "" >> ${detail_log}
    end=$(date)

    if [ $code -ne 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "QUERY FAILED" -erl "RAW TO STAGING PERSIST (Remove duplicate data)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        data=$(cat ${RemoveDupresultFile})
        result=""
        result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Remove duplicate data)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    else
        startTime=$(date -u -d "$start" +"%s")
        endTime=$(date -u -d "$end" +"%s")
        DIFF=$(($endTime - $startTime))
        duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
        data=$(cat ${RemoveDupresultFile})
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Remove duplicate data)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Start: %s' "$start")$'\n'
        result+=$(printf 'End: %s' "$end")$'\n'
        result+=$(printf 'Duration: %s' "$duration")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
fi

################################################################################
#							Create reject record null data
################################################################################

if [ "${check_null_data_column}" != "" ]; then
    set_param_config_file "${optimization_parameter_config_file}" "${workflow_config_file}" "create reject record null data staging persist" "${NullrejFile}"
    echo "select *" >>$NullrejFile
    echo "from ${database_name_stgpst}.${table_name}" >>$NullrejFile
    echo "where ${check_null_data_column};" >>$NullrejFile

    ################################################################################
    #							Re-kinit
    ################################################################################
    re_kinit_ticket "${ticket_cache_file}" "${kerberos_principal}" "${logPath}" "${zone}" "${main_env_config}" "${env_config}" "${run_mode}" "${business_date}" "${logName}" "${execution_id}" "${processType}" "${basePath}" "${database_name}" "${workflow_name}"

    start=$(date)
    echo "################# RAW TO STAGING PERSIST (Create reject record Null Data) : `date +'%F %T'` #################" >> ${detail_log}
    beelinecli -n ${beeline_user} --showHeader=true --outputformat=dsv --delimiterForDSV="|" -f "$NullrejFile" >${ErrorFile} 2>> ${detail_log}
    code=$?
    echo "" >> ${detail_log}
    end=$(date)

    if [ $code -ne 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "QUERY FAILED" -erl "RAW TO STAGING PERSIST (Create reject record Null Data)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        data=$(cat ${NullrejFile})
        result=""
        result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Create reject record Null Data)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    else
        startTime=$(date -u -d "$start" +"%s")
        endTime=$(date -u -d "$end" +"%s")
        DIFF=$(($endTime - $startTime))
        duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
        data=$(cat ${NullrejFile})
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Create reject record Null Data)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Start: %s' "$start")$'\n'
        result+=$(printf 'End: %s' "$end")$'\n'
        result+=$(printf 'Duration: %s' "$duration")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
fi

################################################################################
#							Check null data
################################################################################

if [ "$check_null_data_column" != "" ]; then
    
    start=$(date)
    check_null_count=$(awk 'END { print NR - 1 }' ${ErrorFile})
    end=$(date)
    
    if [ ${check_null_count} -gt 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "CHECK NULL DATA FAILED" -erl "RAW TO STAGING PERSIST (Check Null Data)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Check Null Data)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Table_Name: %s' "$table_name")$'\n'
        result+=$(printf 'Check_Null_Data_Count: %s' "$check_null_count")$'\n'
        result+=$(printf 'Check_Null_Data_Column: %s' "$check_null_data_column")$'\n'
        result+=$(printf 'Statement: %s' "awk 'END { print NR - 1 }' ${ErrorFile}")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "6" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    else
        startTime=$(date -u -d "$start" +"%s")
        endTime=$(date -u -d "$end" +"%s")
        DIFF=$(($endTime - $startTime))
        duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Check Null Data)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Start: %s' "$start")$'\n'
        result+=$(printf 'End: %s' "$end")$'\n'
        result+=$(printf 'Duration: %s' "$duration")$'\n'
        result+=$(printf 'Statement: %s' "awk 'END { print NR - 1 }' ${ErrorFile}")$'\n'
        python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm -f ${ErrorFile}
    fi
fi

################################################################################
#							Check key null data
################################################################################

if [ "$check_key_null_data_column" != "" ]; then

    set_param_config_file "${optimization_parameter_config_file}" "${workflow_config_file}" "check key null data staging persist" "${KeyNullresultFile}"
    echo "select count(*) as count_key_record_null " >>$KeyNullresultFile
    echo "from ${database_name_stgpst}.${table_name} " >>$KeyNullresultFile
    echo "where ${check_key_null_data_column} " >>$KeyNullresultFile

    ################################################################################
    #							Re-kinit
    ################################################################################
    re_kinit_ticket "${ticket_cache_file}" "${kerberos_principal}" "${logPath}" "${zone}" "${main_env_config}" "${env_config}" "${run_mode}" "${business_date}" "${logName}" "${execution_id}" "${processType}" "${basePath}" "${database_name}" "${workflow_name}"

    start=$(date)
    echo "################# RAW TO STAGING PERSIST (Check Key Null Data) : `date +'%F %T'` #################" >> ${detail_log}
    beelinecli -n ${beeline_user} --showHeader=false --outputformat=dsv --delimiterForDSV="|" -f "$KeyNullresultFile" >${KeyNullputFile} 2>> ${detail_log}
    code=$?
    echo "" >> ${detail_log}
    end=$(date)

    if [ $code -ne 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "QUERY FAILED" -erl "RAW TO STAGING PERSIST (Check Key Null Data)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        data=$(cat ${KeyNullresultFile})
        result=""
        result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Check Key Null Data)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    fi

    check_key_null_count=$(cat ${KeyNullputFile})

    if [ ${check_key_null_count} -gt 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "CHECK KEY NULL DATA FAILED" -erl "RAW TO STAGING PERSIST (Check Key Null Data)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        data=$(cat ${KeyNullresultFile})
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Check Key Null Data)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Table_Name: %s' "$table_name")$'\n'
        result+=$(printf 'Check_Key_Null_Data_Count: %s' "$check_key_null_count")$'\n'
        result+=$(printf 'Check_Key_Null_Data_Column: %s' "$check_key_null_data_column")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "6" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    else
        startTime=$(date -u -d "$start" +"%s")
        endTime=$(date -u -d "$end" +"%s")
        DIFF=$(($endTime - $startTime))
        duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
        data=$(cat ${KeyNullresultFile})
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Check Key Null Data)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Start: %s' "$start")$'\n'
        result+=$(printf 'End: %s' "$end")$'\n'
        result+=$(printf 'Duration: %s' "$duration")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
fi

################################################################################
#							Create reject record duplicate data
################################################################################

if [ "${key_column}" != "" ]; then
    set_param_config_file "${optimization_parameter_config_file}" "${workflow_config_file}" "create reject record duplicate data staging persist" "${DuprejFile}"
    echo "select ${column_name_stgpst_all}" >>$DuprejFile
    echo "from (select *, count(1) over (partition by ${key_column}) as dup_count" >>$DuprejFile
    echo "from ${database_name_stgpst}.${table_name}) dup_result" >>$DuprejFile
    echo "where dup_count > 1;" >>$DuprejFile

    ################################################################################
    #							Re-kinit
    ################################################################################
    re_kinit_ticket "${ticket_cache_file}" "${kerberos_principal}" "${logPath}" "${zone}" "${main_env_config}" "${env_config}" "${run_mode}" "${business_date}" "${logName}" "${execution_id}" "${processType}" "${basePath}" "${database_name}" "${workflow_name}"

    start=$(date)
    echo "################# RAW TO STAGING PERSIST (Create reject record Duplicate Data) : `date +'%F %T'` #################" >> ${detail_log}
    beelinecli -n ${beeline_user} --showHeader=true --outputformat=dsv --delimiterForDSV="|" -f "$DuprejFile" >${ErrorFile} 2>> ${detail_log}
    code=$?
    echo "" >> ${detail_log}
    end=$(date)

    if [ $code -ne 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "QUERY FAILED" -erl "RAW TO STAGING PERSIST (Create reject record Duplicate Data)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        data=$(cat ${DuprejFile})
        result=""
        result+=$(printf 'Abort: Beeline RAW TO STAGING PERSIST (Create reject record Duplicate Data)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    else
        startTime=$(date -u -d "$start" +"%s")
        endTime=$(date -u -d "$end" +"%s")
        DIFF=$(($endTime - $startTime))
        duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
        data=$(cat ${DuprejFile})
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Create reject record Duplicate Data)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Start: %s' "$start")$'\n'
        result+=$(printf 'End: %s' "$end")$'\n'
        result+=$(printf 'Duration: %s' "$duration")$'\n'
        result+=$(printf 'Statement: %s' "$data")$'\n'
        python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
fi

################################################################################
#							Check Duplicate
################################################################################

if [ "$key_column" != "" ]; then
    
    start=$(date)
    duplicate_record_count=$(awk 'END { print NR - 1 }' ${ErrorFile})
    end=$(date)
    
    if [ ${duplicate_record_count} -gt 0 ]; then
        err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "FAILED" -erm "CHECK DUPLICATE FAILED" -erl "RAW TO STAGING PERSIST (Check Duplicate)" 2>&1)
        code=$?
        if [ $code -ne 0 ]; then
            result=""
            result+=$(printf 'Abort: Database (FAILED)')$'\n'
            result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
            result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
            result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
            result+=$(printf 'Statement: %s' "$err_message")$'\n'
            python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        fi
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Check Duplicate)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Table_Name: %s' "$real_table_name")$'\n'
        result+=$(printf 'Key_Column: %s' "$key_column")$'\n'
        result+=$(printf 'Duplicate_Record_Count: %s' "$duplicate_record_count")$'\n'
        result+=$(printf 'Statement: %s' "awk 'END { print NR - 1 }' ${ErrorFile}")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "4" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm ${ticket_cache_file}
        exit 1
    else
        startTime=$(date -u -d "$start" +"%s")
        endTime=$(date -u -d "$end" +"%s")
        DIFF=$(($endTime - $startTime))
        duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
        result=""
        result+=$(printf 'Abort: RAW TO STAGING PERSIST (Check Duplicate)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Start: %s' "$start")$'\n'
        result+=$(printf 'End: %s' "$end")$'\n'
        result+=$(printf 'Duration: %s' "$duration")$'\n'
        result+=$(printf 'Statement: %s' "awk 'END { print NR - 1 }' ${ErrorFile}")$'\n'
        python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
        rm -f ${ErrorFile}
    fi
fi

################################################################################
#							SUCCEEDED
################################################################################

if [ -z "$warning_column_value" ]; then
    err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "SUCCEEDED" 2>&1)
    code=$?
    if [ $code -ne 0 ]; then
        result=""
        result+=$(printf 'Abort: Database (FAILED)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$err_message")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
else
    err_message=$(python ${main_dapscripts_fwk_control}/write_load_control_log.py -m "${main_env_config}" -e "${env_config}" -ei $execution_id -s "SUCCEEDED" -erl "Check Warning Column" -erm "Warning message: the data has been truncated in column $warning_column_value" 2>&1)
    code=$?
    if [ $code -ne 0 ]; then
        result=""
        result+=$(printf 'Abort: Database (FAILED)')$'\n'
        result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
        result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
        result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
        result+=$(printf 'Statement: %s' "$err_message")$'\n'
        python ${logPath}/log.py -o "failed" -s "true" -ec "1" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "None" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
    fi
fi

processEnd=$(date)
startTime=$(date -u -d "$processStart" +"%s")
endTime=$(date -u -d "$processEnd" +"%s")
DIFF=$(($endTime - $startTime))
duration=$(echo "$(($DIFF / 3600)) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds")
result=""
result+=$(printf 'Abort: RAW TO STAGING PERSIST (SUCCEEDED)')$'\n'
result+=$(printf 'Execution_ID: %s' "$execution_id")$'\n'
result+=$(printf 'Workflow_Name: %s' "$workflow_name")$'\n'
result+=$(printf 'Business_Date: %s' "$business_date")$'\n'
result+=$(printf 'Start: %s' "$processStart")$'\n'
result+=$(printf 'End: %s' "$processEnd")$'\n'
result+=$(printf 'Duration: %s' "$duration")$'\n'
result+=$(printf 'Statement: %s' "SUCCEEDED")$'\n'
python ${logPath}/log.py -o "succeeded" -s "false" -ec "0" -z "${zone}" -m "${main_env_config}" -e "${env_config}" -r "${run_mode}" -b "${business_date}" -sn "${logName}" -xi "${execution_id}" -t "${processType}" -p "${basePath}" -db "${database_name}" -w "${workflow_name}" -d "${result}"
rm ${ticket_cache_file}

