#!/bin/bash
#@Yujie Chen, gs.yujiechen23@gzu.edu.cn, 2026/2/4

set -e

echo "========== Initializing pipeline =========="
script_dir=$(pwd)
echo "Creating tfzap.psh ..."
cat > ${script_dir}/tfzap.psh << 'EOF'
#!/usr/bin/env psrsh
state Stokes
zap tfzap pols=1
zap tfzap stat=range
zap tfzap smooth=0
zap tfzap mask=iqr
zap tfzap
fscrunch 128
EOF

echo "Creating rechan_pols.psh ..."
cat > ${script_dir}/rechan_pols.psh << 'EOF'
#!/usr/bin/env psrsh
fscrunch
pscrunch
EOF

chmod +x ${script_dir}/tfzap.psh ${script_dir}/rechan_pols.psh
echo "psrsh scripts ready."

# ===================Define===================
data_dir="/home/data/C1"
dates=("20230904" "20231029")

PSRname="C1"
parfile="${script_dir}/J2338+4818.par"  
fname="seq"
minX_MB="1024"
thread="4"
nchan="1024:D"
npol="4"
start_time="60"
nbin="1024"
nsub="300"
parallel_days=6

process_one_day () {

    day=$1
    workdir="${data_dir}/${day}"

    echo "================ $day start ================"

    cd "${workdir}" || { echo "Cannot enter ${workdir}"; exit 1; }

    # If completed, skip
    if [ -f "${PSRname}_${day}_total.add" ]; then
        echo "[$day] Already finished, skipping."
        return
    fi

    # ---------- dspsr ----------
    echo "[$day] Running dspsr..."
    dspsr -cont -U "${minX_MB}" -t "${thread}" -S "${start_time}" \
          -fname "${fname}" -E "${parfile}" -F "${nchan}" \
          -b "${nbin}" -K -nsub "${nsub}" -s -d "${npol}" \
          -O "${PSRname}_${day}" *.fits

    echo "[$day] dspsr done"

    # ---------- tfzap ----------
    echo "[$day] Running tfzap..."
    ${script_dir}/tfzap.psh -e zft ${PSRname}_${day}_*.ar
    echo "[$day] tfzap done"

    # ---------- Check files ----------
    ar_count=$(ls ${PSRname}_${day}_*.ar 2>/dev/null | wc -l)
    zft_count=$(ls ${PSRname}_${day}_*.zft 2>/dev/null | wc -l)

    if [ "$ar_count" -eq 0 ] || [ "$ar_count" -ne "$zft_count" ]; then
        echo "[$day] ERROR: zft incomplete! ar=$ar_count zft=$zft_count"
        exit 1
    fi

    echo "[$day] File check OK"

    rm -f ${PSRname}_${day}_*.ar

    # ---------- psradd ----------
    echo "[$day] Running psradd..."
    psradd -P -o ${PSRname}_${day}_total.add ${PSRname}_${day}_*.zft

    # ---------- Single pulse ----------
    echo "[$day] Running psrtxt2..."
    psrtxt2 -J ${script_dir}/rechan_pols.psh -i 0- -b 0- \
        ${PSRname}_${day}_total.add > ${PSRname}_${day}_singlepulse.txt

    echo "================ $day FINISHED ================"
}

export -f process_one_day
export data_dir PSRname parfile fname minX_MB thread nchan npol start_time nbin nsub script_dir

echo "Starting parallel pipeline ..."
printf "%s\n" "${dates[@]}" | parallel -j ${parallel_days} process_one_day {}

echo "All days completed"
