#!/bin/bash

set -e

echo "========== Initializing pipeline =========="
echo "Creating tfzap.psh ..."
cat > tfzap.psh << 'EOF'
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
cat > rechan_pols.psh << 'EOF'
#!/usr/bin/env psrsh
fscrunch
pscrunch
EOF

chmod +x tfzap.psh rechan_pols.psh
echo "psrsh scripts ready."

# ===================Define===================
data_dir="/home/data/C1"
dates=("20230904" "20231009" "20231105" "20231211" "20231223" "20240111" "20240207" "20240901" "20240906" "20240908" "20240913" "20240929")

PSRname="C1"
parfile="J2338+4818.par"
fname="seq"

# dspsr 参数
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
    echo "================ $day start ================"

    # If completed, skiped
    if [ -f "${PSRname}_${day}_singlepulse.txt" ]; then
        echo "[$day] Already finished, skipping."
        return
    fi

    # ---------- dspsr ----------
    echo "[$day] Running dspsr..."
    dspsr -cont -U "${minX_MB}" -t "${thread}" -S "${start_time}" -fname "${fname}" -E "${parfile}" -F "${nchan}" -b "${nbin}" -K -nsub "${nsub}" -s -d "${npol}" -O "${PSRname}_${day}" "${data_dir}/${day}"/*.fits

    echo "[$day] dspsr done"

    # ---------- Remove RFI----------
    echo "[$day] Running tfzap..."
    ./tfzap.psh -e zft ${PSRname}_${day}_*.ar
    echo "[$day] tfzap done"

    # ---------- Check file ----------
    ar_count=$(ls ${PSRname}_${day}_*.ar 2>/dev/null | wc -l)
    zft_count=$(ls ${PSRname}_${day}_*.zft 2>/dev/null | wc -l)

    if [ "$ar_count" -ne "$zft_count" ]; then
        echo "[$day] ERROR: zft files incomplete! ar=$ar_count zft=$zft_count"
        exit 1
    fi

    echo "[$day] File check OK"

    # ---------- Remove ar files ----------
    rm -f ${PSRname}_${day}_*.ar

    # ---------- Psradd ----------
    echo "[$day] Running psradd..."
    psradd -P -o ${PSRname}_${day}_total.add ${PSRname}_${day}_*.zft

    # ---------- Single pulse ----------
    echo "[$day] Running psrtxt2..."
    psrtxt2 -J rechan_pols.psh -i 0- -b 0- \
        ${PSRname}_${day}_total.add > ${PSRname}_${day}_singlepulse.txt

    echo "================ $day FINISHED ================"
}

export -f process_one_day
export data_dir PSRname parfile fname minX_MB thread nchan npol start_time nbin nsub

# ==============parallel==============
echo "Starting parallel pipeline ..."
printf "%s\n" "${dates[@]}" | parallel -j ${parallel_days} process_one_day {}

echo "All days completed"
