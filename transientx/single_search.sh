#!/bin/bash
# @Yujie Chen, gs.yujiechen23@gzu.edu.cn, 2025/12/15
# transientx + replot_fil with parallel

# Data path
data_dir="/home/data4/PT2025_0005/NGC4147"
dates=("20241222" "20250220")
fits_pattern="*.fits"

# transientx parameters
fill = "mean"
thread="4"
zapthre="3.0"
dms="10"
ddm="0.5"
ndm="100"
thre="7"
minw="0.0005"
maxw="0.5"
length_time="0.5"
Z_RFI="kadaneF 8 4 zdot"

# replot parameters
srcname="NGC4147"
RA="12:10:06.15"
DEC="+18:32:31.8"
telescope="FAST"
dmcutoff="0.01"
widthcutoff="0.01"
snrcutoff="7"
snrloss="0.1"
kadane="8 4 7"
template="/home/software/PulsarX/include/template/fast_fold.template"

# output command files
transientx_commands="transientx.txt"
replot_commands="replot.txt"

> "$transientx_commands"
> "$replot_commands"

> transientx.log
> replot.log

##############################
# Step 1: transientx_fil
##############################

echo "Generating transientx_fil commands..."

for day in "${dates[@]}"; do

    mkdir -p "${day}"
    chmod 777 "${day}"

    echo transientx_fil -v -o "${day}/${srcname}" -t "${thread}" --fill "${fill}" --zapthre "${zapthre}" --dms "${dms}" --ddm "${ddm}" --ndm "${ndm}" --thre "${thre}" --minw "${minw}" --maxw "${maxw}" -l "${length_time}" --drop -z "${Z_RFI}" --psrfits "${data_dir}/${day}/${fits_pattern}" >> "$transientx_commands"
done

echo "Running transientx_fil..."
cat "$transientx_commands" | parallel -j "${thread}" | tee -a transientx.log

wait
echo "transientx_fil finished."

##############################
# Step 2: replot_fil
##############################

echo "Generating replot_fil commands..."

for day in "${dates[@]}"; do
    echo replot_fil -t "${thread}" --zapthre "${zapthre}" --srcname "${srcname}" --ra "${RA}" --dec "${DEC}" --telescope "${telescope}" --dmcutoff "${dmcutoff}" --widthcutoff "${widthcutoff}" --snrcutoff "${snrcutoff}" --snrloss "${snrloss}" --zap --zdot --kadane ${kadane} --candfile "${day}/*.cands" --template "${template}" --arch --clean --psrfits "${data_dir}/${day}/${fits_pattern}" >> "$replot_commands"
done

echo "Running replot_fil..."
cat "$replot_commands" | parallel -j "${thread}" | tee -a replot.log

wait
echo "replot_fil finished."

echo "All commands completed."

