
#!/bin/bash

# -----------------------------------------------------------
# Usage:
#   ./run_Serial.sh <data_path> [label]
#
# Examples:
#   ./run_Serial.sh "C:/.../stocks"
#   ./run_Serial.sh "C:/.../stocks" rows_50pct
# -----------------------------------------------------------

DATASET="C:\Users\Abrar Matar\OneDrive\سطح المكتب\المواد\حوسبة عملي\Project\code\stocks"    # مسار مجلد الأسهم (stocks)
LABEL="Serial"      # اسم للتجربة (اختياري)
RUNS=10         # عدد مرات التشغيل

PROGRAM="./Serial_Version.exe"
# ==== التحقق من المُدخلات ====
if [ -z "$DATASET" ]; then
    echo "Usage: $0 <C:\Users\Abrar Matar\OneDrive\سطح المكتب\المواد\حوسبة عملي\Project\code\stocks> [label]"
    exit 1
fi

# ==== اسم ملف الإخراج ====
timestamp=$(date +"%Y-%m-%d_%H-%M-%S")

if [ -n "$LABEL" ]; then
    OUTFILE="serial_results_${LABEL}_${timestamp}.txt"
else
    OUTFILE="serial_results_${timestamp}.txt"
fi

echo "Saving output to: $OUTFILE"

echo "=======================================" | tee "$OUTFILE"
echo "Running SERIAL Program $RUNS times"    | tee -a "$OUTFILE"
if [ -n "$LABEL" ]; then
    echo "Label        : $LABEL"             | tee -a "$OUTFILE"
fi
echo "=======================================" | tee -a "$OUTFILE"

# ==== مجاميع المتوسطات ====
sum_total=0.0

# ==== تشغيل 10 مرات ====
for ((i=1; i<=RUNS; i++)); do
    echo "---------- Run #$i ----------" | tee -a "$OUTFILE"

    # تشغيل البرنامج السيريال
    output=$("$PROGRAM" "$DATASET")

    # طباعة النتائج لكل Run
    echo "$output" | tee -a "$OUTFILE"

    # استخراج رقم وقت التنفيذ من السطر:
    #  Execution time (serial): 9.589000 seconds
    total=$(echo "$output" | grep "Execution time (serial)" | grep -oE '[0-9]+\.[0-9]+' | head -n1)

    if [ -n "$total" ]; then
        sum_total=$(printf "%s %s\n" "$sum_total" "$total" | awk '{printf "%.6f", $1+$2}')
    else
        echo "WARNING: Failed to parse timing for run #$i" | tee -a "$OUTFILE"
    fi

    echo "" | tee -a "$OUTFILE"
done

# ==== حساب المتوسط ====
avg_total=$(printf "%s %s\n" "$sum_total" "$RUNS" | awk '{printf "%.6f", $1/$2}')

echo "======================================="         | tee -a "$OUTFILE"
echo "Average over $RUNS runs:"                       | tee -a "$OUTFILE"
echo "  Avg Execution time (serial): $avg_total sec"  | tee -a "$OUTFILE"
echo "======================================="         | tee -a "$OUTFILE"

# ==== حفظ ملخص النتائج في CSV (تراكمي) ====
SUMMARY_CSV="serial_summary.csv"

if [ ! -f "$SUMMARY_CSV" ]; then
    echo "timestamp,label,dataset,avg_time,runs" > "$SUMMARY_CSV"
fi

if [ -n "$LABEL" ]; then
    label_val="$LABEL"
else
    label_val="default"
fi

echo "${timestamp},${label_val},\"${DATASET}\",${avg_total},${RUNS}" >> "$SUMMARY_CSV"

echo "Finished! Results saved to: $OUTFILE"
