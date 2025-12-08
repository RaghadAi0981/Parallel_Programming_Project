#!/bin/bash

# -----------------------------------------------------------
# Usage:
#   ./run_mpi.sh <num_processes> <data_path> [max_rows] [label]
#
# Examples:
#   ./run_mpi.sh 8 "C:/.../stocks"
#   ./run_mpi.sh 8 "C:/.../stocks" 20000
#   ./run_mpi.sh 8 "C:/.../stocks" 20000 small
# -----------------------------------------------------------

NP="$1"          # عدد البروسيس
DATASET="$2"     # مسار الداتا (مجلد stocks)
MAXROWS="$3"     # عدد الصفوف (اختياري)
LABEL="$4"       # اسم للتجربة (اختياري)
RUNS=10

PROGRAM="./mpi_stock_analysis.exe"
MPIEXEC="/c/Program Files/Microsoft MPI/Bin/mpiexec.exe"

# ==== التحقق من المُدخلات ====
if [ -z "$NP" ] || [ -z "$DATASET" ]; then
    echo "Usage: $0 <num_processes> <data_path> [max_rows] [label]"
    exit 1
fi

# ==== اسم ملف الإخراج ====
timestamp=$(date +"%Y-%m-%d_%H-%M-%S")

if [ -n "$LABEL" ]; then
    OUTFILE="mpi_results_${LABEL}_${NP}procs_${timestamp}.txt"
elif [ -n "$MAXROWS" ]; then
    OUTFILE="mpi_results_${MAXROWS}rows_${NP}procs_${timestamp}.txt"
else
    OUTFILE="mpi_results_full_${NP}procs_${timestamp}.txt"
fi

echo "Saving output to: $OUTFILE"

echo "=======================================" | tee "$OUTFILE"
echo "Running MPI Program $RUNS times"       | tee -a "$OUTFILE"
echo "MPI Processes : $NP"                   | tee -a "$OUTFILE"
if [ -n "$MAXROWS" ]; then
    echo "Max rows     : $MAXROWS"           | tee -a "$OUTFILE"
else
    echo "Max rows     : FULL DATASET"      | tee -a "$OUTFILE"
fi

if [ -n "$LABEL" ]; then
    echo "Label        : $LABEL"             | tee -a "$OUTFILE"
fi

echo "=======================================" | tee -a "$OUTFILE"

# ==== مجاميع المتوسطات ====
sum_total=0.0
sum_comm=0.0
sum_comp=0.0

# ==== تشغيل 10 مرات ====
for ((i=1; i<=RUNS; i++)); do
    echo "---------- Run #$i ----------" | tee -a "$OUTFILE"

    # تشغيل البرنامج مع أو بدون max_rows
    if [ -n "$MAXROWS" ]; then
        output=$("$MPIEXEC" -np "$NP" "$PROGRAM" "$DATASET" "$MAXROWS")
    else
        output=$("$MPIEXEC" -np "$NP" "$PROGRAM" "$DATASET")
    fi

    # طباعة النتائج لكل Run
    echo "$output" | tee -a "$OUTFILE"

    # استخراج الأرقام بطريقة صحيحة (أول رقم عشري في السطر)
    total=$(echo "$output" | grep "Total execution time" | grep -oE '[0-9]+\.[0-9]+' | head -n1)
    comm=$(echo "$output" | grep "Communication time"   | grep -oE '[0-9]+\.[0-9]+' | head -n1)
    comp=$(echo "$output" | grep "Computation time"     | grep -oE '[0-9]+\.[0-9]+' | head -n1)

    if [ -n "$total" ] && [ -n "$comm" ] && [ -n "$comp" ]; then
        sum_total=$(printf "%s %s\n" "$sum_total" "$total" | awk '{printf "%.6f", $1+$2}')
        sum_comm=$(printf "%s %s\n" "$sum_comm" "$comm"   | awk '{printf "%.6f", $1+$2}')
        sum_comp=$(printf "%s %s\n" "$sum_comp" "$comp"   | awk '{printf "%.6f", $1+$2}')
    else
        echo "WARNING: Failed to parse timing for run #$i" | tee -a "$OUTFILE"
    fi

    echo "" | tee -a "$OUTFILE"
done

# ==== حساب المتوسطات ====
avg_total=$(printf "%s %s\n" "$sum_total" "$RUNS" | awk '{printf "%.6f", $1/$2}')
avg_comm=$(printf "%s %s\n" "$sum_comm" "$RUNS" | awk '{printf "%.6f", $1/$2}')
avg_comp=$(printf "%s %s\n" "$sum_comp" "$RUNS" | awk '{printf "%.6f", $1/$2}')

echo "=======================================" | tee -a "$OUTFILE"
echo "Averages over $RUNS runs:"             | tee -a "$OUTFILE"
echo "  Avg Total execution time : $avg_total seconds" | tee -a "$OUTFILE"
echo "  Avg Communication time   : $avg_comm seconds"  | tee -a "$OUTFILE"
echo "  Avg Computation time     : $avg_comp seconds"  | tee -a "$OUTFILE"
echo "=======================================" | tee -a "$OUTFILE"

# ==== حفظ ملخص النتائج في CSV (تراكمي) ====
SUMMARY_CSV="mpi_summary.csv"

if [ ! -f "$SUMMARY_CSV" ]; then
    echo "timestamp,label,num_procs,max_rows,avg_total,avg_comm,avg_comp" > "$SUMMARY_CSV"
fi

if [ -n "$LABEL" ]; then
    label_val="$LABEL"
else
    if [ -n "$MAXROWS" ]; then
        label_val="${MAXROWS}rows"
    else
        label_val="full_dataset"
    fi
fi

if [ -n "$MAXROWS" ]; then
    maxrows_val="$MAXROWS"
else
    maxrows_val="ALL"
fi

echo "${timestamp},${label_val},${NP},${maxrows_val},${avg_total},${avg_comm},${avg_comp}" >> "$SUMMARY_CSV"

echo "Finished! Results saved to: $OUTFILE"
