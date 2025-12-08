#!/usr/bin/env bash

# ====== إعداد بسيط ======
SRC="openMP_Version.c"
BIN="omp"

if [ $# -ne 1 ]; then
    echo "Usage: $0 <stocks_directory>"
    exit 1
fi

DATA_DIR="$1"

# ====== 1) الكومبايل ======
echo "Compiling $SRC with OpenMP..."
echo "----------------------------------------"
gcc-15 -O3 -fopenmp "$SRC" -o "$BIN"
if [ $? -ne 0 ]; then
    echo "Compilation failed!"
    exit 1
fi
echo "Compilation done. Binary: ./$BIN"
echo

# ====== 2) الإعدادات للتجارب ======
THREADS=(2 4 8 16)
SCHEDULES=("static,1000" "dynamic,1000" "guided,1000")

# ====== 3) تشغيل التجارب ======
for t in "${THREADS[@]}"; do
  for sched in "${SCHEDULES[@]}"; do
    echo "=============================================="
    echo "  OMP_NUM_THREADS = $t"
    echo "  OMP_SCHEDULE    = $sched"
    echo "=============================================="
    export OMP_NUM_THREADS="$t"
    export OMP_SCHEDULE="$sched"

    # تشغيل البرنامج – المخرجات كلها في التيرمنال
    ./"$BIN" "$DATA_DIR"

    echo                                  # سطر فاصل
  done
done
# gcc -O3 -fopenmp openMP_Version.c -o omp
# chmod +x run_omp_tests.sh     
# ./run_omp_tests.sh stocks     
