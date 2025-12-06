#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>

#define MAX_DAYS 1000000   // الحد الأقصى للسجلات اللي نسمح بتحميلها

// هيكل بسيط يمثل بيانات كل يوم بالسهم
typedef struct {
    double open, high, low, close;
} StockData;

// دالة حساب متوسط السعر لليوم الواحد
// (فكرة بسيطة: نجمع الأسعار الأربعة ونقسمها على 4)
double daily_average(StockData s) {
    return (s.open + s.high + s.low + s.close) / 4.0;
}

// دالة لحساب العائد اليومي بين يومين
// إذا كان الإغلاق السابق صفر، نرجّع صفر لتفادي القسمة على صفر
double daily_return(double prev_close, double curr_close) {
    if (prev_close == 0) return 0.0;
    return (curr_close - prev_close) / prev_close;
}

int main(int argc, char *argv[]) {

    int rank, size;

    MPI_Init(&argc, &argv);                         // تشغيل MPI
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);           // رقم العملية الحالية
    MPI_Comm_size(MPI_COMM_WORLD, &size);           // عدد العمليات كلها

    double start_time, end_time;
    int n;                                          // عدد السجلات الكلي
    StockData *data = NULL;

    // عملية الرانك 0 هي اللي تقرأ الداتا من الملف
    if (rank == 0) {

        FILE *file = fopen("stock_data.csv", "r");
        if (!file) {
            printf("Error: cannot open file\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        char line[256];
        fgets(line, sizeof(line), file);            // نتجاهل الهيدر

        data = malloc(sizeof(StockData) * MAX_DAYS);
        n = 0;

        // قراءة الملف سطر سطر
        // نقرأ فقط الأعمدة المهمة: open, high, low, close
        while (fgets(line, sizeof(line), file) && n < MAX_DAYS) {
            sscanf(line, "%*[^,],%lf,%lf,%lf,%lf,%*lf",
                   &data[n].open, &data[n].high, &data[n].low, &data[n].close);
            n++;
        }

        fclose(file);
        printf("Total records read: %d\n", n);
    }

    // نرسل عدد السجلات لكل العمليات
    MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);

    // كل عملية تأخذ جزء من البيانات (chunk)
    int chunk = n / size;
    StockData *local_data = malloc(sizeof(StockData) * chunk);

    // توزيع البيانات بين العمليات
    MPI_Scatter(data, chunk * sizeof(StockData), MPI_BYTE,
                local_data, chunk * sizeof(StockData), MPI_BYTE,
                0, MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);
    start_time = MPI_Wtime();       // بدء حساب الوقت

    // -------- الحسابات المحلية لكل عملية --------

    double local_sum_avg = 0.0;
    double *local_returns = malloc(sizeof(double) * (chunk - 1));

    // حساب المتوسطات والعوائد
    for (int i = 0; i < chunk; i++) {
        local_sum_avg += daily_average(local_data[i]);

        if (i > 0)
            local_returns[i - 1] = daily_return(local_data[i - 1].close,
                                               local_data[i].close);
    }

    // حساب الفولاتيليتي محلياً
    double sum_ret = 0.0;
    for (int i = 0; i < chunk - 1; i++)
        sum_ret += local_returns[i];

    double mean_ret = sum_ret / (chunk - 1);

    double var = 0.0;
    for (int i = 0; i < chunk - 1; i++)
        var += pow(local_returns[i] - mean_ret, 2);

    double local_vol = sqrt(var / (chunk - 1));

    // -------- جمع نتايج كل العمليات --------

    double global_sum_avg = 0.0;
    MPI_Reduce(&local_sum_avg, &global_sum_avg, 1,
               MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    double global_vol = 0.0;
    MPI_Reduce(&local_vol, &global_vol, 1,
               MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);
    end_time = MPI_Wtime();      // نهاية الوقت

    // -------- طباعة النتايج --------
    if (rank == 0) {

        double avg_price = global_sum_avg / n;      // متوسط الأسعار
        double avg_vol = global_vol / size;         // متوسط الفولاتيليتي

        printf("\n===== MPI Stock Analysis Results =====\n");
        printf("Total Processes: %d\n", size);
        printf("Average Daily Price: %.4f\n", avg_price);
        printf("Average Volatility: %.6f\n", avg_vol);
        printf("Execution Time: %.6f seconds\n", end_time - start_time);
        printf("=====================================\n");
    }

    // تنظيف الذاكرة
    free(local_data);
    free(local_returns);
    if (rank == 0) free(data);

    MPI_Finalize();
    return 0;
}
