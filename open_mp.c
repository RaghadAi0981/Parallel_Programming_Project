#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <omp.h>

#define MAX_LINE_LEN 256
#define MAX_DAYS     1000000 

typedef struct {
    char date[20];
    double open, high, low, close, volume;
} StockData;


double daily_average(StockData s) {
    return (s.open + s.high + s.low + s.close) / 4.0;
}


//Also SERIAL There's no loop inside to parallelize.

double daily_return(double prev_close, double curr_close) {
    if (prev_close == 0.0) return 0.0;
    return (curr_close - prev_close) / prev_close;
}


//   SERIAL by design:
 
int read_csv(const char *filename, StockData *data, int max_days) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        return 0;
    }

    char line[MAX_LINE_LEN];
    int count = 0;

    fgets(line, sizeof(line), file);

    while (fgets(line, sizeof(line), file) && count < max_days) {
        if (sscanf(line, "%19[^,],%lf,%lf,%lf,%lf,%lf",
                   data[count].date,
                   &data[count].open,
                   &data[count].high,
                   &data[count].low,
                   &data[count].close,
                   &data[count].volume) == 6) {
            count++;
        }
    }

    fclose(file);
    return count;
}

int main(int argc, char *argv[]) {
    int max_days = MAX_DAYS;
    int file_start_index = 1;

    if (argc >= 2) {
        char *endptr;
        long tmp = strtol(argv[1], &endptr, 10);
        if (*endptr == '\0' && tmp > 0) {
            max_days = (int)tmp;
            file_start_index = 2;
        }
    }

    if (file_start_index >= argc) {
        printf("Usage: %s [max_days] <file1.csv> <file2.csv> ... <fileN.csv>\n", argv[0]);
        return 1;
    }

    int num_files = argc - file_start_index;

    // OpenMP-specific: number of threads available on the system.
    int threads   = omp_get_max_threads();

    
    // reductionOpenMP:
    //   - give each thread a private copy of these variables
    //   - efficiently merge them at the end of the parallel loop
    //   - avoid explicit locks and minimize synchronization overhead.
    long long total_records   = 0;
    long long total_returns   = 0;
    double    sum_prices      = 0.0;
    double    sum_returns     = 0.0;
    double    sum_returns_sq  = 0.0;

    double start_time = omp_get_wtime();   // OpenMP wall-clock timer

   
    #pragma omp parallel for reduction(+:total_records, total_returns, sum_prices, sum_returns, sum_returns_sq) schedule(dynamic)
    for (int f = file_start_index; f < argc; f++) {
        const char *filename = argv[f];

       
        StockData *data = malloc(sizeof(StockData) * max_days);
        if (!data) {
            continue;
        }

    
        int n = read_csv(filename, data, max_days);
        if (n > 1) {
            long long local_records   = n;
            long long local_returns   = n - 1;
            double    local_price_sum = 0.0;
            double    local_ret_sum   = 0.0;
            double    local_ret_sq    = 0.0;

         
            for (int i = 0; i < n; i++) {
                local_price_sum += daily_average(data[i]);
            }

            for (int i = 0; i < n - 1; i++) {
                double r = daily_return(data[i].close, data[i + 1].close);
                local_ret_sum  += r;
                local_ret_sq   += r * r;
            }

       
            total_records   += local_records;
            total_returns   += local_returns;
            sum_prices      += local_price_sum;
            sum_returns     += local_ret_sum;
            sum_returns_sq  += local_ret_sq;
        }

        free(data); // Per-thread memory cleanup
    }

    double end_time = omp_get_wtime();
    double elapsed  = end_time - start_time;


    if (total_records == 0 || total_returns == 0) {
        printf("No sufficient data loaded.\n");
        return 1;
    }

    double avg_price = sum_prices / (double)total_records;
    double mean_ret  = sum_returns / (double)total_returns;
    double mean_sq   = sum_returns_sq / (double)total_returns;

    double variance  = mean_sq - (mean_ret * mean_ret);
    if (variance < 0.0) variance = 0.0;
    double volatility = sqrt(variance);

    printf("OpenMP Stock Analysis (All Files Combined)\n");
    printf("===========================================\n\n");
    printf("Files: %d\n", num_files);
    printf("Max days per file: %d\n", max_days);
    printf("Threads: %d\n\n", threads);

    printf("Total records loaded: %lld\n", total_records);
    printf("Average daily price (all files): %.4f\n", avg_price);
    printf("Volatility (std. dev of returns): %.6f\n", volatility);
    printf("Volatility (percentage): %.4f%%\n", volatility * 100.0);
    printf("Execution time (OpenMP): %.6f seconds\n", elapsed);
    printf("===========================================\n");

    return 0;
}

// gcc -O3 -fopenmp open_mp.c -o omp
// export OMP_NUM_THREADS=2
// export OMP_NUM_THREADS=4
// export OMP_NUM_THREADS=8
// export OMP_NUM_THREADS=16
// export OMP_SCHEDULE="static,1000"
// export OMP_SCHEDULE="dynamic,1000"
// export OMP_SCHEDULE="guided,1000"   
// ./omp stock_data.csv
