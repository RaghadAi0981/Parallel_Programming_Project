/***************************************************************
 * File: stock_analysis_omp.c
 * Project: Stock Market Analysis (OpenMP Version)
 *
 * Description:
 * This program analyzes stock price data by computing:
 *    - Daily average price
 *    - Daily returns (percentage change)
 *    - Price volatility (standard deviation of returns)
 *
 * WHY we use OpenMP in this file:
 * --------------------------------
 * not important for presentation, but keep it in mind for understanding
 * 1) The computations over days (loops) are independent between iterations.
 *    That means each day can be processed by a different CPU core.
 * 2) OpenMP allows us to run these loops in parallel using multiple threads,
 *    which can make the program much faster on multi-core CPUs.
 * 3) We use "reduction" in some places to safely combine partial results
 *    from each thread into one final value (e.g., sums for averages/variance)
 *    without race conditions.
 *
 * Input:
 *    One or more CSV files, each containing:
 *      Date, Open, High, Low, Close, Volume
 *    Optionally, a maximum number of days [max_days] can be provided.
 ***************************************************************/

#include <stdio.h>
#include <stdlib.h> // for malloc, free
#include <string.h>
#include <math.h>
#include <omp.h>    // OpenMP header (we use it for parallel loops and timing)

#define MAX_LINE_LEN 256  // max line length
#define MAX_DAYS     1000 // default max number of days to read

typedef struct {
    char date[20]; 
    double open, high, low, close, volume; // Price and volume columns
} StockData;

// daily_average : summary for daily price (simple helper, stays serial) important
double daily_average(StockData s) {
    return (s.open + s.high + s.low + s.close) / 4.0;
}

// daily_return: explains the change between the previous day's close
// and the current day's close (important for stock movement analysis)
double daily_return(double prev_close, double curr_close) {
    if (prev_close == 0) return 0.0;
    return (curr_close - prev_close) / prev_close;
}

// compute_volatility : calculate the standard deviation of the daily returns (volatility).
// In this function we use OpenMP "reduction" to parallelize the loops that compute
// the sum and variance. We do this because each iteration of the loop is independent,
// but all iterations contribute to one shared value (sum or variance).

double compute_volatility(double *returns, int n) {
    if (n <= 1) return 0.0;

    double sum = 0.0;

    // First pass: compute the sum of returns to get the mean.
    //
    // WHY OpenMP here?
    // - Each iteration only reads returns[i] and updates 'sum'.
    // - There is no dependency between iterations.
    // - However, multiple threads writing to 'sum' at the same time would cause
    //   a race condition.
    //
    // WHY reduction(+:sum)?
    //what did it caclulate exactly the reduction(+:sum) does is:
    // - It tells OpenMP to create a private copy of 'sum' for each thread.
    // - reduction(+:sum) tells OpenMP to:
    //     * give each thread a private copy of 'sum'
    //     * add all private sums together at the end
    //   This is a safe parallel pattern for summation.
    #pragma omp parallel for reduction(+:sum)
    for (int i = 0; i < n; i++) {
        sum += returns[i];
    }

    double mean = sum / n;
    double variance = 0.0;

    // Second pass: compute the variance (sum of squared differences from the mean).
    //
    // Again, each iteration is independent and contributes to the single variable
    // 'variance', so we use another reduction.
    #pragma omp parallel for reduction(+:variance)
    for (int i = 0; i < n; i++) {
        double diff = returns[i] - mean;
        variance += diff * diff;
    }

    return sqrt(variance / n);
}

// to read the file (this part stays serial)
// File I/O is usually not the bottleneck and is more complicated to parallelize safely.
int read_csv(const char *filename, StockData *data, int max_days) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        return 0;
    }

    char line[MAX_LINE_LEN];
    int count = 0;

    // skips the title
    fgets(line, sizeof(line), file);

    while (fgets(line, sizeof(line), file) && count < max_days) {
        // Format: Date,Open,High,Low,Close,Volume
        sscanf(line, "%[^,],%lf,%lf,%lf,%lf,%lf",
               data[count].date, &data[count].open, &data[count].high,
               &data[count].low, &data[count].close, &data[count].volume);
        count++;
    }

    fclose(file);
    return count;
}

int main(int argc, char *argv[]) {

    if (argc < 2) {
        printf("Usage: %s [max_days] <file1.csv> <file2.csv> ... <fileN.csv>\n", argv[0]);
        return 1;
    }

    int max_days = MAX_DAYS;
    int file_start_index = 1;

    // Optional first argument: max_days
    if (argc >= 3) {
        char *endptr;
        long tmp = strtol(argv[1], &endptr, 10);
        if (*endptr == '\0' && tmp > 0) {
            max_days = (int)tmp;
            file_start_index = 2;
        }
    }

    if (argc <= file_start_index) {
        printf("Usage: %s [max_days] <file1.csv> <file2.csv> ... <fileN.csv>\n", argv[0]);
        return 1;
    }

    printf("\nAnalysis result of stock files (OpenMP version)\n");
    printf("===========================================\n\n");

    for (int f = file_start_index; f < argc; f++) {
        const char *filename = argv[f];

        StockData *data = malloc(sizeof(StockData) * max_days);
        double *returns = malloc(sizeof(double) * max_days);
        if (!data || !returns) {
            printf("Memory allocation failed for file: %s\n", filename);
            free(data);
            free(returns);
            continue;
        }

        int n = read_csv(filename, data, max_days);
        if (n <= 1) {
            printf("Not enough data in file: %s\n", filename);
            free(data);
            free(returns);
            continue;
        }

        // symbol name
        char symbol[32];
        strcpy(symbol, filename);
        char *dot = strchr(symbol, '.');
        if (dot) *dot = '\0';

        double start_time = omp_get_wtime();

        double avg_sum = 0.0; // to calculate the cumulative daily average

       // MAIN parallel loop over days:
        // -----------------------------
        // WHY THIS LOOP IS PARALLELIZED:
        // - Each iteration works on a different day (i = 0,1,2,...)
        // - Daily averages and returns do NOT depend on each other
        // - Therefore, each iteration can safely run on a different core

        // WHY WE USE reduction(+:avg_sum) SPECIFICALLY:
        // ---------------------------------------------
        // We need to accumulate the daily average:
        //        avg_sum += daily_average(...)
        // If multiple threads update avg_sum at the same time, we get a
        // race condition (incorrect results).
        //
        // We have several possible ways to fix this, BUT:
        //
        // ❌ 1. Using 'critical' section:
        //      #pragma omp critical
        //         avg_sum += value;
        //    - Correct but VERY SLOW
        //    - All threads must wait in line, destroying parallel speed
        //
        // ❌ 2. Using 'atomic':
        //      #pragma omp atomic
        //         avg_sum += value;
        //    - Works, but still slower because each update must be protected
        //
        // ❌ 3. Using a shared variable without protection:
        //    - Produces WRONG results (race condition)
        //
        // ❌ 4. Using manual locks:
        //    - Correct, but extremely slow and unnecessary
        //
        // ✅ BEST CHOICE → reduction(+:avg_sum)
        // -------------------------------------
        // reduction does THREE things automatically:
        //
        // (1) Gives EACH thread its OWN private copy of avg_sum
        // (2) Threads update their private copies in parallel (NO waiting)
        // (3) At the end, OpenMP COMBINES all private sums into the final avg_sum
        //
        // THIS IS THE FASTEST METHOD.
        // - No locking
        // - No blocking
        // - No race conditions
        // - Perfect for summing independent values
        //
        // That is why REDUCTION is the correct and optimal choice here.
        //
        // --------
        #pragma omp parallel for reduction(+:avg_sum)
        for (int i = 0; i < n; i++) {
            avg_sum += daily_average(data[i]);

            // We don't need reduction for 'returns' because each thread
            // writes to a different index (i-1). There is no conflict.
            if (i > 0) {
                returns[i - 1] = daily_return(data[i - 1].close, data[i].close);
            }
        }

        double overall_avg = avg_sum / n;

        // We have n-1 valid returns (day1 vs day0 up to day(n-1) vs day(n-2))
        double volatility = compute_volatility(returns, n - 1);

        double end_time = omp_get_wtime();
        double elapsed = end_time - start_time;

        printf("Symbol: %s\n", symbol);
        printf("Records loaded: %d\n", n);
        printf("Average daily price (USD): %.4f\n", overall_avg);
        printf("Volatility (std. dev of returns): %.6f\n", volatility);
        printf("Volatility (percentage): %.4f%%\n", volatility * 100);
        printf("Execution time (computation only): %.6f seconds\n", elapsed);
        printf("---------------------------------------------\n");

        free(data);
        free(returns);
    }

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
