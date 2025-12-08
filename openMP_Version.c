#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <omp.h>
#include <dirent.h>

#define MAX_LINE_LEN 256

// Logical price bounds used to clean unrealistic stock prices
#define MIN_PRICE 0.01
#define MAX_PRICE 10000.0

// Allowed year range for safety (maps years to decade indices)
#define MIN_YEAR_GLOBAL 1900
#define MAX_YEAR_GLOBAL 2100
#define MAX_DECADES (((MAX_YEAR_GLOBAL - MIN_YEAR_GLOBAL) / 10) + 1)

// Structure representing **one daily record** of stock data
// date, OHLC (open, high, low, close), and volume
typedef struct {
    char date[20];
    double open, high, low, close, volume;
} StockData;

// Safe strdup implementation (for portability)
// Some compilers or environments may not provide strdup by default.
// This function allocates a new string and copies the content of `s`.
static char *my_strdup(const char *s) {
    size_t len = strlen(s) + 1;
    char *p = (char *)malloc(len);
    if (p) memcpy(p, s, len);
    return p;
}


int read_csv(const char *filename, StockData **data_out) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        fprintf(stderr, "Cannot open file: %s\n", filename);
        *data_out = NULL;
        return 0;
    }

    char line[MAX_LINE_LEN];
    int count = 0, capacity = 0;
    StockData *data = NULL;

    // Skip header line
    fgets(line, sizeof(line), file);

    // Read each subsequent line and parse fields
    while (fgets(line, sizeof(line), file)) {
        // Grow array if needed
        if (count >= capacity) {
            int new_cap = (capacity == 0) ? 1024 : capacity * 2;
            StockData *tmp = realloc(data, new_cap * sizeof(StockData));
            if (!tmp) {
                fprintf(stderr, "Memory allocation failed in read_csv\n");
                free(data);
                fclose(file);
                *data_out = NULL;
                return 0;
            }
            data = tmp;
            capacity = new_cap;
        }

        double adj_temp;
        int parsed = sscanf(
            line,
            "%19[^,],%lf,%lf,%lf,%lf,%lf,%lf",
            data[count].date,
            &data[count].open,
            &data[count].high,
            &data[count].low,
            &data[count].close,
            &adj_temp,
            &data[count].volume
        );

        // Only accept fully parsed lines
        if (parsed == 7) {
            count++;
        }
    }

    fclose(file);
    *data_out = data;
    return count;
}


int main(int argc, char *argv[]) {

    if (argc != 2) {
        printf("Usage: %s <stocks_directory>\n", argv[0]);
        return 1;
    }

    const char *dirpath = argv[1];

    // Open the directory containing CSV files
    DIR *dir = opendir(dirpath);
    if (!dir) {
        fprintf(stderr, "Cannot open directory: %s\n", dirpath);
        return 1;
    }

    struct dirent *entry;
    char filepath[1024];

    // Dynamic list of file paths
    char **file_list = NULL;
    int file_count = 0;
    int file_cap   = 0;

  
    while ((entry = readdir(dir)) != NULL) {
        const char *name = entry->d_name;

        // Skip "." and ".."
        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
            continue;

        // Only keep files ending with ".csv"
        size_t len = strlen(name);
        if (len < 4 || strcmp(name + len - 4, ".csv") != 0)
            continue;

        // Build full path: dirpath/filename
        snprintf(filepath, sizeof(filepath), "%s/%s", dirpath, name);

        // Grow file_list if needed
        if (file_count >= file_cap) {
            int new_cap = (file_cap == 0) ? 128 : file_cap * 2;
            char **tmp = (char **)realloc(file_list, new_cap * sizeof(char *));
            if (!tmp) {
                fprintf(stderr, "Memory allocation failed for file list\n");
                closedir(dir);
                for (int i = 0; i < file_count; i++) free(file_list[i]);
                free(file_list);
                return 1;
            }
            file_list = tmp;
            file_cap  = new_cap;
        }

        // Store a copy of the full path
        file_list[file_count] = my_strdup(filepath);
        if (!file_list[file_count]) {
            fprintf(stderr, "Memory allocation failed for filepath\n");
            closedir(dir);
            for (int i = 0; i < file_count; i++) free(file_list[i]);
            free(file_list);
            return 1;
        }
        file_count++;
    }

    closedir(dir);

    if (file_count == 0) {
        printf("No CSV files found in directory: %s\n", dirpath);
        return 0;
    }

    printf("\nOpenMP Stock Analysis - Market Metrics by Decade (Cleaned)\n");
    printf("Directory: %s\n", dirpath);
    printf("Files found: %d\n", file_count);
    printf("============================================================\n\n");

    // Global accumulators per decade (shared across all threads)
    double sum_avg_decade[MAX_DECADES]    = {0.0};
    long   count_rows_decade[MAX_DECADES] = {0};

    double sum_ret_decade[MAX_DECADES]    = {0.0};
    double sum_ret_sq_decade[MAX_DECADES] = {0.0};
    long   count_ret_decade[MAX_DECADES]  = {0};

    // Global min/max years found across all data
    int global_min_year = 9999;
    int global_max_year = 0;

    // Start timing the parallel computation
    double start = omp_get_wtime();

    // (2) Parallel region: process files in parallel using OpenMP
    //     Each thread accumulates results in its own local arrays.
    #pragma omp parallel
    {
        // Thread-local accumulators (to avoid data races)
        double local_sum_avg[MAX_DECADES]    = {0.0};
        long   local_rows[MAX_DECADES]       = {0};

        double local_sum_ret[MAX_DECADES]    = {0.0};
        double local_sum_ret_sq[MAX_DECADES] = {0.0};
        long   local_ret_count[MAX_DECADES]  = {0};

        int local_min_year = 9999;
        int local_max_year = 0;

        #pragma omp for schedule(runtime)
        for (int idx_file = 0; idx_file < file_count; idx_file++) {

            const char *filename = file_list[idx_file];

            StockData *data = NULL;
            int n = read_csv(filename, &data);
            if (n <= 1 || !data) {
                if (data) free(data);
                continue;
            }

            // Update local min/max year for this thread
            for (int i = 0; i < n; i++) {
                int year = 0;
                sscanf(data[i].date, "%d", &year);
                if (year < local_min_year) local_min_year = year;
                if (year > local_max_year) local_max_year = year;
            }

            // Collect daily average prices
            for (int i = 0; i < n; i++) {
                double o = data[i].open;
                double h = data[i].high;
                double l = data[i].low;
                double c = data[i].close;

                int year = 0;
                sscanf(data[i].date, "%d", &year);

                // Filter invalid years
                if (year < MIN_YEAR_GLOBAL || year > MAX_YEAR_GLOBAL)
                    continue;

                int decade_index = (year - MIN_YEAR_GLOBAL) / 10;
                if (decade_index < 0 || decade_index >= MAX_DECADES)
                    continue;

                // Filter unrealistic prices
                if (o >= MIN_PRICE && o <= MAX_PRICE &&
                    h >= MIN_PRICE && h <= MAX_PRICE &&
                    l >= MIN_PRICE && l <= MAX_PRICE &&
                    c >= MIN_PRICE && c <= MAX_PRICE)
                {
                    double avg = (o + h + l + c) / 4.0;
                    local_sum_avg[decade_index]    += avg;
                    local_rows[decade_index]       += 1;
                }
            }

            // Collect daily returns
            // r = (q - p) / p  between consecutive closes
             for (int i = 0; i < n - 1; i++) {
                double p = data[i].close;
                double q = data[i + 1].close;

                int year = 0;
                sscanf(data[i].date, "%d", &year);

                // Filter invalid years
                if (year < MIN_YEAR_GLOBAL || year > MAX_YEAR_GLOBAL)
                    continue;

                int decade_index = (year - MIN_YEAR_GLOBAL) / 10;
                if (decade_index < 0 || decade_index >= MAX_DECADES)
                    continue;

                // Filter unrealistic / invalid prices and zero division
                if (p >= MIN_PRICE && p <= MAX_PRICE &&
                    q >= MIN_PRICE && q <= MAX_PRICE &&
                    p != 0.0)
                {
                    double r = (q - p) / p;

                    // Exclude extreme outliers (> 100% daily move)
                    if (fabs(r) > 1.0)
                        continue;

                    local_sum_ret[decade_index]    += r;
                    local_sum_ret_sq[decade_index] += r * r;
                    local_ret_count[decade_index]  += 1;
                }
            }

            // Free memory for this file
            free(data);
        } // end for files

        // (3) Merge local thread results into global accumulators
        //     Critical section protects shared global arrays.
        #pragma omp critical
        {
            for (int d = 0; d < MAX_DECADES; d++) {
                sum_avg_decade[d]    += local_sum_avg[d];
                count_rows_decade[d] += local_rows[d];

                sum_ret_decade[d]    += local_sum_ret[d];
                sum_ret_sq_decade[d] += local_sum_ret_sq[d];
                count_ret_decade[d]  += local_ret_count[d];
            }

            if (local_min_year < global_min_year) global_min_year = local_min_year;
            if (local_max_year > global_max_year) global_max_year = local_max_year;
        }
    } // end parallel region

    double end = omp_get_wtime();

    // (4) Print market summary per decade
    printf("Market Summary by Decade (OpenMP):\n");
    printf("------------------------------------------------------------\n");

    int first_decade = (global_min_year / 10) * 10;
    int last_decade  = 2010;  // final printed period: 2010–2020

    for (int decade_start = first_decade; decade_start <= last_decade; decade_start += 10) {
        int d_idx = (decade_start - MIN_YEAR_GLOBAL) / 10;
        if (d_idx < 0 || d_idx >= MAX_DECADES)
            continue;

        long rows = count_rows_decade[d_idx];
        long rets = count_ret_decade[d_idx];

        // Skip decades with no data
        if (rows == 0 && rets == 0)
            continue;

        double mean_price = 0.0;
        double vol = 0.0;
        double mean_r = 0.0;
        double annual_r = 0.0;

        // Average daily price for this decade
        if (rows > 0) {
            mean_price = sum_avg_decade[d_idx] / (double)rows;
        }

        // Volatility and returns for this decade
        if (rets > 0) {
            mean_r  = sum_ret_decade[d_idx] / (double)rets;  // mean daily return
            double mean_r2 = sum_ret_sq_decade[d_idx] / (double)rets;
            double var = mean_r2 - mean_r * mean_r;
            if (var < 0.0) var = 0.0;
            vol = sqrt(var);

            // Approximate annualized return (252 trading days)
            annual_r = mean_r * 252.0;
        }

        int decade_end = (decade_start == 2010) ? 2020 : (decade_start + 9);

        printf("Decade %d–%d:\n", decade_start, decade_end);
        printf("  Rows used:             %ld\n", rows);
        printf("  Mean market price:     %.4f\n", mean_price);
        printf("  Market volatility:     %.4f (%.4f%%)\n",
               vol, vol * 100.0);

        if (rets > 0) {
            printf("  Mean daily return:     %.6f (%.4f%%)\n",
                   mean_r, mean_r * 100.0);
            printf("  Approx annual return:  %.6f (%.4f%%)\n\n",
                   annual_r, annual_r * 100.0);
        } else {
            printf("  Mean daily return:     N/A\n");
            printf("  Approx annual return:  N/A\n\n");
        }
    }

    printf("Overall Years Range in Data: %d–%d\n", global_min_year, global_max_year);
    printf("Execution time (OpenMP): %.6f seconds\n", end - start);

    // Free file paths
    for (int i = 0; i < file_count; i++) free(file_list[i]);
    free(file_list);

    return 0;
}

// gcc -O3 -fopenmp openMP_Version.c -o omp
// export OMP_NUM_THREADS=2
// export OMP_NUM_THREADS=4
// export OMP_NUM_THREADS=8
// export OMP_NUM_THREADS=16
// export OMP_SCHEDULE="static,1000"
// export OMP_SCHEDULE="dynamic,1000"
// export OMP_SCHEDULE="guided,1000"   
// ./omp stocks
