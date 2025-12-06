#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <omp.h>

#define MAX_LINE_LEN 256
#define MAX_DAYS 1000000   

typedef struct {
    char date[20];
    double open, high, low, close, volume;
} StockData;

double daily_average(StockData s) {
    return (s.open + s.high + s.low + s.close) / 4.0;
}

double daily_return(double prev_close, double curr_close) {
    if (prev_close == 0) return 0.0;
    return (curr_close - prev_close) / prev_close;
}

int read_csv(const char *filename, StockData *data, int max_days) {
    FILE *file = fopen(filename, "r");
    if (!file) return 0;

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
                   &data[count].volume) == 6)
        {
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

    printf("\nSerial Stock Analysis (All Files Combined)\n");
    printf("====================================================\n\n");

    long total_records = 0;
    double sum_avg = 0.0;
    double sum_ret = 0.0;
    double sum_ret_sq = 0.0;

    double start = omp_get_wtime();

    for (int f = file_start_index; f < argc; f++) {
        const char *filename = argv[f];
        StockData *data = malloc(sizeof(StockData) * max_days);

        int n = read_csv(filename, data, max_days);
        if (n <= 1) {
            free(data);
            continue;
        }

        total_records += n;

        for (int i = 0; i < n; i++)
            sum_avg += daily_average(data[i]);

        for (int i = 0; i < n - 1; i++) {
            double r = daily_return(data[i].close, data[i + 1].close);
            sum_ret += r;
            sum_ret_sq += r * r;
        }

        free(data);
    }

    double mean_price = sum_avg / total_records;

    double mean_ret = sum_ret / (total_records - 1);
    double mean_sq = sum_ret_sq / (total_records - 1);
    double variance = mean_sq - (mean_ret * mean_ret);
    if (variance < 0) variance = 0;
    double volatility = sqrt(variance);

    double end = omp_get_wtime();

    printf("Files: %d\n", argc - file_start_index);
    printf("Max days per file: %d\n\n", max_days);

    printf("Total records loaded: %ld\n", total_records);
    printf("Average daily price (all files): %.4f\n", mean_price);
    printf("Volatility (std. dev of returns): %.6f\n", volatility);
    printf("Volatility (percentage): %.4f%%\n", volatility * 100);
    printf("Execution time (serial): %.6f seconds\n", end - start);

    return 0;
}