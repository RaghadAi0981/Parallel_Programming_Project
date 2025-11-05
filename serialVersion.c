#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#define MAX_LINE_LEN 256
#define MAX_DAYS 1000000

typedef struct {
    char date[20];
    double open, high, low, close, volume;
} StockData;

//daily_average
double daily_average(StockData s) {
    return (s.open + s.high + s.low + s.close) / 4.0;
}

// daily_return
double daily_return(double prev_close, double curr_close) {
    if (prev_close == 0) return 0.0;
    return (curr_close - prev_close) / prev_close;
}

//compute_volatility
double compute_volatility(double *returns, int n) {
    if (n <= 1) return 0.0;
    double sum = 0.0, mean, variance = 0.0;
    for (int i = 0; i < n; i++) sum += returns[i];
    mean = sum / n;
    for (int i = 0; i < n; i++) variance += pow(returns[i] - mean, 2);
    return sqrt(variance / n);
}

//to read the file
int read_csv(const char *filename, StockData *data) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        return 0;
    }

    char line[MAX_LINE_LEN];
    int count = 0;

    // skips the title 
    fgets(line, sizeof(line), file);

    while (fgets(line, sizeof(line), file) && count < MAX_DAYS) {
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
        printf("Usage: %s <file1.csv> <file2.csv> ... <fileN.csv>\n", argv[0]);
        return 1;
    }

    printf("\nLoaded %d stock files for analysis\n", argc - 1);
    printf("===========================================\n\n");

    for (int f = 1; f < argc; f++) {
        const char *filename = argv[f];
        StockData *data = malloc(sizeof(StockData) * MAX_DAYS);
        double *returns = malloc(sizeof(double) * MAX_DAYS);

        int n = read_csv(filename, data);
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

        double avg_sum = 0.0;
        for (int i = 0; i < n; i++) {
            avg_sum += daily_average(data[i]);
            if (i > 0)
                returns[i - 1] = daily_return(data[i - 1].close, data[i].close);
        }

        double overall_avg = avg_sum / n;
        double volatility = compute_volatility(returns, n - 1);

        printf("Symbol: %s\n", symbol);
        printf("Records loaded: %d\n", n);
        printf("Average daily price (USD): %.4f\n", overall_avg);
        printf("Volatility (std. dev of returns): %.6f\n", volatility);
        printf("Volatility (percentage): %.4f%%\n", volatility * 100);
        printf("---------------------------------------------\n");

        free(data);
        free(returns);
    }

    return 0;
}
