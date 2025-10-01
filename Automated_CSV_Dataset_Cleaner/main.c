//  main.c
//  Automated_CSV_Dataset_Cleaner
//  DavidRichardson02

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <assert.h>

#include "CommonDefinitions.h"
#include "GeneralUtilities.h"
#include "StringUtilities.h"
#include "FileUtilities.h"
#include "DataExtraction.h"
#include "DebuggingUtilities.h"




/* ---------------------------------------------------------------------
 Config
 --------------------------------------------------------------------- */
static const char *kDefaultDataSetPath =
"/Users/98dav/Desktop/Xcode/C-Programs/Automated_CSV_Dataset_Cleaner/physics_particles.txt";

/* ---------------------------------------------------------------------
 Forward decls
 --------------------------------------------------------------------- */
static void test_strings(void);
static void test_general(void);
static void test_files_and_debug_io(const char *samplePath);
static void test_dataset_workflow(const char *dataset_path);


static int file_exists(const char *path);
static char **read_first_n_lines(const char *path, int max_lines, int *out_count);
static void free_lines(char **lines, int n);
static int split_line_by_delim(const char *line, char delim, char ***out_tokens);
static void free_tokens(char **toks, int count);
static void run_all_tests(const char *dataSetPath);






/**
 * main
 * Entry point for the control harness. Accepts an optional dataset path, runs a condensed end-to-end pipeline
 * (count → read → delimiter inference → header typing → missing audit → plottable extraction → write-outs → sample print),
 * cleans up, and then runs auxiliary tests.
 *
 * @param argc Argument count.
 * @param argv Argument vector; argv[1] may override the default dataset path.
 * @return EXIT_SUCCESS on success; 1 on early failure.
 */
int main(int argc, const char *argv[])
{
	/// Determine dataset path from CLI or default
	const char *dataSetFilePathName = kDefaultDataSetPath;
	if (argc > 1 && argv[1] && argv[1][0] != '\0') {
		dataSetFilePathName = argv[1];
	}
	
	/// Announce start and target dataset
	printf("Running Control Test Harness...\n");
	printf("Dataset: %s\n", dataSetFilePathName);
	
	/// Count and bulk-read file into memory
	int totalLines = count_file_lines(dataSetFilePathName, (int)MAX_NUM_FILE_LINES);
	if (totalLines <= 0) { fprintf(stderr, "No lines or read error.\n"); return 1; }
	printf("[INFO] totalLines = %d\n", totalLines);
	
	char **fileContents = read_file_contents(dataSetFilePathName, totalLines);
	if (!fileContents) { fprintf(stderr, "read_file_contents failed.\n"); return 1; }
	
	/// Infer delimiter and count header fields
	char *delimiter = identify_delimiter(fileContents, totalLines);
	if (!delimiter || delimiter[0] == '\0') { delimiter = ","; } // Fallback to comma
	printf("[INFO] delimiter = \"%s\"\n", delimiter);
	
	int fieldCount = count_data_fields(fileContents[0]);
	printf("[INFO] fieldCount (header) = %d\n", fieldCount);
	assert(fieldCount > 0);
	
	/// Build "name;type" header, find plottables, and audit missing values
	char **nameTypePairs = capture_data_set_header_for_plotting(
																fileContents[0], fileContents, delimiter);
	if (!nameTypePairs) { fprintf(stderr, "capture_data_set_header_for_plotting failed.\n"); return 1; }
	
	const char *typeDelimiter = ";";
	int *plottableMask = identify_plottable_fields(nameTypePairs, fieldCount, typeDelimiter);
	if (!plottableMask) { fprintf(stderr, "identify_plottable_fields failed.\n"); return 1; }
	
	printf("\n[HEADER name;type]\n");
	for (int i = 0; i < fieldCount; ++i) {
		printf("  [%2d] %s  %s\n", i, nameTypePairs[i],
			   plottableMask[i] ? "(plottable)" : "(non-plottable)");
	}
	
	int *missingCounts = count_missing_values(
											  fileContents, totalLines, fieldCount, delimiter, fileContents[0]);
	if (!missingCounts) { fprintf(stderr, "count_missing_values failed.\n"); return 1; }
	
	printf("\n[MISSING/INVALID by field]\n");
	for (int i = 0; i < fieldCount; ++i) {
		printf("  [%2d] %s -> %d\n", i, nameTypePairs[i], missingCounts[i]);
	}
	
	/// Build plottable-only dataset and write split outputs
	char **plottableDataSet = capture_data_set_for_plotting(
															fileContents, totalLines, delimiter);
	if (!plottableDataSet) { fprintf(stderr, "capture_data_set_for_plotting failed.\n"); return 1; }
	
	const char *rootOut = write_data_set(
										 fileContents, dataSetFilePathName, totalLines, fieldCount, delimiter);
	printf("\n[WRITE] write_data_set -> %s\n", rootOut ? rootOut : "(null)");
	
	/// Optional sample: print first few values from the first plottable series
	int firstPlotIdx = -1;
	for (int i = 0; i < fieldCount; ++i) if (plottableMask[i]) { firstPlotIdx = i; break; }
	if (firstPlotIdx >= 0) {
		double *oneSeries = extract_plottable_data_field(
														 plottableDataSet, firstPlotIdx, fieldCount, delimiter);
		if (oneSeries) {
			printf("\n[SAMPLE] First plottable series (col=%d):\n", firstPlotIdx);
			int show = (totalLines - 1) < 5 ? (totalLines - 1) : 5;
			for (int k = 0; k < show; ++k) {
				printf("  %g\n", oneSeries[k]);
			}
			free(oneSeries);
		}
	}
	
	/// Cleanup for main pipeline allocations
	deallocate_memory_char_ptr_ptr(fileContents, totalLines);
	deallocate_memory_char_ptr_ptr(nameTypePairs, fieldCount);
	deallocate_memory_char_ptr_ptr(plottableDataSet, totalLines);
	free(missingCounts);
	free(plottableMask);
	
	printf("\n[OK] Control test complete.\n\n\n\n\n\n\n\n\n");
	
	/// Run auxiliary test batteries
	run_all_tests(dataSetFilePathName);
	return EXIT_SUCCESS;
}





















































/* ---------------------------------------------------------------------
 Minimal local helpers (self-contained)
 --------------------------------------------------------------------- */
#ifndef MAX_STRING_SIZE
#define MAX_STRING_SIZE 8192
#endif

/**
 * file_exists
 * Checks whether a file exists and is readable by attempting to open it for reading.
 *
 * @param path The absolute or relative path of the file to probe.
 * @return 1 if the file can be opened (exists & readable), 0 otherwise.
 */
static int file_exists(const char *path) {
	/// Attempt to open to validate existence and read permission
	FILE *f = fopen(path, "r");                  // fopen returns NULL on failure
	if (!f) return 0;                            // Not present or not readable
												 /// Close and signal success
	fclose(f);                                   // Release handle
	return 1;
}

/**
 * read_first_n_lines
 * Reads up to max_lines lines from a text file into a heap-allocated, NULL-terminated array.
 * Trailing '\n' and '\r' are trimmed on each stored line. The number of lines read is returned via out_count.
 *
 * @param path Pathname of the file to read.
 * @param max_lines Maximum number of lines to read.
 * @param out_count Output pointer receiving the actual number of lines read.
 * @return An array of C-strings (last entry NULL) on success; NULL if the file cannot be opened.
 */
static char **read_first_n_lines(const char *path, int max_lines, int *out_count) {
	/// Open file and prepare buffers
	size_t buf_sz = (size_t)MAX_STRING_SIZE;     // Per-line buffer cap
	FILE *fp = fopen(path, "r");                 // Open for reading
	if (!fp) { *out_count = 0; return NULL; }    // Fail early if missing
	
	char **lines = (char **)malloc((size_t)(max_lines + 1) * sizeof(char *)); // +1 for sentinel
	int n = 0;                                   // Lines stored
	
	/// Read up to max_lines and trim line endings
	while (n < max_lines) {
		char *buf = (char *)malloc(buf_sz);      // Per-line buffer
		if (!fgets(buf, (int)buf_sz, fp)) {      // EOF or read error
			free(buf);
			break;
		}
		size_t L = strlen(buf);                   // Length of read line
		if (L && (buf[L-1] == '\n' || buf[L-1] == '\r')) buf[--L] = '\0'; // Trim newline
		if (L && (buf[L-1] == '\r')) buf[--L] = '\0';                     // Trim CR if CRLF
		lines[n++] = buf;                         // Keep the trimmed line
	}
	lines[n] = NULL;                              // Sentinel terminate
	fclose(fp);                                   // Close file
	*out_count = n;                               // Report final count
	return lines;
}

/**
 * free_lines
 * Frees an array of heap-allocated strings produced by read_first_n_lines and the array itself.
 *
 * @param lines The array of strings to free (may be NULL).
 * @param n The number of valid strings in the array (ignores trailing sentinel).
 * @return void
 */
static void free_lines(char **lines, int n) {
	/// Guard and free all strings then the container
	if (!lines) return;                           // Nothing to do
	for (int i = 0; i < n; ++i) free(lines[i]);   // Release each string
	free(lines);                                  // Release the array
}

/**
 * split_line_by_delim
 * Splits a single line on a single-character delimiter and returns a newly allocated array of tokens.
 *
 * @param line The input line to split.
 * @param delim The delimiter character to split on.
 * @param out_tokens Output pointer receiving the newly allocated token array.
 * @return The number of tokens produced.
 */
static int split_line_by_delim(const char *line, char delim, char ***out_tokens) {
	/// Prepare a mutable copy and pre-count to size the array
	size_t len = strlen(line);
	char *work = (char *)malloc(len + 1);         // Mutable buffer
	strcpy(work, line);                           // Duplicate text
	
	int count = 1;                                // Minimum tokens
	for (const char *p = line; *p; ++p) if (*p == delim) count++; // Count delimiters
	
	/// Allocate token array and slice segments
	char **toks = (char **)malloc((size_t)count * sizeof(char *));
	int k = 0;
	char *p = work, *start = work;                // Two-pointer slicing
	
	while (1) {
		if (*p == delim || *p == '\0') {
			size_t seglen = (size_t)(p - start);  // Segment length
			char *field = (char *)malloc(seglen + 1);
			memcpy(field, start, seglen);         // Copy slice
			field[seglen] = '\0';                 // NUL-terminate
			toks[k++] = field;                    // Store token
			if (*p == '\0') break;                // Done
			start = p + 1;                        // Next segment
		}
		p++;                                      // Advance
	}
	
	/// Dispose work buffer and return tokens
	free(work);
	*out_tokens = toks;
	return k;
}

/**
 * free_tokens
 * Frees a token vector produced by split_line_by_delim, including each token and the container array.
 *
 * @param toks The token array to free (may be NULL).
 * @param count Number of valid tokens in the array.
 * @return void
 */
static void free_tokens(char **toks, int count) {
	/// Guard and free tokens then container
	if (!toks) return;
	for (int i = 0; i < count; ++i) free(toks[i]);
	free(toks);
}

/* ---------------------------------------------------------------------
 Tests — Strings
 --------------------------------------------------------------------- */

/**
 * test_strings
 * Exercises delimiter discovery, dominant delimiter inference, and basic token-type classification helpers.
 *
 * @return void
 */
static void test_strings(void)
{
	/// Announce section
	printf("\n== test_strings ==\n");
	
	/// Phase 1: potential delimiters on a mixed sample
	const char *sample = "time-stamp, value_1; value-2\t| note";
	int delimCount = 0;
	char *delims = find_potential_delimiters(sample, &delimCount);
	printf("Potential delimiters: \"%s\" (count=%d)\n", delims ? delims : "", delimCount);
	free(delims);
	
	/// Phase 2: dominant delimiter inference from short lines
	char *lines[] = {
		"ts,value,unit",
		"2025-09-30 20:00:00,3.14,V",
		"2025-09-30 20:01:00,2.71,V",
		NULL
	};
	char *iden = identify_delimiter(lines, 3);
	printf("identify_delimiter => \"%s\"\n", iden ? iden : "(null)");
	if (iden) free(iden);
	
	/// Phase 3: typing probes (numeric-with-units, datetime, generic)
	const char *tok1 = "3.30V";
	const char *tok2 = "2025-09-30 20:02";
	const char *tok3 = "abc_xyz";
	char unitBuf[8] = "V";
	
	printf("is_numeric_with_units(\"%s\",\"%s\") = %s\n",
		   tok1, unitBuf, is_numeric_with_units(tok1, unitBuf) ? "true" : "false");
	
	int *is_dt = string_is_date_time(tok2, "-", 3);
	printf("string_is_date_time(\"%s\") => %s\n", tok2, (is_dt && is_dt[0]) ? "true" : "false");
	free(is_dt);
	
	printf("determine_string_representation_type(\"%s\") => %s\n",
		   tok3, determine_string_representation_type(tok3));
}

/* ---------------------------------------------------------------------
 Tests — Numeric/Sorting/Alloc
 --------------------------------------------------------------------- */

/**
 * is_nondecreasing
 * Verifies an array of doubles is sorted in nondecreasing order.
 *
 * @param a Pointer to the double array.
 * @param n Number of elements in the array.
 * @return 1 if sorted nondecreasing, 0 otherwise.
 */
static int is_nondecreasing(const double *a, int n) {
	/// Linear check for monotonic nondecreasing sequence
	for (int i = 1; i < n; ++i) if (a[i-1] > a[i]) return 0;
	return 1;
}

/**
 * test_general
 * Validates numeric helpers: allocation, random fill, summation, merge sort, radix sort, and order checks.
 *
 * @return void
 */
static void test_general(void)
{
	/// Allocate working arrays
	printf("\n== test_general ==\n");
	const int N = 32;
	double *arr = allocate_memory_double_ptr(N);
	double *a1  = allocate_memory_double_ptr(N);
	double *a2  = allocate_memory_double_ptr(N);
	
	/// Fill with deterministic random data and duplicate
	srand(42);
	for (int i = 0; i < N; ++i) {
		arr[i] = (double)random_in_range(-100, 100) + (rand() / (double)RAND_MAX);
		a1[i] = a2[i] = arr[i];
	}
	
	/// Aggregate and report
	double s = sum_elements(arr, N);
	printf("sum_elements(size=%d) = %.6f\n", N, s);
	
	/// Sort two ways and verify monotonicity
	merge_sort(a1, N);
	radix_sort_doubles(a2, N);
	if (!is_nondecreasing(a1, N) || !is_nondecreasing(a2, N)) {
		fprintf(stderr, "Sorting check failed.\n");
		exit(1);
	}
	printf("Sorting checks passed.\n");
	
	/// Cleanup
	free(arr); free(a1); free(a2);
}

/* ---------------------------------------------------------------------
 Tests — Filesystem + Debug I/O
 --------------------------------------------------------------------- */

/**
 * test_files_and_debug_io
 * Creates a temporary directory, writes a tiny CSV and numeric series, inspects contents, merges a file with itself, prints results, and cleans up.
 *
 * @param samplePath A path whose directory will host the transient test directory.
 * @return void
 */
static void test_files_and_debug_io(const char *samplePath)
{
	/// Create ephemeral test directory
	printf("\n== test_files_and_debug_io ==\n");
	char *dirPath = create_directory(samplePath, "testdir");
	printf("Created directory: %s\n", dirPath);
	
	/// Write a small CSV file
	char *fileAPath = allocate_memory_char_ptr(string_length(dirPath) + 64);
	sprintf(fileAPath, "%s/%s", dirPath, "A.csv");
	char *contentsA[] = {
		"ts,value,unit\n",
		"2025-09-30 20:00:00,1.00,V\n",
		"2025-09-30 20:01:00,2.00,V\n",
		NULL
	};
	write_file_contents(fileAPath, contentsA);
	
	/// Write a numeric series file
	char *fileBPath = allocate_memory_char_ptr(string_length(dirPath) + 64);
	sprintf(fileBPath, "%s/%s", dirPath, "B_values.txt");
	double series[] = {0.0, 1.5, 2.5, 3.5};
	write_file_numeric_data(fileBPath, series, (int)(sizeof(series)/sizeof(series[0])), "B_value");
	
	/// Inspect: count lines and list directory entries
	int aLines = count_file_lines(fileAPath, (int)MAX_NUM_FILE_LINES);
	printf("count_file_lines(%s) = %d\n", fileAPath, aLines);
	print_directory_data_files(dirPath);
	
	/// Merge a file with itself and print content
	char *mergedPath = merge_two_files(fileAPath, fileAPath);
	printf("Merged file path: %s\n", mergedPath);
	print_data_file(mergedPath);
	
	/// Release temp strings for files and directory
	free(mergedPath);
	free(fileAPath);
	free(fileBPath);
	free(dirPath);
	
	/// Remove the scratch directory tree
	char *recompute = create_directory(samplePath, "testdir"); // Recreate name exactly
	if (delete_directory(recompute) != 0) perror("delete_directory");
	free(recompute);
}

/* ---------------------------------------------------------------------
 Tests — Dataset-Driven Workflow (uses your real dataset)
 --------------------------------------------------------------------- */

/**
 * test_dataset_workflow
 * Runs a representative mini-pipeline on the dataset: counts, preview print, delimiter inference, header tokenization, per-column type sampling, and a normalized preview CSV emission.
 *
 * @param dataset_path Path to the dataset to analyze.
 * @return void
 */
static void test_dataset_workflow(const char *dataset_path)
{
	/// Sanity-check dataset
	printf("\n== test_dataset_workflow ==\n");
	printf("Dataset path: %s\n", dataset_path);
	if (!file_exists(dataset_path)) {
		fprintf(stderr, "[WARN] File does not exist. Skipping dataset tests.\n");
		return;
	}
	
	/// Count lines and print a first peek
	int total_lines = count_file_lines(dataset_path, (int)MAX_NUM_FILE_LINES);
	printf("count_file_lines(...) => %d\n", total_lines);
	printf("\n-- First peek (via print_data_file) --\n");
	print_data_file(dataset_path);
	
	/// Sample a subset for delimiter and typing
	int lines_read = 0;
	const int sample_n = 32;
	char **lines = read_first_n_lines(dataset_path, sample_n, &lines_read);
	if (!lines || lines_read == 0) {
		fprintf(stderr, "[WARN] Unable to read sample lines.\n");
		free_lines(lines, lines_read);
		return;
	}
	
	char *iden = identify_delimiter(lines, lines_read);
	char delim = (iden && iden[0]) ? iden[0] : ',';
	printf("\nInferred delimiter: \"%c\" (identify_delimiter => \"%s\")\n",
		   delim, iden ? iden : "(null)");
	if (iden) free(iden);
	
	/// Tokenize header and sample type per column from a small row subset
	if (lines_read > 0) {
		char **hdr = NULL;
		int hdr_cnt = split_line_by_delim(lines[0], delim, &hdr);
		printf("\nHeader has %d column(s):\n", hdr_cnt);
		for (int i = 0; i < hdr_cnt; ++i) printf("  [%d] \"%s\"\n", i, hdr[i]);
		
		int sample_rows = (lines_read - 1) > 12 ? 12 : (lines_read - 1);
		printf("\nPer-column token typing (first %d data row(s)):\n", sample_rows > 0 ? sample_rows : 0);
		for (int c = 0; c < hdr_cnt; ++c) {
			const char *type_str = NULL;
			for (int r = 1; r <= sample_rows; ++r) {
				char **toks = NULL;
				int tcnt = split_line_by_delim(lines[r], delim, &toks);
				if (c < tcnt) {
					const char *tok = toks[c];
					if (tok && tok[0] != '\0') {
						type_str = determine_string_representation_type(tok);
						free_tokens(toks, tcnt);
						break;
					}
				}
				free_tokens(toks, tcnt);
			}
			printf("  Col %d (\"%s\"): %s\n", c, hdr[c], type_str ? type_str : "(unknown/empty)");
		}
		free_tokens(hdr, hdr_cnt);
	}
	
	/// Emit a comma-normalized preview CSV into a sibling output dir
	char *out_dir = create_directory(dataset_path, "test_out");
	printf("\nCreated/cleaned output dir: %s\n", out_dir);
	
	char *preview_path = allocate_memory_char_ptr(string_length(out_dir) + 64);
	sprintf(preview_path, "%s/%s", out_dir, "dataset_preview.csv");
	
	int keep = lines_read < 26 ? lines_read : 26; // Header + up to 25 rows
	char **preview = (char **)malloc((size_t)(keep + 1) * sizeof(char *));
	for (int i = 0; i < keep; ++i) {
		const char *src = lines[i];
		size_t L = strlen(src);
		char *dst = (char *)malloc(L + 2);
		if (delim != ',') {
			for (size_t j = 0; j < L; ++j) dst[j] = (src[j] == delim) ? ',' : src[j];
			dst[L] = '\n'; dst[L+1] = '\0';
		} else {
			memcpy(dst, src, L);
			dst[L] = '\n'; dst[L+1] = '\0';
		}
		preview[i] = dst;
	}
	preview[keep] = NULL;
	
	write_file_contents(preview_path, preview);
	printf("Wrote preview CSV: %s\n", preview_path);
	
	printf("\n-- Preview (normalized) via print_data_file --\n");
	print_data_file(preview_path);
	
	/// Cleanup preview resources and sample lines
	for (int i = 0; i < keep; ++i) free(preview[i]);
	free(preview);
	free(preview_path);
	free(out_dir);
	free_lines(lines, lines_read);
}

/**
 * run_all_tests
 * Executes all local test suites in order: string utilities, numeric/sort, filesystem+debug I/O, and dataset mini-workflow.
 *
 * @param dataSetPath Dataset path passed to I/O and workflow tests.
 * @return void
 */
static void run_all_tests(const char *dataSetPath)
{
	/// Execute string utilities tests
	test_strings();
	/// Execute numeric/sorting tests
	test_general();
	/// Exercise filesystem + debug output helpers
	test_files_and_debug_io(dataSetPath);
	/// Execute dataset-driven mini pipeline
	test_dataset_workflow(dataSetPath);
}
