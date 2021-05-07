/* You may refer to: https://cstack.github.io/db_tutorial/ */
/* Compile: gcc -o myjql myjql.c -O3 */
/* Test: /usr/bin/time -v ./myjql myjql.db < in.txt > out.txt */
/* Compare: diff out.txt ans.txt */

#include <fcntl.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define COLUMN_B_SIZE 11
#define TABLE_MAX_PAGES 500
const uint32_t PAGE_SIZE = 4096;
// FIXME: test whether 500 is enough

// the table struct specified in the PJ
typedef struct {
    uint32_t a;
    char b[COLUMN_B_SIZE + 1];
} Row;

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    Pager* pager;
    uint32_t root_page_num;
} Table;

typedef struct {
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool is_end_of_table;
} Cursor;

Table table;

/* shell IO */

#define INPUT_BUFFER_SIZE 31
struct {
    char buffer[INPUT_BUFFER_SIZE + 1];
    size_t length;
} input_buffer;

typedef enum { INPUT_SUCCESS, INPUT_TOO_LONG } InputResult;

void print_prompt() { printf("myjql> "); }

InputResult read_input() {
    /* we read the entire line as the input */
    input_buffer.length = 0;
    while (input_buffer.length <= INPUT_BUFFER_SIZE &&
           (input_buffer.buffer[input_buffer.length++] = getchar()) != '\n' &&
           input_buffer.buffer[input_buffer.length - 1] != EOF)
        ;
    if (input_buffer.buffer[input_buffer.length - 1] == EOF) exit(EXIT_SUCCESS);
    input_buffer.length--;
    /* if the last character is not new-line, the input is considered too long,
       the remaining characters are discarded */
    if (input_buffer.length == INPUT_BUFFER_SIZE &&
        input_buffer.buffer[input_buffer.length] != '\n') {
        while (getchar() != '\n')
            ;
        return INPUT_TOO_LONG;
    }
    input_buffer.buffer[input_buffer.length] = 0;
    return INPUT_SUCCESS;
}

Pager* pager_open(const char* filename) {
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1) {
        // FIXME: handle open failure
    }
    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->pages_num = (file_length / PAGE_SIZE);

    if (file_length % PAGE_SIZE != 0) {
        // FIXME: handle corrupt file
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        // FIXME: handle more pages than boundary
    }
    // if no cache, read from disk
    if (pager->pages[page_num] == NULL) {
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                // FIXME: handle reading failure
            }
        }
        pager->pages[page_num] = page;
        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
    }
}

void open_file(const char* filename) { /* open file */
    Pager* pager = pager_open(filename);
    table.pager = pager;
    table.root_page_num = 0;

    if (pager->num_pages == 0) {
        // new table
        // TODO: new table
        /*void* root_node = get_page(pager,0);*/
        /*initialize_leaf_node(root_node);*/
        /*set_onde_root(root_node, true);*/
    }
}

void exit_nicely(int code) {
    /* do clean work */
    exit(code);
}

void exit_success() {
    printf("bye~\n");
    exit_nicely(EXIT_SUCCESS);
}

/* specialization of data structure */

void print_row(Row* row) { printf("(%d, %s)\n", row->a, row->b); }

/* statement */

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_DELETE
} StatementType;

struct {
    StatementType type;
    Row row;
    uint8_t flag; /* whether row.a, row.b have valid values */
} statement;

/* B-Tree operations */

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

/* leaf node body */

/* the key to select is stored in `statement.row.b` */
void b_tree_search() {
    /* print selected rows */
    printf("[INFO] select: %s\n", statement.row.b);
}

/* the row to insert is stored in `statement.row` */
void b_tree_insert() {
    /* insert a row */
    printf("[INFO] insert: ");
    print_row(&statement.row);
}

/* the key to delete is stored in `statement.row.b` */
void b_tree_delete() {
    /* delete row(s) */
    printf("[INFO] delete: %s\n", statement.row.b);
}

void b_tree_traverse() {
    /* print all rows */
    printf("[INFO] traverse\n");
}

/* logic starts */

typedef enum { EXECUTE_SUCCESS } ExecuteResult;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_VALUE,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_EMPTY_STATEMENT
} PrepareResult;

MetaCommandResult do_meta_command() {
    if (strcmp(input_buffer.buffer, ".exit") == 0) {
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_insert() {
    statement.type = STATEMENT_INSERT;

    char* keyword = strtok(input_buffer.buffer, " ");
    char* a = strtok(NULL, " ");
    char* b = strtok(NULL, " ");
    int x;

    if (a == NULL || b == NULL) return PREPARE_SYNTAX_ERROR;

    x = atoi(a);
    if (x < 0) return PREPARE_NEGATIVE_VALUE;
    if (strlen(b) > COLUMN_B_SIZE) return PREPARE_STRING_TOO_LONG;

    statement.row.a = x;
    strcpy(statement.row.b, b);

    return PREPARE_SUCCESS;
}

PrepareResult prepare_condition() {
    statement.flag = 0;

    char* keyword = strtok(input_buffer.buffer, " ");
    char* b = strtok(NULL, " ");
    char* c = strtok(NULL, " ");

    if (b == NULL) return PREPARE_SUCCESS;
    if (c != NULL) return PREPARE_SYNTAX_ERROR;

    if (strlen(b) > COLUMN_B_SIZE) return PREPARE_STRING_TOO_LONG;

    strcpy(statement.row.b, b);
    statement.flag |= 2;

    return PREPARE_SUCCESS;
}

PrepareResult prepare_select() {
    statement.type = STATEMENT_SELECT;
    return prepare_condition();
}

PrepareResult prepare_delete() {
    statement.type = STATEMENT_DELETE;
    PrepareResult result = prepare_condition();
    if (result == PREPARE_SUCCESS && statement.flag == 0)
        return PREPARE_SYNTAX_ERROR;
    return result;
}

PrepareResult prepare_statement() {
    if (strlen(input_buffer.buffer) == 0) {
        return PREPARE_EMPTY_STATEMENT;
    } else if (strncmp(input_buffer.buffer, "insert", 6) == 0) {
        return prepare_insert();
    } else if (strncmp(input_buffer.buffer, "select", 6) == 0) {
        return prepare_select();
    } else if (strncmp(input_buffer.buffer, "delete", 6) == 0) {
        return prepare_delete();
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_select() {
    printf("\n");
    if (statement.flag == 0) {
        b_tree_traverse();
    } else {
        b_tree_search();
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement() {
    switch (statement.type) {
        case STATEMENT_INSERT:
            b_tree_insert();
            return EXECUTE_SUCCESS;
        case STATEMENT_SELECT:
            return execute_select();
        case STATEMENT_DELETE:
            b_tree_delete();
            return EXECUTE_SUCCESS;
    }
}

void sigint_handler(int signum) {
    printf("\n");
    exit(EXIT_SUCCESS);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    atexit(&exit_success);
    signal(SIGINT, &sigint_handler);

    open_file(argv[1]);

    while (1) {
        print_prompt();
        switch (read_input()) {
            case INPUT_SUCCESS:
                break;
            case INPUT_TOO_LONG:
                printf("Input is too long.\n");
                continue;
        }

        if (input_buffer.buffer[0] == '.') {
            switch (do_meta_command()) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf("Unrecognized command '%s'.\n", input_buffer.buffer);
                    continue;
            }
        }

        switch (prepare_statement()) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_EMPTY_STATEMENT:
                continue;
            case PREPARE_NEGATIVE_VALUE:
                printf("Column `a` must be positive.\n");
                continue;
            case PREPARE_STRING_TOO_LONG:
                printf("String for column `b` is too long.\n");
                continue;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s'.\n",
                       input_buffer.buffer);
                continue;
        }

        switch (execute_statement()) {
            case EXECUTE_SUCCESS:
                printf("\nExecuted.\n\n");
                break;
        }
    }
}
