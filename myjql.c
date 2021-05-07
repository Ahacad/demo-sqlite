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

/*
 *table
 */
#define COLUMN_B_SIZE 11
#define TABLE_MAX_PAGES 500
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
const uint32_t PAGE_SIZE = 4096;
// FIXME: test whether 500 is enough
// the table struct specified in the PJ
typedef struct {
    uint32_t a;
    char b[COLUMN_B_SIZE + 1];
} Row;
const uint32_t ROW_SIZE = size_of_attribute(Row, a) + size_of_attribute(Row, b);
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

/*
 *nodes
 */

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

// common header
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;
// internal node header
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;
// internal node body
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
// leaf node header
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
    LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE =
    NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NEXT_LEAF_SIZE;
// leaf body
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

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
    return pager->pages[page_num];
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
    pager->num_pages = (file_length / PAGE_SIZE);

    if (file_length % PAGE_SIZE != 0) {
        // FIXME: handle corrupt file
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        // initialize all pages to null
        pager->pages[i] = NULL;
    }

    return pager;
}

/*
 *node utility functions
 */

void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}
void set_node_root(void* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
bool is_node_root(void* node) {
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}
uint32_t* internal_node_num_keys(void* node) {
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}
uint32_t* leaf_node_num_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}
uint32_t* leaf_node_next_leaf(void* node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}
uint32_t* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}
uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}
uint32_t* leaf_node_value(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}
void initialize_internal_node(void* node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}
void initialize_leaf_node(void* node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;
}

void open_file(const char* filename) { /* open file */
    Pager* pager = pager_open(filename);

    // table is already defined globally
    table.pager = pager;
    table.root_page_num = 0;

    if (pager->num_pages == 0) {
        // new table
        void* root_node = get_page(pager, 0);
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
