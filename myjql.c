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
const uint32_t A_SIZE = size_of_attribute(Row, a);
const uint32_t B_SIZE = size_of_attribute(Row, b);
const uint32_t A_OFFSET = 0;
const uint32_t B_OFFSET = A_OFFSET + A_SIZE;
const uint32_t ROW_SIZE = A_SIZE + B_SIZE;

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

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

/*
 *functions declartions
 */
Cursor* leaf_node_find(uint32_t page_num, uint32_t key);

// database utility functions
uint32_t get_unused_page_num(Pager* pager);
void* get_page(Pager* pager, uint32_t page_num);
Pager* pager_open(const char* filename);
void serialize_row(Row* source, void* destination);
// node common
NodeType get_node_type(void* node);
void set_node_type(void* node, NodeType type);
void set_node_root(void* node, bool is_root);
bool is_node_root(void* node);
uint32_t get_node_max_key(void* node);
uint32_t* node_parent(void* node);
// internal nodes
uint32_t internal_node_find_child(void* node, uint32_t key);
uint32_t* internal_node_cell(void* node, uint32_t cell_num);
uint32_t* internal_node_key(void* node, uint32_t key_num);
uint32_t* internal_node_right_child(void* node);
uint32_t* internal_node_num_keys(void* node);
uint32_t* internal_node_child(void* node, uint32_t child_num);
void initialize_internal_node(void* node);
// leaf nodes
uint32_t* leaf_node_num_cells(void* node);
uint32_t* leaf_node_next_leaf(void* node);
uint32_t* leaf_node_cell(void* node, uint32_t cell_num);
uint32_t* leaf_node_key(void* node, uint32_t key_num);
void initialize_leaf_node(void* node);

// TODO: move all definitions here to give autocompletion

/*
 *nodes
 */

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
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS =
    (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE) / INTERNAL_NODE_CELL_SIZE;
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

/*
 *database utility functions
 */
uint32_t get_unused_page_num(Pager* pager) {
    return pager->num_pages;
    // TODO: recycle free pages
}
// get one page by page_num
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
void create_new_root(uint32_t right_child_page_num) {
    void* root = get_page(table.pager, table.root_page_num);
    void* right_child = get_page(table.pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table.pager);
    void* left_child = get_page(table.pager, left_child_page_num);

    // left child has old root all data
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);
    // set root node to internal node
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
    *node_parent(left_child) = table.root_page_num;
    *node_parent(right_child) = table.root_page_num;
}

// copy data to database
void serialize_row(Row* source, void* destination) {
    memcpy(destination + A_OFFSET, &(source->a), A_SIZE);
    memcpy(destination + B_OFFSET, &(source->b), B_SIZE);
}

/*
 *node utility functions
 */

NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}
void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}
void set_node_root(void* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}
bool is_node_root(void* node) {
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}
// get max key number for a node
uint32_t get_node_max_key(void* node) {
    switch (get_node_type(node)) {
        case NODE_INTERNAL:
            return *internal_node_key(node, *internal_node_num_keys(node) - 1);
        case NODE_LEAF:
            return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    }
}
// return parent pointer of a node
uint32_t* node_parent(void* node) { return node + PARENT_POINTER_OFFSET; }

/*
 *internal nodes utility functions
 */

// return pointer to a cell in internal node
uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
    return node + INTERNAL_NODE_HEADER_SIZE +
           cell_num * INTERNAL_NODE_CELL_SIZE;
}
// get key of one child by its cell number (key_num)
uint32_t* internal_node_key(void* node, uint32_t key_num) {
    return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}
// return pointer to right child in internal node
uint32_t* internal_node_right_child(void* node) {
    return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}
// get pointer to number of keys for internal node
uint32_t* internal_node_num_keys(void* node) {
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}
// get child from an internal node by child number
uint32_t* internal_node_child(void* node, uint32_t child_num) {
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys) {
        // FIXME: handle more child than keys
    } else if (child_num == num_keys) {
        return internal_node_right_child(node);
    } else {
        return internal_node_cell(node, child_num);
    }
}
// add new child to internal node
void internal_node_insert(uint32_t parent_page_num, uint32_t child_page_num) {
    void* parent = get_page(table.pager, parent_page_num);
    void* child = get_page(table.pager, child_page_num);
    uint32_t child_max_key = get_node_max_key(child);
    uint32_t index = internal_node_find_child(parent, child_max_key);

    uint32_t original_num_keys = *internal_node_num_keys(parent);
    *internal_node_num_keys(parent) = original_num_keys + 1;

    if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
        // TODO: implement internal node split
    }
    uint32_t right_child_page_num = *internal_node_right_child(parent);
    void* right_child = get_page(table.pager, right_child_page_num);

    if (child_max_key > get_node_max_key(right_child)) {
        *internal_node_child(parent, original_num_keys) = right_child_page_num;
        *internal_node_key(parent, original_num_keys) =
            get_node_max_key(right_child);
        *internal_node_right_child(parent) = child_page_num;
    } else {
        for (uint32_t i = original_num_keys; i > index; i--) {
            void* destination = internal_node_cell(parent, i);
            void* source = internal_node_cell(parent, i - 1);
            memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_child(parent, index) = child_page_num;
        *internal_node_key(parent, index) = child_max_key;
    }
}
// leaf node utility functions
uint32_t* leaf_node_num_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}
// get leaf-node-next key
uint32_t* leaf_node_next_leaf(void* node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}
// get one particular cell in a leaf node by cell number
uint32_t* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}
//      return create_new
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
void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
}
// new leaf node
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
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
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

// return index of the child which should contain the key
uint32_t internal_node_find_child(void* node, uint32_t key) {
    uint32_t num_keys = *internal_node_num_keys(node);

    // binary search to find left boundary
    uint32_t left = 0, right = num_keys, mid, key_to_right;
    while (left < right) {
        mid = left + ((right - left) >> 2);
        key_to_right = *internal_node_key(node, mid);
        if (key_to_right >= mid) {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    return left;
}
Cursor* internal_node_find(uint32_t page_num, uint32_t key) {
    void* node = get_page(table.pager, page_num);

    uint32_t child_index = internal_node_find_child(node, key);
    uint32_t child_num = *internal_node_child(node, child_index);
    void* child = get_page(table.pager, child_num);
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(child_num, key);
        case NODE_INTERNAL:
            return internal_node_find(child_num, key);
    }
}
Cursor* leaf_node_find(uint32_t page_num, uint32_t key) {
    // find key on leaf node
    void* node = get_page(table.pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = &table;
    cursor->page_num = page_num;
    cursor->is_end_of_table = false;

    // binary search
    uint32_t left = 0, right = num_cells, mid, key_at_index;
    while (left != right) {
        mid = left + ((right - left) >> 2);
        key_at_index = *leaf_node_key(node, mid);
        if (key == key_at_index) {
            cursor->cell_num = mid;
            return cursor;
        } else if (key < key_at_index) {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    cursor->cell_num = left;
    return cursor;
}
Cursor* table_find(uint32_t key) {
    uint32_t root_page_num = table.root_page_num;
    void* root_node = get_page(table.pager, root_page_num);
    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(root_page_num, key);
    } else {
        return internal_node_find(root_page_num, key);
    }
}

// node is full, need spliting
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    // old node on the left, new node on the right
    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t old_max = get_node_max_key(old_node);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    *node_parent(new_node) = *node_parent(old_node);
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node) = new_page_num;

    for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
        void* destination_node;
        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
            destination_node = new_node;
        } else {
            destination_node = old_node;
        }
        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = leaf_node_cell(destination_node, index_within_node);

        if (i == cursor->cell_num) {
            serialize_row(value,
                          leaf_node_value(destination_node, index_within_node));
            *leaf_node_key(destination_node, index_within_node) = key;
        } else if (i > cursor->cell_num) {
            memcpy(destination, leaf_node_cell(old_node, i - 1),
                   LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(destination, leaf_node_cell(old_node, i),
                   LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if (is_node_root(old_node)) {
        return create_new_root(new_page_num);
    } else {
        uint32_t parent_page_num = *node_parent(old_node);
        uint32_t new_max = get_node_max_key(old_node);
        void* parent = get_page(table.pager, parent_page_num);
        update_internal_node_key(parent, old_max, new_max);
        internal_node_insert(parent_page_num, new_page_num);
        return;
    }
}
// handle inserting node
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    void* node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // node is full
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }
    if (cursor->cell_num < num_cells) {
        // in the middle
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
                   LEAF_NODE_CELL_SIZE);
        }
    }
    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}
/* the row to insert is stored in `statement.row` */
void b_tree_insert() {
    /* insert a row */
    printf("[INFO] insert: ");
    print_row(&statement.row);

    Row* row_to_insert = &statement.row;
    uint32_t key_to_insert = row_to_insert->a;
    Cursor* cursor = table_find(key_to_insert);

    void* node = get_page(table.pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    if (cursor->cell_num < num_cells) {
        // FIXME: handle duplicate key
    }

    leaf_node_insert(cursor, row_to_insert->a, row_to_insert);
    free(cursor);
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

