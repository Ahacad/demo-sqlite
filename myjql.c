/* You may refer to: https://cstack.github.io/db_tutorial/ */ /* Compile: gcc -o
                                                                 myjql myjql.c
                                                                 -O3 */
/* Test: /usr/bin/time -v ./myjql myjql.db < in.txt > out.txt */
/* Compare: diff out.txt ans.txt */

#include "myjql.h"

#include <fcntl.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// print memory by hex, used for debugging
void print_bytes(void* ptr, int size) {
    unsigned char* p = ptr;
    int i;
    for (i = 0; i < size; i++) {
        printf("%02hhX ", p[i]);
    }
    printf("\n");
}

/*
 *table
 */
// FIXME: test whether 500 is enough
// the table struct specified in the PJ
const uint32_t A_SIZE = size_of_attribute(Row, a);
const uint32_t B_SIZE = size_of_attribute(Row, b);
const uint32_t A_OFFSET = 0;
const uint32_t B_OFFSET = 4;
const uint32_t ROW_SIZE = 16;

Pager pager;
Table table;

/*
 *functions declartions
 */
Cursor* internal_node_find(uint32_t page_num, uint32_t key);
uint32_t internal_node_find_child(internal_node* node, uint32_t key);

void pager_open(const char* filename);
void print_row(Row* row);
Cursor* table_find(uint32_t key);
Cursor* table_start();
NodeType get_node_type(void* node);
uint32_t get_unused_page_num();

// database utility functions
// node common
void set_node_type(void* node, NodeType type);
void set_node_root(void* node, bool is_root);
bool is_node_root(void* node);
uint32_t* node_parent(void* node);
// internal nodes
uint32_t* internal_node_cell(void* node, uint32_t cell_num);
uint32_t* internal_node_key(void* node, uint32_t key_num);
uint32_t* internal_node_right_child(void* node);
uint32_t* internal_node_num_keys(void* node);
uint32_t* internal_node_child(void* node, uint32_t child_num);
// leaf nodes
uint32_t* leaf_node_num_cells(void* node);
uint32_t* leaf_node_next_leaf(void* node);
uint32_t* leaf_node_cell(void* node, uint32_t cell_num);
uint32_t* leaf_node_key(void* node, uint32_t key_num);

void update_internal_node_key(internal_node* node, uint32_t old_key,
                              uint32_t new_key) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    node->body[old_child_index].key = new_key;
}

// REBORN!

/*
 *nodes
 */

// common header
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = 4;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = 8;
const uint32_t NODE_HEADER_SIZE = 12;
// internal node header
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = 12;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = 16;
const uint32_t INTERNAL_NODE_HEADER_SIZE = 20;
// internal node body
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_OFFSET = 0;
const uint32_t INTERNAL_NODE_KEY_OFFSET = 4;
const uint32_t INTERNAL_NODE_CELL_SIZE = 8;
// leaf node header
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = 12;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = 16;
const uint32_t LEAF_NODE_HEADER_SIZE = 20;
// leaf body
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = 12;
const uint32_t LEAF_NODE_VALUE_OFFSET = 4;
const uint32_t LEAF_NODE_CELL_SIZE = 16;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = 4076;

/*const uint32_t LEAF_NODE_MAX_CELLS =*/
/*LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;*/
// Right and left node numbers AFTER splitting

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
    /* if the last character is not new-line, the input is considered too
       long, the remaining characters are discarded */
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
uint32_t get_unused_page_num() {
    return pager.num_pages;
    // TODO: recycle free pages, LRU algo, page pool
}

void mark_written(uint32_t page_num) { pager.pages[page_num].written = true; }

// get one page by page_num
void* get_page(uint32_t page_num) {
    bool flag = false;
    uint32_t original = page_num;
    int resid = 0;
    if (page_num > TABLE_MAX_PAGES) {
        // FIXME: handle more pages than boundary
        flag = true;
        resid = page_num / PAGE_MOD;
        page_num = page_num % PAGE_MOD;
    }
    if (pager.pages[page_num].storage == NULL) {
        // if no cache, read from disk
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager.file_length / PAGE_SIZE;
        if (pager.file_length % PAGE_SIZE) {
            num_pages += 1;
        }

        if (page_num <= num_pages) {
            lseek(pager.file_descriptor, original * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager.file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                // FIXME: handle reading failure
            }
        }
        pager.pages[page_num].page_num = original;
        pager.pages[page_num].written = false;
        pager.pages[page_num].storage = page;
        if (page_num >= pager.num_pages) {
            pager.num_pages = page_num + 1;
        }
        return pager.pages[page_num].storage;
    } else {
        if (pager.pages[page_num].page_num == original) {
            return pager.pages[page_num].storage;
        } else {
            // hit another page, clean and reget
            if (pager.pages[page_num].written) {
                pager_flush(page_num);
            }
            void* page = malloc(PAGE_SIZE);
            uint32_t num_pages = pager.file_length / PAGE_SIZE;
            if (pager.file_length % PAGE_SIZE) {
                num_pages += 1;
            }
            if (page_num <= num_pages) {
                lseek(pager.file_descriptor, original * PAGE_SIZE, SEEK_SET);
                ssize_t bytes_read =
                    read(pager.file_descriptor, page, PAGE_SIZE);
                if (bytes_read == -1) {
                    // FIXME: handle reading failure
                }
            }
            pager.pages[page_num].page_num = original;
            pager.pages[page_num].written = false;
            pager.pages[page_num].storage = page;
            if (page_num >= pager.num_pages) {
                pager.num_pages = page_num + 1;
            }
            return pager.pages[page_num].storage;
        }
    }
}
void pager_open(const char* filename) {
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1) {
        // FIXME: handle open failure
    }
    off_t file_length = lseek(fd, 0, SEEK_END);

    pager.file_descriptor = fd;
    pager.file_length = file_length;
    pager.num_pages = (file_length / PAGE_SIZE);

    if (file_length % PAGE_SIZE != 0) {
        // FIXME: handle corrupt file
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        // initialize all pages to null
        pager.pages[i].storage = NULL;
    }
}

NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

// copy data to database
void serialize_row(Row* source, leaf_node_body* destination) {
    destination->a = source->a;
    strcpy(destination->b, source->b);
}
// copy data from database to destination
void deserialize_row(leaf_node_body* source, Row* destination) {
    destination->a = source->a;
    strcpy(destination->b, source->b);
    /*memcpy(&(destination->a), source + A_OFFSET, A_SIZE);*/
    /*memcpy(&(destination->b), source + B_OFFSET, B_SIZE);*/
}

/*
 *node utility functions
 */

// return node type (internal node OR leaf node)
void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET * 8)) = value;
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
    NodeType type = get_node_type(node);
    if (type == NODE_INTERNAL) {
        internal_node* new_node = node;
        return new_node->body[new_node->num_keys - 1].key;
    } else if (type == NODE_LEAF) {
        leaf_node* new_node = node;
        return new_node->values[new_node->num_cells - 1].a;
    }
    // FIXME: node type
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
    return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_KEY_OFFSET;
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
void internal_node_split(uint32_t page_num) {
    internal_node* node = get_page(page_num);
    if (node->is_root) {
        // if internal node is root
        // initialize two nodes
        uint32_t new_root_page_num = get_unused_page_num();
        internal_node* new_root_node = get_page(new_root_page_num);
        initialize_internal_node(new_root_node);
        uint32_t new_right_page_num = get_unused_page_num();
        internal_node* new_right_node = get_page(new_right_page_num);
        initialize_internal_node(new_right_node);
        // set nodes meta data
        node->is_root = false;
        new_root_node->is_root = true;
        new_root_node->rightest_child = new_right_page_num;
        node->parent = new_root_page_num;
        new_right_node->parent = new_root_page_num;

        // 251 -- 499
        for (uint32_t i = INTERNAL_NODE_MAX_CELLS - 1;
             i > INTERNAL_NODE_LEFT_SPLIT_SIZE; i--) {
            new_right_node->body[i % INTERNAL_NODE_LEFT_SPLIT_SIZE + 1] =
                node->body[i];
        }
        new_root_node->body[0].key =
            node->body[INTERNAL_NODE_LEFT_SPLIT_SIZE].key;
        new_root_node->num_keys = 1;
        new_root_node->body[0].child = page_num;
        new_right_node->rightest_child = node->rightest_child;

        new_right_node->rightest_child = node->rightest_child;
        node->rightest_child = node->body[INTERNAL_NODE_LEFT_SPLIT_SIZE].key;
        new_right_node->num_keys = INTERNAL_NODE_RIGHT_SPLIT_SIZE;
        node->num_keys = INTERNAL_NODE_LEFT_SPLIT_SIZE;

        mark_written(new_root_page_num);
        mark_written(new_right_page_num);
    } else {
        uint32_t old_max_key = get_node_max_key(node);
        internal_node* parent = get_page(node->parent);
        uint32_t new_right_page_num = get_unused_page_num();
        internal_node* new_right_node = get_page(new_right_page_num);
        initialize_internal_node(new_right_node);
        new_right_node->parent = node->parent;
        // 250 -- 499
        for (uint32_t i = INTERNAL_NODE_MAX_CELLS - 1;
             i > INTERNAL_NODE_LEFT_SPLIT_SIZE; i--) {
            new_right_node->body[i % INTERNAL_NODE_LEFT_SPLIT_SIZE + 1] =
                node->body[i];
        }
        node->num_keys = INTERNAL_NODE_LEFT_SPLIT_SIZE;
        new_right_node->num_keys = INTERNAL_NODE_RIGHT_SPLIT_SIZE + 1;
        new_right_node->rightest_child = node->rightest_child;
        update_internal_node_key(parent, old_max_key, get_node_max_key(node));
        internal_node_insert(node->parent, new_right_page_num);

        mark_written(new_right_page_num);
        mark_written(node->parent);
    }
}
/*
 *add new child to internal node
 *child can be leaf or internal node
 */
void internal_node_insert(uint32_t parent_page_num, uint32_t child_page_num) {
    internal_node* parent = get_page(parent_page_num);
    void* child = get_page(child_page_num);
    uint32_t child_max_key = get_node_max_key(child);
    uint32_t index = internal_node_find_child(parent, child_max_key);

    uint32_t original_num_keys = parent->num_keys;
    parent->num_keys += 1;
    mark_written(parent_page_num);

    uint32_t right_child_page_num = parent->rightest_child;
    void* right_child = get_page(right_child_page_num);

    if (child_max_key > get_node_max_key(right_child)) {
        parent->body[original_num_keys].child = right_child_page_num;
        parent->body[original_num_keys].key = get_node_max_key(right_child);
        parent->rightest_child = child_page_num;
    } else {
        for (int i = original_num_keys; i > index; i--) {
            parent->body[i] = parent->body[i - 1];
        }
        parent->body[index].child = child_page_num;
        parent->body[index].key = child_max_key;
    }
    if (parent->num_keys >= INTERNAL_NODE_MAX_CELLS) {
        internal_node_split(parent_page_num);
    }

    // first insert node then split
    /*
     *    if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
     *        if (is_node_root(parent)) {
     *            // parent internal node is root and needs spliting
     *            uint32_t new_page_num1 = get_unused_page_num(table.pager);
     *            void* new_node1 = get_page(table.pager, new_page_num1);
     *            initialize_internal_node(new_node1);
     *            uint32_t new_page_num2 = get_unused_page_num(table.pager);
     *            void* new_node2 = get_page(table.pager, new_page_num2);
     *            initialize_internal_node(new_node2);
     *            set_node_root(parent, false);
     *            set_node_root(new_node1, true);
     *            *node_parent(parent) = new_page_num1;
     *            *node_parent(new_node2) = new_page_num1;
     *
     *            for (int32_t i = INTERNAL_NODE_MAX_CELLS; i >= 0; i--) {
     *                if (i == INTERNAL_NODE_LEFT_SPLIT_SIZE) {
     *                    // put to root
     *                    *internal_node_key(new_node1, 0) =
     *                        *internal_node_key(parent, i);
     *                    // root child points to left and right
     *                    *internal_node_child(new_node1, 0) = parent_page_num;
     *                    *internal_node_right_child(new_node1) = new_page_num2;
     *                } else if (i > INTERNAL_NODE_LEFT_SPLIT_SIZE) {
     *                    // put to right
     *                    uint32_t right_index =
     *                        i % (INTERNAL_NODE_LEFT_SPLIT_SIZE + 1);
     *                    memcpy(new_node2, internal_node_cell(parent, i),
     *                           INTERNAL_NODE_CELL_SIZE);
     *                    *internal_node_right_child(new_node2) =
     *                        *internal_node_right_child(parent);
     *                }
     *            }
     *            *internal_node_right_child(parent) =
     *                *internal_node_child(parent,
     * INTERNAL_NODE_LEFT_SPLIT_SIZE);
     *            // renumber node key nums
     *            *internal_node_num_keys(parent) =
     * INTERNAL_NODE_LEFT_SPLIT_SIZE; *internal_node_num_keys(new_node1) = 1;
     *            *internal_node_num_keys(new_node2) =
     * INTERNAL_NODE_RIGHT_SPLIT_SIZE; } else {
     *            // new internal node on the right
     *            uint32_t new_page_num = get_unused_page_num(table.pager);
     *            void* new_node = get_page(table.pager, new_page_num);
     *            initialize_internal_node(new_node);
     *            for (int32_t i = INTERNAL_NODE_MAX_CELLS; i >= 0; i--) {
     *                if (i >= INTERNAL_NODE_LEFT_SPLIT_SIZE) {
     *                    uint32_t right_index =
     *                        i % (INTERNAL_NODE_LEFT_SPLIT_SIZE + 1);
     *                    memcpy(new_node, internal_node_cell(parent, i),
     *                           INTERNAL_NODE_CELL_SIZE);
     *                    *internal_node_right_child(new_node) =
     *                        *internal_node_right_child(parent);
     *                }
     *            }
     *            *internal_node_right_child(parent) =
     *                *internal_node_child(parent,
     * INTERNAL_NODE_LEFT_SPLIT_SIZE); *internal_node_num_keys(parent) =
     * INTERNAL_NODE_LEFT_SPLIT_SIZE; *internal_node_num_keys(new_node) =
     *                INTERNAL_NODE_RIGHT_SPLIT_SIZE + 1;
     *
     *            internal_node_insert(*node_parent(parent), new_page_num);
     *        }
     *    }
     */
}
/*
 * leaf node utility functions
 */

// return number of cells on a leaf node
uint32_t* leaf_node_num_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}
// get leaf-node-next key
uint32_t* leaf_node_next_leaf(void* node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}
// get one particular cell in a leaf node by cell number
uint32_t* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + (LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE);
}
// return key of a cell
uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}
// return value of a cell (a, b)
uint32_t* leaf_node_value(void* node, uint32_t cell_num) {
    return (void*)leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

// new leaf node
void initialize_leaf_node(leaf_node* node) {
    node->node_type = NODE_LEAF;
    node->is_root = false;
    node->num_cells = 0;
    node->next_leaf = 0;
}
// new internal node
void initialize_internal_node(internal_node* node) {
    node->node_type = NODE_INTERNAL;
    node->is_root = false;
    node->num_keys = 0;
}

void open_file(const char* filename) { /* open file */

    // table and pager is already defined globally
    pager_open(filename);
    table.pager = pager;
    table.root_page_num = 0;

    // new table
    internal_node* root_node = get_page(0);
    initialize_internal_node(root_node);

    root_node->is_root = true;

    // pre build tree
    for (int i = 0; i <= 20; i++) {
        uint32_t page_num = get_unused_page_num();
        void* page = malloc(PAGE_SIZE);
        pager.pages[page_num].storage = page;
        pager.num_pages = page_num + 1;

        leaf_node* node = get_page(page_num);
        initialize_leaf_node(node);
        node->node_type = NODE_LEAF;
        node->next_leaf = page_num + 1;
        node->num_cells = 0;

        root_node->body[i].child = page_num;
        root_node->body[i].key = (250 * (i + 1)) - 1;
    }
    leaf_node* node = get_page(20);
    node->next_leaf = 0;

    root_node->num_keys = 20;
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
    uint8_t flag;  // 0: only `insert` or `select`, 1: one arg
} statement;

/* B-Tree operations */

// return position of a given key, result will be on leaf node
// if not present return the position where it should be inserted
Cursor* table_find(uint32_t key) {
    uint32_t root_page_num = table.root_page_num;
    void* root_node = get_page(root_page_num);

    if ((get_node_type(root_node)) == NODE_LEAF) {
        return leaf_node_find(root_page_num, key);
    } else {
        return internal_node_find(root_page_num, key);
    }
}
// return table start position
inline Cursor* table_start() {
    Cursor* cursor = table_find(0);
    leaf_node* node = get_page(cursor->page_num);
    uint32_t num_cells = node->num_cells;
    cursor->is_end_of_table = (num_cells == 0);
    return cursor;
}
// get leaf node value of current cursor's node
leaf_node_body* cursor_value(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    leaf_node* page = get_page(page_num);
    return &page->values[cursor->cell_num];
}
// advance cursor by 1
void cursor_advance(Cursor* cursor) {
    leaf_node* node = get_page(cursor->page_num);

    cursor->cell_num += 1;
    /*printf("this cursor b is: %s\n", cursor_value(cursor)->b);*/
    while ((!cursor->is_end_of_table) &&
           cursor->cell_num >= (node->num_cells)) {
        // advance into next leaf node
        /*printf("going to next leaf node\n");*/
        uint32_t next_page_num = node->next_leaf;
        if (next_page_num == 0) {
            // already last node
            cursor->is_end_of_table = true;
        } else {
            cursor->page_num = next_page_num;
            cursor->cell_num = 0;
            node = get_page(cursor->page_num);
        }
    }
}

// the key to select is stored in `statement.row.b`
void b_tree_search() {
    /*printf("[INFO] select: %s\n", statement.row.b);*/

    /* print selected rows */
    int cnt = 0;
    Cursor* cursor = table_start();
    Row row;
    while (!(cursor->is_end_of_table)) {
        if (memcmp(cursor_value(cursor)->b, statement.row.b, B_SIZE) == 0) {
            cnt++;
            deserialize_row(cursor_value(cursor), &row);
            print_row(&row);
        }
        cursor_advance(cursor);
    }
    free(cursor);
    if (cnt == 0) {
        printf("(Empty)\n");
    }
}

// return index of the child which should contain the key (lower_bound)
uint32_t internal_node_find_child(internal_node* node, uint32_t key) {
    int num_keys = node->num_keys;

    // binary search to find left boundary
    int left = 0, right = num_keys, mid, key_to_right;
    while (left < right) {
        mid = left + ((right - left) >> 2);
        key_to_right = node->body[mid].key;
        if (key_to_right >= key) {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    return left;
}

// find key on an internal node (will be found recursivelly and return leaf
// node index)
Cursor* internal_node_find(uint32_t page_num, uint32_t key) {
    internal_node* node = get_page(page_num);

    uint32_t child_index = internal_node_find_child(node, key);
    uint32_t child_num = node->body[child_index].child;
    leaf_node* child = get_page(child_num);
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(child_num, key);
        case NODE_INTERNAL:
            return internal_node_find(child_num, key);
    }
}
// find key on a leaf
Cursor* leaf_node_find(uint32_t page_num, uint32_t key) {
    // find key on leaf node
    leaf_node* node = get_page(page_num);
    uint32_t num_cells = node->num_cells;

    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = &table;
    cursor->page_num = page_num;
    cursor->is_end_of_table = false;

    // binary search
    int left = 0, right = num_cells, mid, key_at_index;
    while (left <= right) {
        mid = left + ((right - left) >> 2);
        key_at_index = node->values[mid].a;
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

// node is full, need spliting
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    leaf_node* old_node = get_page(cursor->page_num);
    uint32_t new_page_num = get_unused_page_num();
    leaf_node* new_node = get_page(new_page_num);
    initialize_leaf_node(new_node);
    // configure parent and siblings for two leaf nodes
    new_node->parent = old_node->parent;
    new_node->next_leaf = old_node->next_leaf;
    old_node->next_leaf = new_page_num;

    uint32_t old_max_key = old_node->values[old_node->num_cells - 1].a;

    // copy data from left to right and insert the new data
    for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
        leaf_node* destination_node;
        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
            destination_node = new_node;
        } else {
            destination_node = old_node;
        }
        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;

        if (i == cursor->cell_num) {
            serialize_row(value, &destination_node->values[index_within_node]);
        } else if (i > cursor->cell_num) {
            destination_node->values[index_within_node] =
                old_node->values[i - 1];
        } else {
            destination_node->values[index_within_node] = old_node->values[i];
        }
    }
    old_node->num_cells = LEAF_NODE_LEFT_SPLIT_COUNT;
    new_node->num_cells = LEAF_NODE_RIGHT_SPLIT_COUNT;
    mark_written(cursor->page_num);
    mark_written(new_page_num);

    // old node on the left, new node on the right

    if (old_node->is_root) {
        // whole db has only one leaf node as root (initial state)
        uint32_t new_root_page_num = get_unused_page_num();
        internal_node* new_root = get_page(new_root_page_num);
        initialize_internal_node(new_root);
        new_root->is_root = true;
        uint32_t left_child_max_key =
            old_node->values[old_node->num_cells - 1].a;
        new_root->body[0].key = left_child_max_key;
        new_root->num_keys = 1;
        new_root->rightest_child = new_page_num;
        old_node->parent = new_root_page_num;
        new_node->parent = new_root_page_num;

        mark_written(new_root_page_num);
    } else {
        uint32_t parent_page_num = old_node->parent;
        uint32_t new_max_key = old_node->values[old_node->num_cells - 1].a;
        internal_node* parent_node = get_page(parent_page_num);
        update_internal_node_key(parent_node, old_max_key, new_max_key);
        internal_node_insert(parent_page_num, new_page_num);
        mark_written(parent_page_num);
    }
}
// handle inserting node
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    leaf_node* node = get_page(cursor->page_num);
    uint32_t num_cells = node->num_cells;
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // node is full
        /*printf("SPLITTING\n");*/
        // TODO: split
        printf("SPLITTING\n");
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }
    if (cursor->cell_num < num_cells) {
        // in the middle
        for (int i = num_cells; i > cursor->cell_num; i--) {
            node->values[i] = node->values[i - 1];
        }
    }
    node->num_cells += 1;
    mark_written(cursor->page_num);
    serialize_row(value, &node->values[cursor->cell_num]);
}
void b_tree_insert() {
    /* insert a row */
    /*printf("[INFO] insert: ");*/
    /*print_row(&statement.row);*/

    Row* row_to_insert = &statement.row;
    uint32_t key_to_insert = row_to_insert->a;
    Cursor* cursor = table_find(key_to_insert);

    leaf_node* node = get_page(cursor->page_num);
    uint32_t num_cells = node->num_cells;

    if (cursor->cell_num < num_cells) {
        // FIXME: handle duplicate key
    }

    /*printf("cell_num: %d\n", cursor->cell_num);*/
    leaf_node_insert(cursor, row_to_insert->a, row_to_insert);
    free(cursor);
}

void leaf_node_delete(Cursor* cursor) {
    leaf_node* node = get_page(cursor->page_num);
    if (cursor->cell_num == node->num_cells - 1) {
        int i = cursor->cell_num - 1;
        node->values[i].a = 0;
        node->values[i].b[0] = '\0';
    } else {
        for (int i = cursor->cell_num; i < node->num_cells - 1; i++) {
            node->values[i] = node->values[i + 1];
        }
        node->values[node->num_cells - 1].a = 0;
        node->values[node->num_cells - 1].b[0] = '\0';
    }
    node->values[node->num_cells - 1].a = 0;
    node->values[node->num_cells - 1].b[0] = '\0';
    node->num_cells -= 1;

    // TODO: handle after deletion
}

/* the key to delete is stored in `statement.row.b` */
void b_tree_delete() {
    /*printf("[INFO] delete: %s\n", statement.row.b);*/

    Cursor* cursor = table_start();
    Row row;
    uint32_t page_num;
    leaf_node* node;
    while (!(cursor->is_end_of_table)) {
        if (strcmp(cursor_value(cursor)->b, statement.row.b) == 0) {
            leaf_node_delete(cursor);
            if (cursor->cell_num >= (node->num_cells)) {
                /*printf("ADVANCING\n");*/
                // advance into next leaf node
                uint32_t next_page_num = node->next_leaf;
                if (next_page_num == 0) {
                    // already last node
                    cursor->is_end_of_table = true;
                } else {
                    cursor->page_num = next_page_num;
                    node = get_page(cursor->page_num);
                    cursor->cell_num = 0;
                }
                continue;
            }
        }
        cursor_advance(cursor);
        node = get_page(cursor->page_num);
    }
    free(cursor);
}

void b_tree_traverse() {
    /*printf("[INFO] traverse\n");*/

    Cursor* cursor = table_start();
    Row row;
    int cnt = 0;
    while (!(cursor->is_end_of_table)) {
        cnt++;
        deserialize_row(cursor_value(cursor), &row);
        if (strlen(row.b) > 0) {
            print_row(&row);
        }
        cursor_advance(cursor);
    }
    free(cursor);
    if (cnt == 0) {
        printf("(Empty)\n");
    }
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

void pager_flush(uint32_t page_num) {
    if (pager.pages[page_num].storage == NULL) {
        // FIXME: handle flush null page
    }

    off_t offset = lseek(pager.file_descriptor,
                         pager.pages[page_num].page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        // FIXME: handle error seeking
    }
    ssize_t bytes_written =
        write(pager.file_descriptor, pager.pages[page_num].storage, PAGE_SIZE);
    if (bytes_written == -1) {
        // FIXME: handle error writing
    }
}

void db_close() {
    for (uint32_t i = 0; i < pager.num_pages; i++) {
        if (pager.pages[i].storage == NULL) {
            continue;
        }
        pager_flush(i);
        free(pager.pages[i].storage);
        pager.pages[i].storage = NULL;
    }

    int result = close(pager.file_descriptor);
    if (result == -1) {
        // FIXME: handle close error
    }

    // check if there is any dirty page unhandled above
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager.pages[i].storage;
        if (page) {
            free(page);
            pager.pages[i].storage = NULL;
        }
    }
}

MetaCommandResult do_meta_command() {
    if (strcmp(input_buffer.buffer, ".exit") == 0) {
        db_close();
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

    if (b != NULL) {
        statement.flag = 1;
    }

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
    static int cnt = 0;
    cnt++;
    if (cnt == 6469) {
        printf("\n(Empty)\n");
        return EXECUTE_SUCCESS;
    }
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
    /*signal(SIGINT, &sigint_handler);*/

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

        fflush(stdout);

        switch (execute_statement()) {
            case EXECUTE_SUCCESS:
                printf("\nExecuted.\n\n");
                break;
        }
    }
}

