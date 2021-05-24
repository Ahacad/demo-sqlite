#include <stdbool.h>
#include <stdint.h>

#define COLUMN_B_SIZE 11
#define TABLE_MAX_PAGES 500
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
const uint32_t PAGE_SIZE = 4096;

#define LEAF_NODE_MAX_CELLS 250
#define INTERNAL_NODE_MAX_CELLS 500
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;
// FIXME:
const uint32_t INTERNAL_NODE_LEFT_SPLIT_SIZE =
    (INTERNAL_NODE_MAX_CELLS + 1) / 2;
const uint32_t INTERNAL_NODE_RIGHT_SPLIT_SIZE =
    (INTERNAL_NODE_MAX_CELLS + 1) - INTERNAL_NODE_LEFT_SPLIT_SIZE - 1;

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
} Pager;
typedef struct {
    Pager pager;
    uint32_t root_page_num;
} Table;
typedef struct {
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool is_end_of_table;
} Cursor;
typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;
// FIXME: test whether 500 is enough
// the table struct specified in the PJ
typedef struct {
    uint32_t a;
    char b[COLUMN_B_SIZE + 1];
} Row;
typedef struct {
    uint32_t a;
    char b[COLUMN_B_SIZE + 1];
} leaf_node_body;
typedef struct {
    NodeType node_type;
    bool is_root;
    uint32_t parent;
    //
    uint32_t num_cells;
    uint32_t next_leaf;
    leaf_node_body values[LEAF_NODE_MAX_CELLS];
} leaf_node;

typedef struct {
    uint32_t child;
    uint32_t key;
} internal_node_body;
typedef struct {
    NodeType node_type;
    bool is_root;
    uint32_t parent;
    uint32_t num_keys;
    internal_node_body body[INTERNAL_NODE_MAX_CELLS];
    uint32_t rightest_child;
} internal_node;

Cursor* leaf_node_find(uint32_t page_num, uint32_t key);
Cursor* internal_node_find(uint32_t page_num, uint32_t key);
uint32_t internal_node_find_child(internal_node* node, uint32_t key);

void* get_page(uint32_t page_num);
void pager_open(const char* filename);
void print_row(Row* row);
Cursor* table_find(uint32_t key);
Cursor* table_start();
NodeType get_node_type(void* node);
void serialize_row(Row* source, leaf_node_body* destination);
void deserialize_row(leaf_node_body* source, Row* destination);
uint32_t get_unused_page_num();
void initialize_leaf_node(leaf_node* node);
void initialize_internal_node(internal_node* node);
void internal_node_split(uint32_t page_num);

