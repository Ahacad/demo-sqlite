

leaf node: 

| name        | size(bype) |
| ---         | ---        |
| node_type   | 1          |
| is_root     | 1          |
| parent_node | 4          |
| num_cells   | 4          |
| next_leaf   | 4          |
|             |            |
| key         | 4          |
| value       | row size   |

internal node:

| name        | size(bype) |
| ---         | ---        |
| node_type   | 1          |
| is_root     | 1          |
| parent_node | 4          |
| num_keys    | 4          |
| right_child | 4          |
|             |            |
| key         | 4          |
| child       | 4          |
