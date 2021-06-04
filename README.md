
compile: 

```bash
gcc myjql.c
```



leaf node: 

| name        | size(byte) |
| ---         | ---        |
| node_type   | 4          |
| is_root     | 4          |
| parent_node | 4          |
| num_cells   | 4          |
| next_leaf   | 4          |
|             |            |
| key   (a)   | 4          |
| value (b)   | row size   |

internal node:

| name        | size(byte) |
| ---         | ---        |
| node_type   | 4          |
| is_root     | 4          |
| parent_node | 4          |
| num_keys    | 4          |
| right_child | 4          |
|             |            |
| child       | 4          |
| key         | 4          |



### Original bug

原来的 gcc 版本过高，gcc-7.5 不支持 const 时进行运算，现在已经修复。
