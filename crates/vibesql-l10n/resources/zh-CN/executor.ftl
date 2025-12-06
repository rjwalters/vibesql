# VibeSQL 执行器错误消息 - 简体中文
# 此文件包含 vibesql-executor crate 的所有错误消息。

# =============================================================================
# 表错误
# =============================================================================

executor-table-not-found = 未找到表 '{ $name }'
executor-table-already-exists = 表 '{ $name }' 已存在

# =============================================================================
# 列错误
# =============================================================================

executor-column-not-found-simple = 在表 '{ $table_name }' 中未找到列 '{ $column_name }'
executor-column-not-found-searched = 未找到列 '{ $column_name }'（已搜索的表：{ $searched_tables }）
executor-column-not-found-with-available = 未找到列 '{ $column_name }'（已搜索的表：{ $searched_tables }）。可用列：{ $available_columns }
executor-invalid-table-qualifier = 列 '{ $column }' 的表限定符 '{ $qualifier }' 无效。可用表：{ $available_tables }
executor-column-already-exists = 列 '{ $name }' 已存在
executor-column-index-out-of-bounds = 列索引 { $index } 越界

# =============================================================================
# 索引错误
# =============================================================================

executor-index-not-found = 未找到索引 '{ $name }'
executor-index-already-exists = 索引 '{ $name }' 已存在
executor-invalid-index-definition = 无效的索引定义：{ $message }

# =============================================================================
# 触发器错误
# =============================================================================

executor-trigger-not-found = 未找到触发器 '{ $name }'
executor-trigger-already-exists = 触发器 '{ $name }' 已存在

# =============================================================================
# 模式错误
# =============================================================================

executor-schema-not-found = 未找到模式 '{ $name }'
executor-schema-already-exists = 模式 '{ $name }' 已存在
executor-schema-not-empty = 无法删除模式 '{ $name }'：模式不为空

# =============================================================================
# 角色和权限错误
# =============================================================================

executor-role-not-found = 未找到角色 '{ $name }'
executor-permission-denied = 权限被拒绝：角色 '{ $role }' 对 { $object } 没有 { $privilege } 权限
executor-dependent-privileges-exist = 存在依赖权限：{ $message }

# =============================================================================
# 类型错误
# =============================================================================

executor-type-not-found = 未找到类型 '{ $name }'
executor-type-already-exists = 类型 '{ $name }' 已存在
executor-type-in-use = 无法删除类型 '{ $name }'：类型仍在使用中
executor-type-mismatch = 类型不匹配：{ $left } { $op } { $right }
executor-type-error = 类型错误：{ $message }
executor-cast-error = 无法将 { $from_type } 转换为 { $to_type }
executor-type-conversion-error = 无法将 { $from } 转换为 { $to }

# =============================================================================
# 表达式和查询错误
# =============================================================================

executor-division-by-zero = 除以零
executor-invalid-where-clause = 无效的 WHERE 子句：{ $message }
executor-unsupported-expression = 不支持的表达式：{ $message }
executor-unsupported-feature = 不支持的功能：{ $message }
executor-parse-error = 解析错误：{ $message }

# =============================================================================
# 子查询错误
# =============================================================================

executor-subquery-returned-multiple-rows = 标量子查询返回了 { $actual } 行，预期为 { $expected } 行
executor-subquery-column-count-mismatch = 子查询返回了 { $actual } 列，预期为 { $expected } 列
executor-column-count-mismatch = 派生列列表有 { $provided } 列，但查询生成了 { $expected } 列

# =============================================================================
# 约束错误
# =============================================================================

executor-constraint-violation = 违反约束：{ $message }
executor-multiple-primary-keys = 不允许多个 PRIMARY KEY 约束
executor-cannot-drop-column = 无法删除列：{ $message }
executor-constraint-not-found = 在表 '{ $table_name }' 中未找到约束 '{ $constraint_name }'

# =============================================================================
# 资源限制错误
# =============================================================================

executor-expression-depth-exceeded = 超出表达式深度限制：{ $depth } > { $max_depth }（防止栈溢出）
executor-query-timeout-exceeded = 超出查询超时时间：{ $elapsed_seconds } 秒 > { $max_seconds } 秒
executor-row-limit-exceeded = 超出行处理限制：{ $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = 超出内存限制：{ $used_gb } GB > { $max_gb } GB

# =============================================================================
# 过程/变量错误
# =============================================================================

executor-variable-not-found-simple = 未找到变量 '{ $variable_name }'
executor-variable-not-found-with-available = 未找到变量 '{ $variable_name }'。可用变量：{ $available_variables }
executor-label-not-found = 未找到标签 '{ $name }'

# =============================================================================
# SELECT INTO 错误
# =============================================================================

executor-select-into-row-count = 过程式 SELECT INTO 必须返回恰好 { $expected } 行，实际返回了 { $actual } 行{ $plural }
executor-select-into-column-count = 过程式 SELECT INTO 列数不匹配：{ $expected } 个变量{ $expected_plural }，但查询返回了 { $actual } 列{ $actual_plural }

# =============================================================================
# 过程和函数错误
# =============================================================================

executor-procedure-not-found-simple = 在模式 '{ $schema_name }' 中未找到过程 '{ $procedure_name }'
executor-procedure-not-found-with-available = 在模式 '{ $schema_name }' 中未找到过程 '{ $procedure_name }'
    .available = 可用过程：{ $available_procedures }
executor-procedure-not-found-with-suggestion = 在模式 '{ $schema_name }' 中未找到过程 '{ $procedure_name }'
    .available = 可用过程：{ $available_procedures }
    .suggestion = 您是否想输入 '{ $suggestion }'？

executor-function-not-found-simple = 在模式 '{ $schema_name }' 中未找到函数 '{ $function_name }'
executor-function-not-found-with-available = 在模式 '{ $schema_name }' 中未找到函数 '{ $function_name }'
    .available = 可用函数：{ $available_functions }
executor-function-not-found-with-suggestion = 在模式 '{ $schema_name }' 中未找到函数 '{ $function_name }'
    .available = 可用函数：{ $available_functions }
    .suggestion = 您是否想输入 '{ $suggestion }'？

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' 需要 { $expected } 个参数{ $expected_plural }（{ $parameter_signature }），实际传入了 { $actual } 个参数{ $actual_plural }
executor-parameter-type-mismatch = 参数 '{ $parameter_name }' 需要类型 { $expected_type }，实际传入了类型 { $actual_type } 的值 '{ $actual_value }'
executor-argument-count-mismatch = 参数数量不匹配：预期 { $expected } 个，实际 { $actual } 个

executor-recursion-limit-exceeded = 超出最大递归深度（{ $max_depth }）：{ $message }
executor-recursion-call-stack = 调用栈：
executor-function-must-return = 函数必须返回值
executor-invalid-control-flow = 无效的控制流：{ $message }
executor-invalid-function-body = 无效的函数体：{ $message }
executor-function-read-only-violation = 函数只读约束违规：{ $message }

# =============================================================================
# EXTRACT 错误
# =============================================================================

executor-invalid-extract-field = 无法从 { $value_type } 类型的值中提取 { $field }

# =============================================================================
# 列式/Arrow 错误
# =============================================================================

executor-arrow-downcast-error = 无法将 Arrow 数组转换为 { $expected_type }（{ $context }）
executor-columnar-type-mismatch-binary = { $operation } 的类型不兼容：{ $left_type } 与 { $right_type }
executor-columnar-type-mismatch-unary = { $operation } 的类型不兼容：{ $left_type }
executor-simd-operation-failed = SIMD { $operation } 失败：{ $reason }
executor-columnar-column-not-found = 列索引 { $column_index } 越界（批次有 { $batch_columns } 列）
executor-columnar-column-not-found-by-name = 未找到列：{ $column_name }
executor-columnar-length-mismatch = { $context } 中列长度不匹配：预期 { $expected }，实际 { $actual }
executor-unsupported-array-type = { $operation } 不支持的数组类型：{ $array_type }

# =============================================================================
# 空间错误
# =============================================================================

executor-spatial-geometry-error = { $function_name }：{ $message }
executor-spatial-operation-failed = { $function_name }：{ $message }
executor-spatial-argument-error = { $function_name } 需要 { $expected }，实际传入 { $actual }

# =============================================================================
# 游标错误
# =============================================================================

executor-cursor-already-exists = 游标 '{ $name }' 已存在
executor-cursor-not-found = 未找到游标 '{ $name }'
executor-cursor-already-open = 游标 '{ $name }' 已打开
executor-cursor-not-open = 游标 '{ $name }' 未打开
executor-cursor-not-scrollable = 游标 '{ $name }' 不可滚动（未指定 SCROLL）

# =============================================================================
# 存储和一般错误
# =============================================================================

executor-storage-error = 存储错误：{ $message }
executor-other = { $message }
