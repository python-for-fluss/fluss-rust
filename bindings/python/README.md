# Fluss Python Bindings

Python bindings for Fluss using PyO3 and Maturin.

## 系统要求

- Python 3.8+
- Rust 1.70+
- Maturin (`pip install maturin`)

## 开发环境设置

### 1. 安装 Maturin

```bash
pip install maturin
```

### 2. 构建开发版本

```bash
cd bindings/python
maturin develop
```

### 3. 构建发布版本

```bash
maturin build --release
```

### 4. 运行示例

```bash
python example/example.py
```

## 项目结构

```
bindings/python/
├── Cargo.toml              # Rust 依赖配置
├── pyproject.toml          # Python 项目配置
├── README.md              # 本文件
├── src/                   # Rust 源代码
│   ├── lib.rs            # 主入口模块
│   ├── config.rs         # 配置相关
│   ├── connection.rs     # 连接管理
│   ├── admin.rs          # 管理操作
│   ├── table.rs          # 表操作
│   ├── types.rs          # 数据类型
│   └── error.rs          # 错误处理
├── python/               # Python 包源码
│   └── fluss_python/
│       ├── __init__.py   # Python 包入口
│       └── py.typed      # 类型声明
└── example/              # 示例代码
    └── example.py
```

## API 概述

### 基本用法

```python
import fluss_python as fluss

# 创建配置
config = fluss.Config("127.0.0.1:9123", 30000)

# 建立连接
with fluss.FlussConnection(config) as conn:
    # 获取管理客户端
    admin = conn.get_admin()
    
    # 创建表路径
    table_path = fluss.TablePath("my_database", "my_table")
    
    # 创建表
    admin.create_table(table_path, schema, ignore_if_exists=True)
    
    # 获取表
    table = conn.get_table(table_path)
    
    # 写入数据
    with table.new_append() as writer:
        writer.write_pandas(df)
    
    # 读取数据
    scanner = table.new_log_scanner()
    result = scanner.scan_earliest(end_timestamp)
    df = result.to_pandas()
```

### 核心类

#### `Config`
配置 Fluss 连接参数

#### `FlussConnection`
连接到 Fluss 集群的主要接口

#### `FlussAdmin`
管理表的创建、删除等操作

#### `FlussTable`
表示一个 Fluss 表，提供读写操作

#### `TableWriter`
用于向表写入数据，支持 PyArrow 和 Pandas

#### `LogScanner`
用于扫描表的日志数据

#### `ScanResult`
扫描结果，支持转换为 PyArrow、Pandas 或 DuckDB

## 开发指南

### 添加新功能

1. 在相应的 Rust 模块中实现功能
2. 使用 `#[pyclass]` 和 `#[pymethods]` 暴露给 Python
3. 在 `lib.rs` 的 `fluss_python` 模块中注册新类
4. 更新 Python 类型声明文件 `py.typed`
5. 添加测试和示例

### 测试

```bash
# 构建并安装到开发环境
maturin develop

# 运行测试
python -m pytest tests/

# 运行示例
python example/example.py
```

### 发布

```bash
# 构建 wheel
maturin build --release

# 发布到 PyPI
maturin publish
```

## 待实现功能

- [ ] 实际的 Fluss 客户端集成
- [ ] PyArrow 表的读写支持
- [ ] 异步操作支持 
- [ ] 错误处理完善
- [ ] 完整的测试套件
- [ ] 性能优化
- [ ] 文档生成

## 许可证

Apache 2.0 License
