import duckdb
import os


def process_tpch_q3(tbl_path, query_path):
    """
    读取tpch_q3.tbl文件（含INSERT/DELETE操作）并执行查询
    :param tbl_path: tpch_q3.tbl文件路径
    :param query_path: query.sql文件路径
    """
    # 连接DuckDB（可改为文件路径持久化）
    con = duckdb.connect(database=':memory:')

    try:
        # 创建TPCH所需表结构
        create_tables(con)

        # 读取并处理tpch_q3.tbl数据
        load_tpch_q3_data(con, tbl_path)

        # 执行查询并输出结果
        execute_query(con, query_path)

    except Exception as e:
        print(f"执行出错: {e}")
    finally:
        con.close()


def create_tables(con):
    """创建TPCH表结构（与之前保持一致）"""
    # 客户表
    con.execute("""
                CREATE TABLE customer
                (
                    c_custkey    INT,
                    c_name       VARCHAR,
                    c_address    VARCHAR,
                    c_nationkey  INT,
                    c_phone      VARCHAR,
                    c_acctbal    DECIMAL(12, 2),
                    c_mktsegment VARCHAR,
                    c_comment    VARCHAR
                )
                """)

    # 订单表
    con.execute("""
                CREATE TABLE orders
                (
                    o_orderkey      INT,
                    o_custkey       INT,
                    o_orderstatus   CHAR(1),
                    o_totalprice    DECIMAL(12, 2),
                    o_orderdate     DATE,
                    o_orderpriority VARCHAR,
                    o_clerk         VARCHAR,
                    o_shippriority  INT,
                    o_comment       VARCHAR
                )
                """)

    # 订单项表
    con.execute("""
                CREATE TABLE lineitem
                (
                    l_orderkey      INT,
                    l_partkey       INT,
                    l_suppkey       INT,
                    l_linenumber    INT,
                    l_quantity      DECIMAL(12, 2),
                    l_extendedprice DECIMAL(12, 2),
                    l_discount      DECIMAL(12, 2),
                    l_tax           DECIMAL(12, 2),
                    l_returnflag    CHAR(1),
                    l_linestatus    CHAR(1),
                    l_shipdate      DATE,
                    l_commitdate    DATE,
                    l_receiptdate   DATE,
                    l_shipinstruct  VARCHAR,
                    l_shipmode      VARCHAR,
                    l_comment       VARCHAR
                )
                """)
    print("表结构创建完成")


def load_tpch_q3_data(con, tbl_path):
    """读取tpch_q3.tbl并处理INSERT/DELETE操作"""
    if not os.path.exists(tbl_path):
        raise FileNotFoundError(f"文件 {tbl_path} 不存在")

    # 定义表结构字段数量映射（用于校验）
    table_field_counts = {
        "customer": 8,  # c_custkey到c_comment共8个字段
        "orders": 9,  # o_orderkey到o_comment共9个字段
        "lineitem": 16  # l_orderkey到l_comment共16个字段
    }

    with open(tbl_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            # 按|分割字段（格式：操作类型|表名|字段1|字段2|...）
            parts = line.split('|')
            if len(parts) < 3:
                print(f"警告：第{line_num}行格式错误，跳过 - {line}")
                continue

            operation = parts[0].upper()
            table_name = parts[1].lower()

            # 校验表名和字段数量
            if table_name not in table_field_counts:
                print(f"警告：第{line_num}行不支持的表名 {table_name}，跳过")
                continue

            fields = parts[2:]  # 提取实际数据字段
            if len(fields) != table_field_counts[table_name]:
                print(
                    f"警告：第{line_num}行字段数量不匹配，跳过 - 预期{table_field_counts[table_name]}个，实际{len(fields)}个")
                continue

            # 处理INSERT操作
            if operation == "INSERT":
                # 构建参数化插入语句（避免SQL注入风险）
                placeholders = ', '.join(['?' for _ in fields])
                con.execute(f"""
                    INSERT INTO {table_name} VALUES ({placeholders})
                """, fields)

            # 处理DELETE操作
            elif operation == "DELETE":
                # 对于DELETE，使用主键字段构建条件（根据TPCH主键定义）
                if table_name == "customer":
                    # 主键：c_custkey（第一个字段）
                    con.execute(f"DELETE FROM {table_name} WHERE c_custkey = ?", [fields[0]])
                elif table_name == "orders":
                    # 主键：o_orderkey（第一个字段）
                    con.execute(f"DELETE FROM {table_name} WHERE o_orderkey = ?", [fields[0]])
                elif table_name == "lineitem":
                    # 主键：l_orderkey + l_linenumber（第一个和第四个字段）
                    con.execute(f"DELETE FROM {table_name} WHERE l_orderkey = ? AND l_linenumber = ?",
                                [fields[0], fields[3]])

            else:
                print(f"警告：第{line_num}行不支持的操作类型 {operation}，跳过")


def execute_query(con, query_path):
    """执行query.sql并输出结果"""
    if not os.path.exists(query_path):
        print(f"错误：查询文件 {query_path} 不存在")
        return

    with open(query_path, 'r') as f:
        query = f.read()

    print("\n执行查询结果：")
    result = con.execute(query).fetchall()

    # 获取列名
    columns = [desc[0] for desc in con.description]
    print(columns)

    # 打印所有结果
    for row in result:
        print(row)

if __name__ == "__main__":
    # 配置文件路径（根据实际情况修改）
    TPCH_Q3_TBL_PATH = "./data/tpch_q3.tbl"  # tpch_q3.tbl文件路径
    QUERY_FILE = "./query3.sql"              # 查询文件路径

    process_tpch_q3(TPCH_Q3_TBL_PATH, QUERY_FILE)