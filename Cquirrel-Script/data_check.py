import duckdb
import os
import csv
from decimal import Decimal, ROUND_HALF_UP


def process_tpch_q3(tbl_path, query_path, output_csv_path):
    """
    读取tpch_q3.tbl执行查询，与output.csv对比结果
    :param tbl_path: tpch_q3.tbl文件路径
    :param query_path: query.sql文件路径
    :param output_csv_path: 输出结果CSV文件路径
    """
    # 连接DuckDB（内存模式）
    con = duckdb.connect(database=':memory:')

    try:
        # 创建表结构并加载数据
        create_tables(con)
        load_tpch_q3_data(con, tbl_path)

        # 执行查询并获取结果
        query_result = execute_query(con, query_path)

        # 读取output.csv结果
        csv_result = read_output_csv(output_csv_path)

        # 比较结果
        compare_results(query_result, csv_result)

    except Exception as e:
        print(f"执行出错: {e}")
    finally:
        con.close()


def create_tables(con):
    """创建TPCH表结构"""
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

    table_field_counts = {
        "customer": 8,
        "orders": 9,
        "lineitem": 16
    }

    with open(tbl_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            parts = line.split('|')
            if len(parts) < 3:
                print(f"警告：第{line_num}行格式错误，跳过 - {line}")
                continue

            operation = parts[0].upper()
            table_name = parts[1].lower()

            if table_name not in table_field_counts:
                print(f"警告：第{line_num}行不支持的表名 {table_name}，跳过")
                continue

            fields = parts[2:]
            if len(fields) != table_field_counts[table_name]:
                print(
                    f"警告：第{line_num}行字段数量不匹配，跳过 - 预期{table_field_counts[table_name]}个，实际{len(fields)}个")
                continue

            if operation == "INSERT":
                placeholders = ', '.join(['?' for _ in fields])
                con.execute(f"""
                    INSERT INTO {table_name} VALUES ({placeholders})
                """, fields)

            # 处理DELETE操作
            elif operation == "DELETE":
                if table_name == "customer":
                    con.execute(f"DELETE FROM {table_name} WHERE c_custkey = ?", [fields[0]])
                elif table_name == "orders":
                    con.execute(f"DELETE FROM {table_name} WHERE o_orderkey = ?", [fields[0]])
                elif table_name == "lineitem":
                    con.execute(f"DELETE FROM {table_name} WHERE l_orderkey = ? AND l_linenumber = ?",
                                [fields[0], fields[3]])
            else:
                print(f"警告：第{line_num}行不支持的操作类型 {operation}，跳过")


def execute_query(con, query_path):
    """执行查询并返回格式化结果"""
    if not os.path.exists(query_path):
        raise FileNotFoundError(f"查询文件 {query_path} 不存在")

    with open(query_path, 'r') as f:
        query = f.read()

    print("\n执行查询结果：")
    result = con.execute(query).fetchall()

    # 格式化结果：(l_orderkey, o_orderdate, o_shippriority, revenue)
    # 日期转为字符串，revenue保留4位小数
    formatted = []
    for row in result:
        orderkey = row[0]
        orderdate = str(row[1])  # 转为'YYYY-MM-DD'字符串
        shippriority = row[2]
        revenue = Decimal(str(row[3])).quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP)
        formatted.append((orderkey, orderdate, shippriority, revenue))

    return set(formatted)  # 用集合实现无序比较


def read_output_csv(csv_path):
    """读取output.csv并返回格式化结果"""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"输出文件 {csv_path} 不存在")

    formatted = []
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # 跳过表头

        for line_num, row in enumerate(reader, 2):  # 行号从2开始（表头为1）
            if not row:
                continue

            # 处理可能的空格（如"48899, 1995-01-19, 0, 13272.0672"）
            cleaned = [col.strip() for col in row]
            if len(cleaned) != 4:
                print(f"警告：CSV第{line_num}行格式错误，跳过 - {row}")
                continue

            try:
                orderkey = int(cleaned[0])
                orderdate = cleaned[1]
                shippriority = int(cleaned[2])
                revenue = Decimal(cleaned[3]).quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP)
                formatted.append((orderkey, orderdate, shippriority, revenue))
            except (ValueError, Decimal.InvalidOperation) as e:
                print(f"警告：CSV第{line_num}行数据转换失败，跳过 - {row}，错误：{e}")

    return set(formatted)


def compare_results(query_set, csv_set):
    """比较查询结果和CSV结果"""
    # 计算差异
    only_query = query_set - csv_set
    only_csv = csv_set - query_set

    # 输出比较结果
    print("\n===== 结果比较 =====")
    print(f"查询结果条数: {len(query_set)}")
    print(f"CSV文件条数: {len(csv_set)}")
    print(f"匹配条数: {len(query_set & csv_set)}")

    if not only_query and not only_csv:
        print("✅ 结果完全一致")
        return

    if only_query:
        print(f"\n❌ 仅查询结果存在的记录 ({len(only_query)}条):")
        for item in sorted(only_query):  # 排序后输出，便于查看
            print(f"  {item}")

    if only_csv:
        print(f"\n❌ 仅CSV文件存在的记录 ({len(only_csv)}条):")
        for item in sorted(only_csv):  # 排序后输出，便于查看
            print(f"  {item}")


if __name__ == "__main__":
    # 配置文件路径
    TPCH_Q3_TBL_PATH = "./data/tpch_q3.tbl"
    QUERY_FILE = "./query3.sql"
    OUTPUT_CSV_PATH = "./data/output.csv"  # 新增：output.csv路径

    process_tpch_q3(TPCH_Q3_TBL_PATH, QUERY_FILE, OUTPUT_CSV_PATH)