import duckdb
import os
import csv
from decimal import Decimal, ROUND_HALF_UP


def process_tpch_q3(tbl_path, query_path, output_csv_path):
    """
    Read tpch_q3.tbl to execute the query and compare the results with output.csv
    :param tbl_path: Path to the tpch_q3.tbl file
    :param query_path: Path to the query.sql file
    :param output_csv_path: Path to the output CSV file
    """
    # Connect to DuckDB (in-memory mode)
    con = duckdb.connect(database=':memory:')

    try:
        # Create table structure and load data
        create_tables(con)
        load_tpch_q3_data(con, tbl_path)

        # Execute the query and get the results
        query_result = execute_query(con, query_path)

        # Read the results from output.csv
        csv_result = read_output_csv(output_csv_path)

        # Compare the results
        compare_results(query_result, csv_result)

    except Exception as e:
        print(f"Execution error: {e}")
    finally:
        con.close()


def create_tables(con):
    """Create the TPCH table structure"""
    # Customer table
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

    # Orders table
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

    # Lineitem table
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
    print("Table structure creation completed")


def load_tpch_q3_data(con, tbl_path):
    """Read tpch_q3.tbl and process INSERT/DELETE operations"""
    if not os.path.exists(tbl_path):
        raise FileNotFoundError(f"File {tbl_path} does not exist")

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
                print(f"Warning: Line {line_num} has incorrect format, skipping - {line}")
                continue

            operation = parts[0].upper()
            table_name = parts[1].lower()

            if table_name not in table_field_counts:
                print(f"Warning: Line {line_num} has unsupported table name {table_name}, skipping")
                continue

            fields = parts[2:]
            if len(fields) != table_field_counts[table_name]:
                print(
                    f"Warning: Line {line_num} has mismatched field count, skipping - Expected {table_field_counts[table_name]}, got {len(fields)}")
                continue

            if operation == "INSERT":
                placeholders = ', '.join(['?' for _ in fields])
                con.execute(f"""
                    INSERT INTO {table_name} VALUES ({placeholders})
                """, fields)

            # Process DELETE operations
            elif operation == "DELETE":
                if table_name == "customer":
                    con.execute(f"DELETE FROM {table_name} WHERE c_custkey = ?", [fields[0]])
                elif table_name == "orders":
                    con.execute(f"DELETE FROM {table_name} WHERE o_orderkey = ?", [fields[0]])
                elif table_name == "lineitem":
                    con.execute(f"DELETE FROM {table_name} WHERE l_orderkey = ? AND l_linenumber = ?",
                                [fields[0], fields[3]])
            else:
                print(f"Warning: Line {line_num} has unsupported operation type {operation}, skipping")


def execute_query(con, query_path):
    """Execute the query and return formatted results"""
    if not os.path.exists(query_path):
        raise FileNotFoundError(f"Query file {query_path} does not exist")

    with open(query_path, 'r') as f:
        query = f.read()

    print("\nExecution results:")
    result = con.execute(query).fetchall()

    # Format the results: (l_orderkey, o_orderdate, o_shippriority, revenue)
    # Convert date to string and format revenue to 4 decimal places
    formatted = []
    for row in result:
        orderkey = row[0]
        orderdate = str(row[1])  # Convert to 'YYYY-MM-DD' string
        shippriority = row[2]
        revenue = Decimal(str(row[3])).quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP)
        formatted.append((orderkey, orderdate, shippriority, revenue))

    return set(formatted)  # Use a set for unordered comparison


def read_output_csv(csv_path):
    """Read output.csv and return formatted results"""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Output file {csv_path} does not exist")

    formatted = []
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header

        for line_num, row in enumerate(reader, 2):  # Line numbers start from 2 (header is 1)
            if not row:
                continue

            # Handle possible spaces (e.g., "48899, 1995-01-19, 0, 13272.0672")
            cleaned = [col.strip() for col in row]
            if len(cleaned) != 4:
                print(f"Warning: Line {line_num} in CSV has incorrect format, skipping - {row}")
                continue

            try:
                orderkey = int(cleaned[0])
                orderdate = cleaned[1]
                shippriority = int(cleaned[2])
                revenue = Decimal(cleaned[3]).quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP)
                formatted.append((orderkey, orderdate, shippriority, revenue))
            except (ValueError, Decimal.InvalidOperation) as e:
                print(f"Warning: Data conversion failed for line {line_num} in CSV, skipping - {row}, Error: {e}")

    return set(formatted)


def compare_results(query_set, csv_set):
    """Compare query results with CSV results"""
    # Calculate differences
    only_query = query_set - csv_set
    only_csv = csv_set - query_set

    # Output comparison results
    print("\n===== Result Comparison =====")
    print(f"Number of query results: {len(query_set)}")
    print(f"Number of CSV file entries: {len(csv_set)}")
    print(f"Number of matching entries: {len(query_set & csv_set)}")

    if not only_query and not only_csv:
        print("✅ Results are identical")
        return

    if only_query:
        print(f"\n❌ Records only in query results ({len(only_query)} entries):")
        for item in sorted(only_query):  # Sort before output for easier viewing
            print(f"  {item}")

    if only_csv:
        print(f"\n❌ Records only in CSV file ({len(only_csv)} entries):")
        for item in sorted(only_csv):  # Sort before output for easier viewing
            print(f"  {item}")


if __name__ == "__main__":
    # Configure file paths
    TPCH_Q3_TBL_PATH = "./data/tpch_q3.tbl"
    QUERY_FILE = "./query3.sql"
    OUTPUT_CSV_PATH = "./data/output.csv"  # New: Path to output.csv

    process_tpch_q3(TPCH_Q3_TBL_PATH, QUERY_FILE, OUTPUT_CSV_PATH)