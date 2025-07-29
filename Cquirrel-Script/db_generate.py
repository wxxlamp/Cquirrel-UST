import duckdb


def generate_tpch_tbl(scale_factor=1.0, output_dir="."):
    """
    Generate TPC-H tables and export them as .tbl files
    :param scale_factor: Data scale factor (sf=1 corresponds to approximately 1GB of data)
    :param output_dir: Directory for output .tbl files
    """
    # Connect to DuckDB (in-memory mode, data not retained after exit)
    con = duckdb.connect(database=':memory:')

    try:
        # Install and load TPC-H extension
        con.sql("INSTALL tpch;")
        con.sql("LOAD tpch;")

        # Generate TPC-H data (based on scale_factor)
        print(f"Starting to generate TPC-H data with scale factor: {scale_factor}...")
        con.sql(f"CALL dbgen(sf={scale_factor});")
        print("TPC-H data generation completed")

        # List of tables to export (corresponding to those used in the code)
        tables = [
            "customer",
            "lineitem",
            "nation",
            "orders",
            "supplier"
        ]

        # Export each table as a .tbl file (pipe-separated, no header)
        for table in tables:
            output_path = f"{output_dir}/{table}.tbl"
            # Original TPC-H format uses pipe delimiter with an extra pipe at the end of each line; simulate this format
            con.sql(f"""
                COPY (
                    SELECT * FROM {table}
                ) TO '{output_path}' WITH (
                    FORMAT CSV,
                    DELIMITER '|',
                    HEADER FALSE,
                    QUOTE ''  # Disable quotes to avoid escaping special characters in fields
                );
            """)
            print(f"Exported table {table} to {output_path}")

    finally:
        # Close the connection
        con.close()


if __name__ == "__main__":
    # Generate small-scale data (sf=0.1 is approximately 100MB) and output to current directory
    generate_tpch_tbl(scale_factor=0.1, output_dir=".")