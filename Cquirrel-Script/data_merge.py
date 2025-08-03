import random

def read_lines(file_name, limit=None):
    with open(file_name, 'r') as f:
        lines = f.readlines()
    return lines[:limit] if limit else lines

def generate_q3_input():
    # 读取TPC-H原始表数据
    customer_lines = read_lines('./data/customer.tbl')
    orders_lines = read_lines('./data/orders.tbl', 100000)  # 限制订单数据量
    lineitem_lines = read_lines('./data/lineitem.tbl', 500000)

    # 生成带操作符的输入数据（INSERT/DELETE）
    output = []
    for line in customer_lines:
        output.append(f"INSERT|customer|{line.strip()}")
    for line in orders_lines:
        output.append(f"INSERT|orders|{line.strip()}")
    for line in lineitem_lines:
        output.append(f"INSERT|lineitem|{line.strip()}")

    # 随机插入一些DELETE操作（模拟数据变更）
    random.shuffle(output)
    for i in range(1000):  # 随机删除1000条记录
        idx = random.randint(0, len(output)-1)
        output[idx] = output[idx].replace("INSERT", "DELETE")

    # 写入输出文件
    with open('./data/tpch_q3.tbl', 'w') as f:
        f.write('\n'.join(output))

if __name__ == '__main__':
    generate_q3_input()
    print("Q3 input data generated: tpch_q3.tbl")