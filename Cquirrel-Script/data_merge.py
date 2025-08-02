import random

def read_all_lines(file_name):
    file_name = './data/' + file_name
    with open(file_name, 'r') as file:
        lines = file.readlines()
    return lines

def write_all_lines(file_names1,file_names2, output_file):
    rows1 = []

    for file_name in file_names1:
        lines = read_all_lines(file_name)
        file_prefix = file_name.replace('.tbl', '')
        for i, line in enumerate(lines):
            rows1.append(f"{file_prefix}|{line.strip()}\n")
    rows2 = []
    for file_name in file_names2:
        lines = read_all_lines(file_name)
        file_prefix = file_name.replace('.tbl', '')
        for i, line in enumerate(lines):
            if i == 500000:
                break
            rows2.append(f"{file_prefix}|{line.strip()}\n")
    random.shuffle(rows2)
    with open(output_file, 'w') as output:
        for row in rows1:
            output.write(row)
        for row in rows2:
            output.write(row)


def main():
    file_names1 = [
        'customer.tbl',
        'nation.tbl',
        'supplier.tbl'
    ]

    file_names2 = [
        'lineitem.tbl',
        'orders.tbl',
    ]

    output_file = './data/output_large.tbl' # output file name

    write_all_lines(file_names1,file_names2, output_file)
    print(f"Successfully wrote all lines from each file to {output_file}")

if __name__ == '__main__':
    main()
