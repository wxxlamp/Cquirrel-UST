# Operation Guide

**Attention**: Your local machine must have Python 3.9 or higher installed.

1. **Install Dependencies**: Run `pip install` to install the required Python dependencies.
2. **Generate Data**: Execute `python data_generate.py` to generate each table. The data size is determined by the parameters in the code.
3. **Merge Data**: Merge all data into `tpch_q3.tbl` by running `python data_merge.py`. This will include both `INSERT` and `DELETE` commands.
4. **Validate Results**: After executing the Flink task, validate the results by running `python data_check.py`.