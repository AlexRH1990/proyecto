# Data Preprocessing with PySpark and Pandas

## Demo: 

Image


## Overview
This project demonstrates various data preprocessing techniques using both PySpark and Pandas. The preprocessing tasks include encoding categorical variables, scaling numerical data, and handling different data formats effectively. The project leverages the power of distributed data processing with PySpark while maintaining the simplicity and flexibility of Pandas.

## Key Files
- **`Prueba 1.ipynb`**: This Jupyter Notebook serves as the main entry point to the project. It demonstrates how to use the utilities defined in the `Library1.py` module to preprocess data. The notebook showcases various transformations applied to a sample dataset, illustrating the conversion and scaling of features using both PySpark and Pandas.

- **`PruebaX.ipynb`**: Another Jupyter Notebook that complements the preprocessing tasks. This notebook might include additional preprocessing steps, feature engineering, or data exploration tasks, building upon the functionalities provided in the `Library1.py` module.

- **`Library1.py`**: A Python module that encapsulates common data preprocessing operations into a utility class `Data_utils`. The methods provided include:
  - **`one_hot_pandas(dataframe, column_name)`**: Applies one-hot encoding to a specified column in a Pandas DataFrame.
  - **`ordinal_encoder(dataframe, column_name)`**: Applies ordinal encoding to a specified column in a Pandas DataFrame.
  - **`min_max_scaler_pandas(dataframe, columns_name)`**: Scales specified columns in a Pandas DataFrame using Min-Max scaling.
  - **`ordinal_encoder_pyspark(dataframe, list_column_names)`**: Applies ordinal encoding to specified columns in a PySpark DataFrame.
  - **`min_max_scaler_pyspark(dataframe, columns_name)`**: Scales specified columns in a PySpark DataFrame using Min-Max scaling.

- **`boston.csv`**: A dataset used in the project for demonstrating the preprocessing techniques. This dataset contains various features, likely related to real estate, which are processed using the utilities provided in `Library1.py`.

## Requirements
To run this project, you will need the following:

- Python 3.6 or later
- `pandas`
- `scikit-learn`
- `pyspark`
- Jupyter Notebook or JupyterLab (optional but recommended)

You can install the necessary dependencies with:

```bash
pip install pandas scikit-learn pyspark

