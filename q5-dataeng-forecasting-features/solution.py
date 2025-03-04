import pandas as pd
import argparse
import os


class DataLoader:
    """
    Handles loading and merging data from CSV files
    """

    def __init__(self, sales_file, product_file, brand_file, store_file):
        self.sales_file = sales_file
        self.product_file = product_file
        self.brand_file = brand_file
        self.store_file = store_file

    def load_data(self):
        # Read data from CSV files
        sales = pd.read_csv(self.sales_file, parse_dates=["date"])
        product = pd.read_csv(self.product_file)
        brand = pd.read_csv(self.brand_file)
        store = pd.read_csv(self.store_file)

        # Merge individual dataset to create one big data cube
        sales = sales.merge(product, left_on="product", right_on="id", suffixes=("", "_prod"))
        sales = sales.merge(brand, left_on="brand", right_on="name", suffixes=("", "_brand"))
        sales = sales.merge(store, left_on="store", right_on="id", suffixes=("", "_store"))

        return sales


class FeatureCalculator:
    """Calculates time series features such as moving averages and lags."""

    def __init__(self, sales_df):
        self.sales_df = sales_df

    def compute_features(self):
        df = self.sales_df.copy()
        df = df.rename(columns={"quantity": "sales_product", "product": "product_id", "store": "store_id", "id_brand": "brand_id"})

        # Product Features

        # MA7_P: Moving average of the sales of the product and the store in the past 7 days
        df["MA7_P"] = df.groupby(["product_id", "store_id"])["sales_product"].transform(lambda x: x.rolling(7, min_periods=1).mean())
        # LAG7_P: Lag features: 7 days earlier sales of the product and the store (sales on the same day last week)
        df["LAG7_P"] = df.groupby(["product_id", "store_id"])["sales_product"].shift(7)


        # Brand Features

        # sales_brand: Total sales of all the products from the same brand and the store
        df["sales_brand"] = df.groupby(["brand_id", "store_id", "date"])["sales_product"].transform("sum")
        # MA7_B: Moving average of the total sales of all the products from the same brand and the store in the past 7 days
        df["MA7_B"] = df.groupby(["brand_id", "store_id"])["sales_brand"].transform(lambda x: x.rolling(7, min_periods=1).mean())
        # LAG7_B: Lag features: 7 days earlier total sales of all the products from the same brand and the store (sales on the same day last week)
        df["LAG7_B"] = df.groupby(["brand_id", "store_id"])["sales_brand"].shift(7)


        ### Store Features

        # - sales_store: Total sales of the store
        df["sales_store"] = df.groupby(["store_id", "date"])["sales_product"].transform("sum")
        # - MA7_S: Moving average of the total sales of the store in the past 7 days
        df["MA7_S"] = df.groupby("store_id")["sales_store"].transform(lambda x: x.rolling(7, min_periods=1).mean())
        # - LAG7_S: Lag features: 7 days earlier total sales of the store (sales on the same day last week)
        df["LAG7_S"] = df.groupby("store_id")["sales_store"].shift(7)

        return df[["product_id", "store_id", "brand_id", "date", "sales_product", "MA7_P", "LAG7_P",
                   "sales_brand", "MA7_B", "LAG7_B", "sales_store", "MA7_S", "LAG7_S"]]


class WMAPECalculator:
    """
    Calculates WMAPE(Weighted Mean Absolute Percentage Error) for each product-store-brand group
    Link: https://www.baeldung.com/cs/mape-vs-wape-vs-wmape, https://en.wikipedia.org/wiki/WMAPE
    """

    def __init__(self, feature_df):
        self.feature_df = feature_df

    def compute_wmape(self):
        df = self.feature_df.dropna()
        df["abs_error"] = abs(df["sales_product"] - df["MA7_P"])

        wmape_df = df.groupby(["product_id", "store_id", "brand_id"]).apply(
            lambda x: (x["abs_error"].sum() / x["sales_product"].sum())
        ).reset_index(name="WMAPE")

        return wmape_df


class DataProcessor:
    """Handles the entire pipeline from data loading to feature extraction and WMAPE calculation."""

    def __init__(self, sales_file, product_file, brand_file, store_file, min_date, max_date, top_n):
        self.sales_file = sales_file
        self.product_file = product_file
        self.brand_file = brand_file
        self.store_file = store_file
        self.min_date = min_date
        self.max_date = max_date
        self.top_n = top_n

    def process(self):
        # Load and filter data
        loader = DataLoader(self.sales_file, self.product_file, self.brand_file, self.store_file)
        sales_df = loader.load_data()
        sales_df = sales_df[(sales_df["date"] >= self.min_date) & (sales_df["date"] <= self.max_date)]

        # Compute features
        feature_calculator = FeatureCalculator(sales_df)
        feature_df = feature_calculator.compute_features()
        feature_df = feature_df.sort_values(by=["product_id", "brand_id", "store_id", "date"])
        feature_df.to_csv("features.csv", index=False)
        print("First output written to: features.csv")

        # Compute WMAPE
        wmape_calculator = WMAPECalculator(feature_df)
        wmape_df = wmape_calculator.compute_wmape()
        wmape_df = wmape_df.sort_values(by="WMAPE", ascending=False).head(self.top_n)
        wmape_df.to_csv("mapes.csv", index=False)
        print("Second output written to: mapes.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate sales features and WMAPE from sales data.")
    parser.add_argument("--min-date", type=str, default="2021-01-08", help="Start date in YYYY-MM-DD format")
    parser.add_argument("--max-date", type=str, default="2021-05-30", help="End date in YYYY-MM-DD format")
    parser.add_argument("--top", type=int, default=5, help="Number of rows in the WMAPE output")

    args = parser.parse_args()
    prepath = "./q5-dataeng-forecasting-features/input_data/data/"
    processor = DataProcessor(os.path.join(prepath, "sales.csv"), 
                              os.path.join(prepath, "product.csv"), 
                              os.path.join(prepath, "brand.csv"), 
                              os.path.join(prepath, "store.csv"), 
                              args.min_date, 
                              args.max_date, 
                              args.top)
    processor.process()
    
