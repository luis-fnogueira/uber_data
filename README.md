# Uber personal data
## The goal of this project is to extract, transform and load (ETL) personal data from Uber rides for data analysis

### 1. Where can one find your own dataset?

Go to https://myprivacy.uber.com/privacy/exploreyourdata/download, log in and click on **request your data**.

After a while (maybe a couple days), you'll receive an e-mail from Uber saying that your data is ready to be downloaded. Once you download it, you'll have a zip file with a few folders. We'll use the **Rider/trips_data.csv**

[![image](https://i.imgur.com/jQuegIA.png)]

### 2. A few considerations

This specific project was done by me, for my specific personal data. Therefore, the transformation step may not work for your own analysis. But, if you know Python, it'll be simple to make your own adjustments!

I did not shared my own data on the samples that are in the dags folder, they are a few samples datasets that do not correspond to a real user. Its purpose is to provide to the developer an idea of the raw data and the final outcome.

### 3. The code
#### 3.1 Overview
So, let's finally understand what's been done here!

After extracting the files from Zip, we dump it in a raw_table on Postgres. Then it's necessary to read from this table, make the transformations in Python (using Pandas) and save it in another csv file called transformed_data.csv. That done, we load it in a table called structured_data.

![image](https://i.imgur.com/UI61NbD.png)

#### 3.2 Extract step

After creating the tables in Postgres we're ready to extract the data. But there's a tiny problem in timestamp columns: they have a **"+0000 UTC"** that doesn't tell us nothing and it's not supported in timestamp data type in Postgres, like this:

| Request time|
| :---        |
| 2022-12-03 13:28:36 +0000 UTC| 

So, we need to remove it in order to insert data in the database. The `remove_tmz` function does that for us, we just need to pass a list with the columns that need to be transformed, the dataframe and voilà!

`df[cols] = df[cols].apply(lambda s: s.str.replace("\+0000 UTC", ""))`

That done, data is can be loaded in the raw_data table! It's done by the `send_data` function, which in turn uses the `to_sql` function of Pandas:

`data.to_sql(
    name=table_name,
    con=create_engine(self.URI),
    schema=schema,
    if_exists="append",
    index=False,
    dtype={},
)`