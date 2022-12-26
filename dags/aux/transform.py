import re
import pandas as pd

class Transform:

    @staticmethod
    def remove_tmz(cols: list, df: pd.DataFrame) -> None:

        """
        This functions remove a substring from the uber_data dataframe.
        
        Args:

            cols: list. The columns that need to be applied
            df: Pandas Dataframe. The df where the data come from.

        Returns:

            It changes data in a dataframe.
        
        """

        df[cols] = df[cols].apply(lambda s: s.str.replace("\+0000 UTC", ""))


    @staticmethod
    def remove_not_completed(dataframe: pd.DataFrame) -> None:

        """
        This function removes the trips that were not completed.

        Args:
            dataframe: pd.Dataframe. The dataframe itself

        Returns:

            Drops non completed rows        
        """

        for index in dataframe.index:

            # Removing non completed trips
            dataframe.drop(dataframe[dataframe['Trip or Order Status'] != "COMPLETED"].index, inplace = True)


    @staticmethod
    def types_all_upper(dataframe: pd.DataFrame) -> None:

        """
        Turning every Product Type in Upper case, so all UberX rides fit in the same category
        
        Args:
            Dataframe: pd.DataFrame. The Dataframe that'll be changed
        Returns:
            Changes every product type to upper case
        """

        dataframe['Product Type'] = dataframe['Product Type'].apply(lambda s: str.upper(s))


    @staticmethod
    def check_city(dataframe: pd.DataFrame) -> None:

        """
        This functions checks if there is the string "Jundiaí" in the Address column.
        If true, it'll change the city column to Jundiaí as well.
        
        Args:
            Dataframe: pd.DataFrame. The Dataframe that'll be changed
        Returns:
            Changes the City column based on a condition
        """

        for index in dataframe.index:

            if re.search("Jundiaí", dataframe.loc[index, "Dropoff Address"]):
                dataframe.loc[index, "City"] = "Jundiaí"