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