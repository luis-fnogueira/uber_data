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