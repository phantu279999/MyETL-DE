import unittest
import pandas as pd
from etl.transform import clean_data, merge_data

class TestTransform(unittest.TestCase):

    def test_clean_data(self):
        df = pd.DataFrame({
            'id': [1, 2, 2],
            'value': [10, None, 20]
        })
        cleaned_df = clean_data(df)
        self.assertEqual(len(cleaned_df), 2)
        self.assertFalse(cleaned_df.isnull().values.any())

    def test_merge_data(self):
        df1 = pd.DataFrame({'id': [1, 2], 'value1': [10, 20]})
        df2 = pd.DataFrame({'id': [1, 2], 'value2': [100, 200]})
        merged_df = merge_data(df1, df2, 'id')
        self.assertIn('value2', merged_df.columns)
        self.assertEqual(len(merged_df), 2)

if __name__ == '__main__':
    unittest.main()
