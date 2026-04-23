
from drug_harmonizer_with_purplebook import DrugHarmonizer
import unittest

class TestDateStandardization(unittest.TestCase):
    def setUp(self):
        # Create a mock/dummy harmonizer partially to access the method
        # We don't need to load real files for this unit test of the _standardize_dates method
        # But we need to bypass __init__ or handle it gracefully.
        # It's cleaner to just instantiate the class but mock the loaders or pass dummys if they don't fail immediately.
        # Since __init__ prints and loads, let's just create a dummy class with the method if possible, or
        # better yet, assume we can test it by calling it directly if we simulate a result dict.
        
        # Actually simplest is to just monkeypatch __init__ to do nothing
        original_init = DrugHarmonizer.__init__
        DrugHarmonizer.__init__ = lambda self, *args, **kwargs: None
        self.harmonizer = DrugHarmonizer()
        DrugHarmonizer.__init__ = original_init
        
    def test_date_parsing_sorting(self):
        # Input mixed dates
        result = {
            "recent_approval_dates": [
                "20230213",
                "20020522",
                "20131220",
                "25-AUG-14",
                "19-MAY-14",
                "Sep 28, 2023",
                "May 21, 2002",
                "01-JAN-10"
            ]
        }
        
        self.harmonizer._standardize_dates(result)
        
        dates = result['recent_approval_dates']
        first = result['first_approval_date']
        
        print("\nProcessed Dates (Newest First):")
        for d in dates:
            print(d)
        print(f"First Approval Date (Oldest): {first}")
        
        # Verification
        # Oldest date: May 21, 2002 (or 20020522 which is May 22, 2002)
        # 20020522 -> 2002-05-22
        # May 21, 2002 -> 2002-05-21
        # So oldest is 2002-05-21
        
        expected_first = "2002-05-21"
        self.assertEqual(first, expected_first)
        
        # Newest date: Sep 28, 2023 -> 2023-09-28
        self.assertEqual(dates[0], "2023-09-28")
        
        # Check sorting order
        self.assertTrue(all(dates[i] >= dates[i+1] for i in range(len(dates)-1)))
        
        # Check format
        for d in dates:
            self.assertRegex(d, r'^\d{4}-\d{2}-\d{2}$')

if __name__ == '__main__':
    unittest.main()
