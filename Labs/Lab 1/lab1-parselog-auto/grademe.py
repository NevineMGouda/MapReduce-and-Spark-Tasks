import unittest
import os


cwd = os.getcwd()
TESTS_DIR=os.path.join(cwd,"test")

def main():

    test_modules = unittest.defaultTestLoader.discover(start_dir=TESTS_DIR, pattern='test*.py', top_level_dir=None)
    unittest.TextTestRunner().run(test_modules)





# Output the collected arguments



if __name__ == '__main__':
    main()
