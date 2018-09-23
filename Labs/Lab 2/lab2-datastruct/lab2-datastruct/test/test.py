from solution import *
from grademe import *
# class Lab1Test(unittest.TestCase):
#     @grade(5)
#     def test_getFileNameBad(self):
#         f = getFileName()
#         expect(f).to(equal('Bar'))
#
#     @grade(5)
#     def test_getFileName(self):
#         f = getFileName()
#         expect(f).to(equal('abc.txt'))
#

class Lab2(unittest.TestCase):
    #@grade(5)
    def testTupleRecordsToDictRecords(self):
        tuples =[
                ("Mahmoud", 21, "student"),
                ("Heba", 34, "teacher")
        ]
        labels = ["name", "age", "job"]
        dictRecords = tupleRecordsToDictRecords(tuples, labels)
        answer = [
                {"name":"Mahmoud", "age":21, "job":"student"},
                {"name":"Heba", "age":34, "job":"teacher"}
        ]
        self.assertEqual(dictRecords,answer)

    #@grade(2.5)
    def testkeyByIndex(self):
        tuples=[("Mahmoud", 21, "student"),
                ("Heba", 34, "teacher")]
        keyed = keyByKey(tuples,0)
        keyedCorrect=[("Mahmoud", ("Mahmoud", 21, "student")),
                    ("Heba", ("Heba", 34, "teacher"))]
        self.assertEqual(keyed, keyedCorrect)


        keyed = keyByKey(tuples, 1)
        keyedCorrect = [(21, ("Mahmoud", 21, "student")),
                        (34, ("Heba", 34, "teacher"))]
        self.assertEqual(keyed, keyedCorrect)


        keyed = keyByKey(tuples, 2)
        keyedCorrect = [( "student", ("Mahmoud", 21, "student")),
                        ("teacher", ("Heba", 34, "teacher"))]
        self.assertEqual(keyed, keyedCorrect)


    #grade(2.5)
    def testkeyByKey(self):
        dicts = [
            {"name": "Mahmoud", "age": 21, "job": "student"},
            {"name": "Heba", "age": 34, "job": "teacher"}
        ]

        keyed = keyByIndex(dicts, "name")
        keyedCorrect = [("Mahmoud", {"name": "Mahmoud", "age": 21, "job": "student"}),
                        ("Heba", {"name": "Heba", "age": 34, "job": "teacher"})]
        self.assertEqual(keyed, keyedCorrect)

        keyed = keyByIndex(dicts, "age")
        keyedCorrect = [(21, {"name": "Mahmoud", "age": 21, "job": "student"}),
                        (34, {"name": "Heba", "age": 34, "job": "teacher"})]
        self.assertEqual(keyed, keyedCorrect)

        keyed = keyByIndex(dicts, "job")
        keyedCorrect = [("student", {"name": "Mahmoud", "age": 21, "job": "student"}),
                        ("teacher", {"name": "Heba", "age": 34, "job": "teacher"})]
        self.assertEqual(keyed, keyedCorrect)


    def testCountByKey(self):
        records = [("a", 3), ("b", 7), ("a", [1, 2, 4]), ("b", "hello"), ("a", {1:2, 3:5})]
        answer = countByKey(records)
        correct = {"a":3, "b":2}
        self.assertEqual(answer,correct)


    def testGroupByKey(self):
        records = [("a", 3), ("b", 7), ("a", [1, 2, 4]), ("b", "hello"), ("a", {1: 2, 3: 5})]
        answer = groupByKey(records)
        correct = {"a": [3, [1,2,4], {1: 2, 3: 5}], "b": [7,"hello"]}
        self.assertEqual(answer, correct)



