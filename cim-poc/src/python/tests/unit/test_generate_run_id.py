import unittest

from generate_run_id import GenerateRunID


class RunTestCase(unittest.TestCase):

    def test_run_with_generated_ids(self):
        # generates initial ts
        run = GenerateRunID.parse("prod", True)
        self.assertTrue(run.initial_ts.isnumeric())
        self.assertIsNone(run.incremental_ts)
        self.assertEqual(run.suffix, "prod")

        # generates incremental ts
        run = GenerateRunID.parse("1656892018_prod", True)
        self.assertEqual(run.initial_ts, "1656892018")
        self.assertTrue(run.incremental_ts.isnumeric())
        self.assertEqual(run.suffix, "prod")

        # all values present, no ts generation required
        run = GenerateRunID.parse("1656892018_165689399_prod")
        self.assertEqual(run.initial_ts, "1656892018")
        self.assertEqual(run.incremental_ts, "165689399")
        self.assertEqual(run.suffix, "prod")

        # no values present, only initial ts is generated
        run = GenerateRunID.parse("", True)
        self.assertTrue(run.initial_ts.isnumeric())
        self.assertIsNone(run.incremental_ts)
        self.assertIsNone(run.suffix)

    def test_run_with_no_generated_ids(self):
        # no initial ts provided resulting in error
        self.assertRaises(ValueError, GenerateRunID.parse, "prod", False)

        # just parse initial ts and suffix
        run = GenerateRunID.parse("1656892018_prod", False)
        self.assertEqual(run.initial_ts, "1656892018")
        self.assertIsNone(run.incremental_ts)
        self.assertEqual(run.suffix, "prod")

        # just parse initial ts, incremental ts and suffix
        run = GenerateRunID.parse("1656892018_165689399_prod", False)
        self.assertEqual(run.initial_ts, "1656892018")
        self.assertEqual(run.incremental_ts, "165689399")
        self.assertEqual(run.suffix, "prod")

        # no values present and no generation is requested resulting in error
        self.assertRaises(ValueError, GenerateRunID.parse, "", False)

        # Garbage inputs handling
        # Invalid Suffix
        self.assertRaises(ValueError, GenerateRunID.parse, "1656892018_165689399_p!@#rod", False)

        # Invalid Initial ts - Initial timestamp is missing error
        self.assertRaises(ValueError, GenerateRunID.parse, "1!@#656892018_165689399_prod", False)

        # Invalid Incremental ts
        self.assertRaises(ValueError, GenerateRunID.parse, "1656892018_1!@#65689399_prod", False)

        # Invalid Additional arguments
        self.assertRaises(ValueError, GenerateRunID.parse, "1656892018_165689399_165685896_prod", False)

        # Invalid format due to duplicate suffixes in multiple places
        self.assertRaises(ValueError, GenerateRunID.parse, "1656892018_partA_partB", False)
        self.assertRaises(ValueError, GenerateRunID.parse, "partA_1656892018_partB", False)
        self.assertRaises(ValueError, GenerateRunID.parse, "partA_partB_partC", False)

    def test_schema_name(self):
        run = GenerateRunID.parse("1656892018")
        self.assertEqual(run.get_schema_name(), '_1656892018')

        run = GenerateRunID.parse("1656892018_1656893999")
        self.assertEqual(run.get_schema_name(), '_1656892018')

        run = GenerateRunID.parse("1656892018_prod")
        self.assertEqual(run.get_schema_name(), '_1656892018_prod')

        run = GenerateRunID.parse("1656892018_1656893999_prod")
        self.assertEqual(run.get_schema_name(), '_1656892018_prod')

        run = GenerateRunID.parse("_1656892018_prod")
        self.assertEqual(run.get_schema_name(), '_1656892018_prod')

        run = GenerateRunID.parse(" _1656892018_prod ")
        self.assertEqual(run.get_schema_name(), '_1656892018_prod')

        run = GenerateRunID.parse("_prod ")
        self.assertEqual(run.get_schema_name(), '_' + run.initial_ts + '_prod')

        run = GenerateRunID.parse("")
        self.assertEqual(run.get_schema_name(), '_' + run.initial_ts)

    def test_run_name(self):
        run = GenerateRunID.parse("1656892018")
        self.assertEqual(run.get_run_name(), '1656892018_' + run.incremental_ts)

        run = GenerateRunID.parse("1656892018_1656893999")
        self.assertEqual(run.get_run_name(), '1656892018_1656893999')

        run = GenerateRunID.parse("1656892018_prod")
        self.assertEqual(run.get_run_name(), '1656892018_' + run.incremental_ts + '_prod')

        run = GenerateRunID.parse("1656892018_1656893999_prod")
        self.assertEqual(run.get_run_name(), '1656892018_1656893999_prod')

        run = GenerateRunID.parse("  ___1656892018_prod_ ")
        self.assertEqual(run.get_run_name(), '1656892018_' + run.incremental_ts + '_prod')

        run = GenerateRunID.parse("prod")
        self.assertEqual(run.get_run_name(), run.initial_ts + '_prod')

        run = GenerateRunID.parse(None)
        self.assertEqual(run.get_run_name(), run.initial_ts)

