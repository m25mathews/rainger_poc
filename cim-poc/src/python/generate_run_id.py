import time


# This class parses the input run information as well as generates run timestamps.
# Use this class to extract and generate the schema and run names.
class GenerateRunID:
    def __init__(self, initial_ts, incremental_ts, suffix, generate_ts):

        generated_ts = None
        if generate_ts:
            generated_ts = self.__get_timestamp()

        self.initial_ts, self.incremental_ts, self.suffix = None, None, None

        if suffix:
            self.suffix = self.__sanitize(suffix)

        if initial_ts:
            self.initial_ts = self.__sanitize(initial_ts)
        elif not initial_ts and generated_ts:
            self.initial_ts = generated_ts
        elif not initial_ts:
            raise ValueError("Initial timestamp is missing")

        if initial_ts and incremental_ts:
            self.incremental_ts = self.__sanitize(incremental_ts)
        elif initial_ts and not incremental_ts and generate_ts:
            self.incremental_ts = generated_ts

    @classmethod
    def parse(cls, input, generate_ts=True):
        input = cls.__sanitize(input)
        values = input.split("_")

        # default the values to None
        nones = [None] * 3
        first, second, third, *four = values + nones
        initial_ts, incremental_ts, suffix = nones

        # Remove all None from remainder
        four = list(filter(None, four))

        if len(four) > 0:
            raise ValueError("Invalid additional parts")

        # Parse the first parameter for either parent or suffix
        if first and first.isnumeric():
            initial_ts = first
        elif first and first.isalnum():
            suffix = first
        elif first and not first.isnumeric() and not first.isalnum():
            raise ValueError("Invalid first part")

        # Parse the second parameter for either suffix or ts
        if second and second.isnumeric():
            incremental_ts = second
        elif second and second.isalnum() and not suffix:
            suffix = second
        elif second and second.isalnum() and suffix:
            raise ValueError("Suffix already defined in the first part")
        elif second and not second.isnumeric() and not second.isalnum():
            raise ValueError("Invalid second part")

        # Parse the third parameter for suffix
        if third and third.isalnum() and not suffix:
            suffix = third
        elif third and third.isalnum() and suffix:
            raise ValueError("Suffix already defined earlier")
        elif third and not third.isalnum():
            raise ValueError("Invalid suffix")

        return cls(initial_ts, incremental_ts, suffix, generate_ts)

    def get_schema_name(self):
        """
        Gets the schema name and is always the intial_ts and suffix combined by _ and prefixed by _ and ignore None
        """
        schema = "_".join(filter(None, [self.initial_ts, self.suffix]))
        return "_" + self.__sanitize(schema)

    def get_run_name(self):
        """
        Gets the run name.
        For initial run, the generated timestamp is returned with suffix. This will be the same as schema name, in this case.
        For incremental run, the generated timesstamp is sandwiched between the original initial run and the suffix
        """
        name = "_".join(filter(None, [self.initial_ts, self.incremental_ts, self.suffix]))
        return self.__sanitize(name)

    @staticmethod
    def __get_timestamp():
        return str(int(time.time()))

    @staticmethod
    def __sanitize(value):
        return value.strip().strip("_") if value else ""
