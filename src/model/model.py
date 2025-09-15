import time

import nextmv


class DecisionModel(nextmv.Model):
    def solve(self, input: nextmv.Input) -> nextmv.Output:
        """Solves the given problem and returns the solution."""

        nextmv.log("Solve happens here")

        start_time = time.time()
        # The decision model code would go here...

        statistics = nextmv.Statistics(
            run=nextmv.RunStatistics(duration=time.time() - start_time),
            result=nextmv.ResultStatistics(
                duration=None,
                value=123,
                custom={
                    "my_custom_statistic": 42,
                    "another_one": 3.14,
                },
            ),
        )

        sol_file = nextmv.csv_solution_file(
            "solution",
            data=[
                {"id": 1, "value": "A"},
                {"id": 2, "value": "B"},
                {"id": 3, "value": "C"},
            ],
        )

        return nextmv.Output(
            options=input.options,
            solution_files=[sol_file],
            statistics=statistics,
            output_format=nextmv.OutputFormat.MULTI_FILE,
        )
