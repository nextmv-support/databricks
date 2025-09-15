import nextmv
import pandas as pd

from src.model import DecisionModel


def main() -> None:
    """Entry point for the program."""

    options = nextmv.Options(
        nextmv.Option("sample-option", str, "", "Sample option - replace me.", False),
    )

    sample_input_file = nextmv.DataFile(
        name="sample-input.csv",
        loader=lambda path: pd.read_csv(path),
        input_data_key="sample",
    )

    input = nextmv.load(
        options=options,
        path="inputs",
        data_files=[sample_input_file],
        input_format=nextmv.InputFormat.MULTI_FILE,
    )
    model = DecisionModel()
    output = model.solve(input)
    nextmv.write(output, path="outputs")


if __name__ == "__main__":
    main()
