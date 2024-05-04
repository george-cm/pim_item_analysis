"""Load test data into the doc_test.db"""

import sys
from pathlib import Path

from main import main as run


def main() -> None:
    """Main function."""
    main_file: str = (Path(__file__).parent / "main.py").as_posix()
    sys.argv = [
        main_file,
        "load_pim_data",
        "c:/Users/e313532/Honeywell/PUBLIC SPS PIM Data Management - "
        "General/Projects/Doc Jose Mendes 2024-03-20/pim_data",
        "-dbf",
        "doc_test.db",
        "-l",
        "Doc Jose Mendes 2024-03-20",
    ]
    run()
    sys.argv = [
        main_file,
        "load_pim_data",
        "c:/Users/e313532/Honeywell/PUBLIC SPS PIM Data Management - "
        "General/Projects/DoC Documentation - Hungarian DOC 13728358/pim_data",
        "-dbf",
        "doc_test.db",
        "-l",
        "DoC Documentation - Hungarian DOC 13728358",
    ]
    run()
    sys.argv = [
        main_file,
        "load_doc_data",
        "c:/Users/e313532/Honeywell/PUBLIC SPS PIM Data Management - "
        "General/Projects/Doc Jose Mendes 2024-03-20/request_data",
        "2024-03-20",
        "-dbf",
        "doc_test.db",
        "-l",
        "Doc Jose Mendes 2024-03-20",
    ]
    run()
    sys.argv = [
        main_file,
        "load_doc_data",
        "c:/Users/e313532/Honeywell/PUBLIC SPS PIM Data Management - "
        "General/Projects/DoC Documentation - Hungarian DOC 13728358/request_data",
        "2024-04-30",
        "-dbf",
        "doc_test.db",
        "-l",
        "DoC Documentation - Hungarian DOC 13728358",
    ]
    run()
    sys.argv = [
        main_file,
        "list",
        "-dbf",
        "doc_test.db",
    ]
    run()


if __name__ == "__main__":
    # import timeit
    # from datetime import timedelta

    # print(str(timedelta(seconds=timeit.timeit(main, number=1))))
    main()
