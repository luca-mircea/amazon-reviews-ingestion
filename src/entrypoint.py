import argparse

from src import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a Python job")
    parser.add_argument(
        "--task",
        "-task",
        type=str,
        default=None,
        dest="task_name",
        help="Name of the task to execute",
    )
    parser.add_argument(
        "--start_timestamp",
        "-start_timestamp",
        type=str,
        default=None,
        help="Timestamp of the date interval start",
    )

    parser.add_argument(
        "--end_timestamp",
        "-end_timestamp",
        type=str,
        default=None,
        help="Timestamp of the date interval end",
    )

    args = parser.parse_args()

    task_name = args.task_name
    start_timestamp = args.start_timestamp
    end_timestamp = args.end_timestamp

    task = getattr(main, task_name)
    task(start_timestamp, end_timestamp)
