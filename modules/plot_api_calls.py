"""plot_api_calls - Plotting API call frequencies.

This module provides functions to visualize the frequency of API calls 
either on a per-minute or per-second basis.

Functions:
    plot_api_calls_per_minute(api_call_timestamps, title)
    plot_api_calls_per_second(api_call_timestamps, title)
"""

import matplotlib.pyplot as plt
import pandas as pd


def plot_api_calls_per_minute(
    api_call_timestamps, title="Number of API Calls Per Minute"
):
    """Plot the number of API calls per minute.

    Generates a time series plot that displays the frequency of API calls
    made per minute based on a list of timestamps.

    Args:
        api_call_timestamps (list of datetime.datetime): Timestamps when API calls were made.
        title (str, optional): Title of the plot. Defaults to 'Number of API Calls Per Minute'.

    Returns:
        None: Displays the plot.
    """

    df = pd.DataFrame(api_call_timestamps, columns=["timestamp"])
    df.set_index("timestamp", inplace=True)
    df["count"] = 1

    calls_per_minute = df.resample("T").size()

    plt.figure(figsize=(12, 6))
    calls_per_minute.plot()

    plt.title(title)
    plt.xlabel("Timestamp")
    plt.ylabel("Number of Calls")
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)

    yticks_max = calls_per_minute.max() + 2
    yticks_interval = yticks_max // 10 or 1
    plt.yticks(range(0, yticks_max, yticks_interval))

    plt.tight_layout()
    plt.show()


def plot_api_calls_per_second(
    api_call_timestamps, title="Number of API Calls Per Second"
):
    """Plot the number of API calls per second.

    Generates a time series plot that displays the frequency of API calls
    made per second based on a list of timestamps.

    Args:
        api_call_timestamps (list of datetime.datetime): Timestamps when API calls were made.
        title (str, optional): Title of the plot. Defaults to 'Number of API Calls Per Second'.

    Returns:
        None: Displays the plot.
    """

    df = pd.DataFrame(api_call_timestamps, columns=["timestamp"])
    df.set_index("timestamp", inplace=True)
    df["count"] = 1

    calls_per_second = df.resample("S").size()

    plt.figure(figsize=(12, 6))
    calls_per_second.plot()

    plt.title(title)
    plt.xlabel("Timestamp")
    plt.ylabel("Number of Calls")
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)

    yticks_max = calls_per_second.max() + 2
    yticks_interval = yticks_max // 10 or 1
    plt.yticks(range(0, yticks_max, yticks_interval))

    plt.tight_layout()
    plt.show()
