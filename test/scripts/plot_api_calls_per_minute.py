import matplotlib.pyplot as plt
import pandas as pd


def plot_api_calls_per_minute(
    api_call_timestamps, title="Number of API Calls Per Minute"
):
    """
    Plot the number of API calls per minute based on the provided timestamps.

    Parameters:
    - api_call_timestamps (list): List of timestamps when API calls were made.
    - title (str, optional): Title of the plot. Defaults to 'Number of API Calls Per Minute'.

    Returns:
    - None: Shows the plot.
    """

    # Convert timestamps to DataFrame
    df = pd.DataFrame(api_call_timestamps, columns=["timestamp"])
    df.set_index("timestamp", inplace=True)
    df["count"] = 1

    # Resample per minute and count the number of API calls
    calls_per_minute = df.resample("T").size()

    # Plot
    plt.figure(figsize=(12, 6))
    calls_per_minute.plot()

    plt.title(title)
    plt.xlabel("Timestamp")
    plt.ylabel("Number of Calls")
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)

    # Adjust the y-axis ticks for better readability
    yticks_max = calls_per_minute.max() + 2
    yticks_interval = yticks_max // 10 or 1
    plt.yticks(range(0, yticks_max, yticks_interval))

    plt.tight_layout()
    plt.show()


# Example Usage:
# plot_api_calls_per_minute(api_call_timestamps, "API Calls for Fetch Profile Data Per Minute")
