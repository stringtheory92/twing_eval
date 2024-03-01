import pandas as pd
import matplotlib.pyplot as plt


def visualize_sim_ratio_bins(bins_df):
    bins = [float(col) for col in bins_df.columns[1:]]
    print("bins: ", bins)

    bin_sums = bins_df.sum(axis=0)[1:]

    plt.figure(figsize=(10, 6))
    plt.bar(bins, bin_sums)
    plt.xlabel("Bins")
    plt.ylabel("Sum of counts")
    plt.title("Sum of counts for each bin")
    plt.xticks(bins, rotation=45)
    plt.show()


def calculate_similarity_threshold(bins_df):
    """
    Based on column overlap distribution of current dataset:
    bin    count(bin)
    0.0    28
    0.1    12
    0.2     6
    0.3     9
    0.4    12
    0.5    33
    0.6     8
    0.7     4
    0.8     2
    0.9     3

    similarity threshold:
    calculated by maximum percent decrease from one bin to the next. For current dataset:
    0.0    28
    0.1    12    -57%
    0.2     6    -50%
    0.3     9
    0.4    12
    0.5    33
    0.6     8    -76%
    0.7     4    -50%
    0.8     2    -50%
    0.9     3

    0.6 is the threshold, therefore all tables contained in buckets 0.6 and greater are similar
    """
    bin_sums = bins_df.sum(axis=0)[1:]
    bin_sums_df = pd.DataFrame(bin_sums).reset_index()
    bin_sums_df.columns = ["bin", "count"]
    bin_sums_df["percent_change"] = bin_sums_df["count"].pct_change()

    min_percent_change_idx = bin_sums_df["percent_change"].idxmin()
    threshold = bin_sums_df.loc[min_percent_change_idx, "bin"]

    return threshold


def create_sim_ratio_bins_dataset(df):
    """
    bins increment by 0.1. first version lists tables within each bin for debug/extended analysis. second version uses counts for visualization

    *first bin should not include 0.0 values
    """

    # [ tables ] per bin
    bins = [i / 10 for i in range(11)]
    bins_df = pd.DataFrame(columns=bins)

    bins_df["table"] = df["table"]

    for col in bins_df.columns:
        if col != "table":
            bins_df[col] = [[] for _ in range(len(bins_df))]

    for table in df["table"]:
        for col in df.columns:

            if col != "table":
                sim_ratio = df[df["table"] == table][col].values[0]
                print("sr: ", table, sim_ratio)
                for i in range(len(bins) - 1):

                    # alternate - include 0.0 values in count
                    # if bins[i] <= sim_ratio < bins[i + 1]:
                    if sim_ratio != 0.0 and (bins[i] <= sim_ratio < bins[i + 1]):

                        if (
                            bins_df.at[
                                bins_df.index[bins_df["table"] == table][0], bins[i]
                            ]
                            is []
                        ):
                            bins_df.at[
                                bins_df.index[bins_df["table"] == table][0], bins[i]
                            ] = [col]
                        else:
                            bins_df.at[
                                bins_df.index[bins_df["table"] == table][0], bins[i]
                            ].append(col)

    bins_df = bins_df[["table"] + [bin_val for bin_val in bins[:-1]]]
    print("x: ", bins_df)

    bins_df.to_csv("bins_df.csv", index=False)

    # count [ tables ] per bin
    bins_count_df = pd.DataFrame(bins_df)

    # Iterate over each column (except 'table')
    for col in bins_count_df.columns:
        # Skip the 'table' column
        if col == "table":
            continue

        # Initialize a list to store counts for each bin
        bin_counts = []

        # Iterate over each bin
        for i in range(len(bins_count_df)):
            # Get the list of tables for the current bin
            tables = bins_count_df[col][i]

            # Count the number of tables in the bin
            count = len(tables)

            # Append the count to the list
            bin_counts.append(count)

        # Replace the list in the DataFrame with the counts
        bins_count_df[col] = bin_counts
    bins_count_df.to_csv("bins_count_df.csv", index=False)
    # print("bcdf: ", bins_count_df)

    return bins_count_df
