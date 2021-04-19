from ast import literal_eval
from typing import List, Set, Dict

import numpy as np
from pandas import read_csv, DataFrame, to_datetime

from db_reader import retrieve_and_save_searches_clicks_bookings

CSV_FILES_DIR = "csv_files"


def format_columns_of_lists(data: DataFrame) -> DataFrame:
    target_cols = [
        "clicked_listing_ids",
        "strict_listing_ids",
        "booking_page_listing_ids",
    ]
    for col in target_cols:
        data.loc[:, col] = data.loc[:, col].fillna("[]")
        # transform each row of the column from str to list of str
        data.loc[:, col] = data.loc[:, col].apply(literal_eval)
    return data


def calculate_mean_actioned_position(
    actioned_listing_ids: List[int],
    ranked_listing_ids: List[int],
) -> float:
    ranking_sum = 0
    for clicked_listing_id in actioned_listing_ids:
        ranking_sum += ranked_listing_ids.index(clicked_listing_id) + 1
    mean_click_position = ranking_sum / len(actioned_listing_ids)
    return mean_click_position


def get_booked_listing_ids_set(bookings: DataFrame) -> Set[int]:
    booked_listing_ids_set = set()
    booked_listing_ids = bookings.booking_page_listing_ids[bookings.search_day < to_datetime('2020-11-27')]
    for _, listing_ids in booked_listing_ids.iteritems():
        booked_listing_ids_set.update(listing_ids)
    return booked_listing_ids_set


def calculate_ranking_of_booked_listings(
    listing_id_to_rank_dict: Dict[int, int],
    booked_listing_ids_set: Set[int],
) -> float:
    rankings = []
    for listing_id, rank in listing_id_to_rank_dict.items():
        if listing_id in booked_listing_ids_set:
            rankings.append(rank)

    if rankings:
        return np.mean(rankings)
    else:
        return np.nan


def get_clicks_and_bookings(csv_file_name: str):
    df = read_csv(f"csv_files/{csv_file_name}")
    df = df[
        [
            "search_ts",
            "anonymous_id",
            "clicked_listing_ids",
            "strict_listing_ids",
            "booking_page_listing_ids",
        ]
    ]
    df.search_ts = to_datetime(df.search_ts)
    df["search_day"] = df.search_ts.values.astype('<M8[D]')
    df = format_columns_of_lists(df)
    df.strict_listing_ids = df.strict_listing_ids.apply(
        lambda listing_ids: [int(listing_id) for listing_id in listing_ids],
    )

    clicks = df[df.clicked_listing_ids.astype(bool)]
    mean_click_positions = clicks.apply(
        lambda row: calculate_mean_actioned_position(
            actioned_listing_ids=row.clicked_listing_ids,
            ranked_listing_ids=row.strict_listing_ids,
        ),
        axis=1,
    )
    clicks["mean_click_positions"] = mean_click_positions
    clicks["listing_id_to_rank_dicts"] = clicks.strict_listing_ids.apply(
        lambda listing_ids: {listing_id: idx + 1 for idx, listing_id in enumerate(listing_ids)},
    )

    bookings = df[df.booking_page_listing_ids.astype(bool)]
    mean_booked_positions = bookings.apply(
        lambda row: calculate_mean_actioned_position(
            actioned_listing_ids=row.booking_page_listing_ids,
            ranked_listing_ids=row.strict_listing_ids,
        ),
        axis=1,
    )
    bookings.loc[:, "mean_book_positions"] = mean_booked_positions

    booked_listing_ids_set = get_booked_listing_ids_set(bookings)
    print(booked_listing_ids_set)
    mean_ranking_of_booked_listings = clicks.listing_id_to_rank_dicts.apply(
        func=calculate_ranking_of_booked_listings,
        booked_listing_ids_set=booked_listing_ids_set,
    )
    clicks["mean_ranking_of_booked_listings"] = mean_ranking_of_booked_listings
    print(mean_ranking_of_booked_listings[~mean_ranking_of_booked_listings.isna()].head())

    return clicks, bookings


START_DATE = "2020-10-01"
END_DATE = "2021-04-15"

file_name = retrieve_and_save_searches_clicks_bookings(
    start_date=START_DATE,
    end_date=END_DATE,
)
clicks, bookings = get_clicks_and_bookings(file_name)
clicks.to_csv(f"csv_files/clicks_{START_DATE}__{END_DATE}.csv", index_label=False)
bookings.to_csv(f"csv_files/bookings_{START_DATE}__{END_DATE}.csv", index_label=False)
