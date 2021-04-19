from dotenv import load_dotenv

from utils import pull_from_snowflake, retrieve_and_save_data


def retrieve_listings():
    query = """
        SELECT
            id AS listing_id,
            bedrooms,
            bathrooms,
            sqft_livable,
            monthly_price
        FROM PLATFORM.PROD.LISTINGS
        WHERE status = 'active'
    """
    return pull_from_snowflake(query)


def retrieve_leases():
    query = """
        SELECT
            listing_id,
            lease_start,
            lease_end,
            status
        FROM PLATFORM.PROD.LEASES
        WHERE status = 'approved'
    """
    return pull_from_snowflake(query)


def get_searches_clicks_bookings_query(
    data_retrieval_start_date: str,
    data_retrieval_end_date: str,
):
    return f"""
        WITH
        search AS (
            SELECT
                created_at AS search_ts,
                id AS search_id,
                anonymous_id,
                price_min AS min_price_filter,
                price_max AS max_price_filter,
                rect_north,
                rect_south,
                rect_east,
                rect_west,
                parking AS parking_filter,
                pets_allowed AS pets_filter,
                laundry AS laundry_filter,
                listing_ids AS strict_listing_ids,
                bedrooms AS bedrooms_filter,
                start_date::date - created_at::date AS lead_time_in_days,
                COALESCE(start_date, end_date) IS NOT NULL AS has_date_filter
            FROM
                PLATFORM.HISTORICALS.LISTING_SEARCH_LOGS
            WHERE
                strict_listing_ids IS NOT NULL
                AND (user_id NOT IN (SELECT user_id FROM platform.prod.employees) OR user_id IS NULL)
                AND search_ts >= '{data_retrieval_start_date}'
                AND search_ts <= '{data_retrieval_end_date}'
                AND (lead_time_in_days IS NULL OR lead_time_in_days >= 0)
                AND anonymous_id IS NOT NULL
        ),
        click AS (
            SELECT
                received_at AS click_ts,
                anonymous_id,
                listing_id AS clicked_listing_id,
                context_page_url
            FROM
                PLATFORM.JAVASCRIPT.LISTING_READ_VIEWED
            WHERE
                clicked_listing_id IS NOT NULL
                AND (user_id NOT IN (SELECT user_id FROM platform.prod.employees) OR user_id IS NULL)
                AND click_ts >= '{data_retrieval_start_date}'
                AND click_ts <= '{data_retrieval_end_date}'
        ),
        booking_page AS (
            SELECT
                anonymous_id,
                received_at AS booking_page_ts,
                context_page_referrer,
                listing_id AS booking_page_listing_id
            FROM
                PLATFORM.JAVASCRIPT.BOOKINGS_V3_NEW_PAGE_LOAD
            WHERE
                (user_id NOT IN (SELECT user_id FROM platform.prod.employees) OR user_id IS NULL)
                AND booking_page_ts >= '{data_retrieval_start_date}'
                AND booking_page_ts <= '{data_retrieval_end_date}'
        ),
        search_click_book AS (
            SELECT
                MAX(search_ts) AS search_ts,
                clicked_listing_id,
                booking_page_listing_id,
                search.anonymous_id AS anonymous_id,
                search_id, bedrooms_filter, rect_north, rect_south, rect_east, rect_west, min_price_filter, max_price_filter, parking_filter, pets_filter, laundry_filter, has_date_filter, lead_time_in_days, strict_listing_ids
            FROM
                search
                INNER JOIN click
                    ON search.anonymous_id = click.anonymous_id
                    AND TIMESTAMPDIFF(MINUTE, search_ts::TIMESTAMP_NTZ, click_ts::TIMESTAMP_NTZ) BETWEEN 0 AND 15
                    AND ARRAY_CONTAINS(click.clicked_listing_id::TEXT::VARIANT, search.strict_listing_ids)
                LEFT JOIN booking_page
                    ON click.anonymous_id = booking_page.anonymous_id
                    AND TIMESTAMPDIFF(MINUTE, search_ts::TIMESTAMP_NTZ, booking_page_ts::TIMESTAMP_NTZ) BETWEEN 0 AND 30
                    AND click.context_page_url = booking_page.context_page_referrer
            GROUP BY
                clicked_listing_id,
                booking_page_listing_id,
                search.anonymous_id,
                search_id, bedrooms_filter, rect_north, rect_south, rect_east, rect_west, min_price_filter, max_price_filter, parking_filter, pets_filter, laundry_filter, has_date_filter, lead_time_in_days, strict_listing_ids
        )
        SELECT
            ARRAYAGG(clicked_listing_id) AS clicked_listing_ids,
            ARRAYAGG(booking_page_listing_id) AS booking_page_listing_ids,
            search_ts,
            anonymous_id,
            search_id, bedrooms_filter, rect_north, rect_south, rect_east, rect_west, min_price_filter, max_price_filter, parking_filter, pets_filter, laundry_filter, has_date_filter, lead_time_in_days, strict_listing_ids
        FROM
            search_click_book
        GROUP BY
            search_ts,
            anonymous_id,
            search_id, bedrooms_filter, rect_north, rect_south, rect_east, rect_west, min_price_filter, max_price_filter, parking_filter, pets_filter, laundry_filter, has_date_filter, lead_time_in_days, strict_listing_ids
    """


def retrieve_and_save_searches_clicks_bookings(
    start_date: str,
    end_date: str,
) -> str:
    load_dotenv()
    query = get_searches_clicks_bookings_query(
        data_retrieval_start_date=start_date,
        data_retrieval_end_date=end_date,
    )
    file_name = f"searches_clicks_bookings_{start_date}__{end_date}.csv"
    retrieve_and_save_data(
        query=query,
        file_name=file_name
    )
    return file_name
