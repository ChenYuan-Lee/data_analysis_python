from utils import pull_from_snowflake


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
