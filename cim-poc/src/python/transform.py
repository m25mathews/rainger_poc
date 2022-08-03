from curation_wizard import preprocess_loc_row


def dim_loc_str_row(row):
    """
    Transforms a row of DIM_LOCATION to a String
    :param row: Pandas row of DIM_LOCATION
    :return: String
    """
    items = [
        row.SOLD_ACCOUNT,
        # row.BILL_ACCOUNT,
        row.SHIP_ACCOUNT,
        row.TRACK_CODE,
        row.SUB_TRACK_CODE,
        row.DEPARTMENT,
        row.ATTENTION,
        row.SUPPLEMENTAL,
        row.RECEIVER,
        row.STREET_NUM,
        row.STREET,
        row.CITY,
        row.STATE,
        row.ZIP5,
        row.COUNTRY,
        # row.IDOMAIN
    ]
    result = " ".join([str(i) for i in items if i])
    return result


def dim_loc_str_no_acct_info(row):
    """
    Transforms a row of DIM_LOCATION to a String
    :param row: Pandas row of DIM_LOCATION
    :return: String
    """
    items = [
        row.DEPARTMENT,
        row.ATTENTION,
        row.SUPPLEMENTAL,
        row.RECEIVER,
        row.STREET_NUM,
        row.STREET,
        row.CITY,
        row.STATE,
        row.ZIP5,
        row.COUNTRY,
        row.IDOMAIN,
    ]
    result = " ".join([str(i) for i in items if i])
    return result


def dim_loc_str_simple(row):
    """
    Transforms a row of DIM_LOCATION to a simplified String for fuzzy matching with OPS_LOCATION
    without account info.
    :param row: Pandas row of DIM_LOCATION
    :return: String
    """
    items = [
        row.DEPARTMENT,
        row.ATTENTION,
        row.SUPPLEMENTAL,
        row.RECEIVER,
        row.STREET_NUM,
        row.STREET,
        row.CITY,
        row.STATE,
        row.ZIP5,
    ]
    result = " ".join([str(i) for i in items if i])
    return result


def dim_loc_str_address(row):
    """
    Transforms a row of DIM_LOCATION to a simplified String for fuzzy matching with OPS_LOCATION
    without account info.
    :param row: Pandas row of DIM_LOCATION
    :return: String
    """
    items = [
        row.STREET_NUM,
        row.STREET,
        row.CITY,
        row.STATE,
        row.ZIP5,
    ]
    result = " ".join([str(i) for i in items if i])
    return result

def ops_loc_str_simple_without_sublocation(row):
    items = [
        row.OPS_STREET,
        row.OPS_CITY,
        row.OPS_STATE,
        row.OPS_ZIP5,
    ]
    result = " ".join([str(i) for i in items if i])
    return result


def ops_loc_str_simple(row):
    items = [
        row.OPS_STREET,
        row.OPS_SUBLOCATION,
        row.OPS_CITY,
        row.OPS_STATE,
        row.OPS_ZIP5,
    ]
    result = " ".join([str(i) for i in items if i])
    return result

def dnb_loc_str_simple(row):
    items = [
        row.PHYS_STRT_AD,
        row.PHYS_CTY,
        row.PHYS_ST_ABRV,
        row.PHYS_ZIP5
    ]
    result = " ".join([str(i) for i in items if i])
    return result

def keepstock_loc_str_simple(row):
    items = [
        row.ADDRESS1,
        row.CITY,
        row.PROVINCE,
        row.ZIP5
    ]
    result = " ".join([str(i) for i in items if i])
    return result

def dim_loc_tokenize(
    row, column_to_dict={}, ignored_chars=preprocess_loc_row.IGNORED_CHARS
):
    tokens = set(
        token
        for field_tokens in (
            preprocess_loc_row.clean_and_tokenize_field(row[column], ignored_chars, column_to_dict.get(column, None))
            for column in row.index
        )
        for token in field_tokens
    )
    return tokens
