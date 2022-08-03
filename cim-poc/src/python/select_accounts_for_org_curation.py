import persistence
import query
import pandas as pd
from fuzzywuzzy import fuzz
from typing import List


def sample_soldto_accounts_by_revenue_and_slash_bins(account_df : pd.DataFrame, num_bins : int = 10,
                                                     bin_size : int = 10, seed : int = 1):
    """ Takes a Data Frame of Sold-To accounts with account names and total revenue, and
        returns a representative sample by:
            1. whether the account is a slash account
            2. the revenue decile of the account relative to the set
    :param account_df: a Data Frame of Grainger Sold-To accounts with columns ACCOUNT_NAME and TOTAL_ORDER_DOLLARS
    :param num_bins: the number of bins you want to cut TOTAL_ORDER_DOLLARS into (default: 10)
    :param bin_size: the sample size out of each bin (with replacements)
    :param seed: the seed of the random sampler
    :return: sample_df - the aforementioned data frame
    """

    account_df['has_slash'] = account_df.ACCOUNT_NAME.str.contains('\\/')
    account_df['TOTAL_ORDER_DOLLARS'] = account_df['TOTAL_ORDER_DOLLARS'].astype('float')
    account_df['revenue_bin'] = pd.cut(account_df.TOTAL_ORDER_DOLLARS, bins=num_bins, right=False, labels=False)

    sample_df = account_df.groupby(['has_slash', 'revenue_bin']).sample(
                n=bin_size,
                random_state=seed,
                replace=True
            )
    return sample_df


def filter_soldto_accounts_by_track_code (account_df: pd.DataFrame, track_code: str, num_bins: int = 10):
    """ Given a DataFrame of Grainger Sold-To accounts (with track code), this returns
        the same list but filtered down to the specified track code
    :param account_df: a Data Frame of Grainger SAP Sold-To accounts (with account names)
    :param track_code: a 5-character SAP track code (e.g. AMAZON = X5QAA)
    :param num_bins: the number of bins we cut TOTAL_ORDER_DOLLARS by (relic of old function)
    :return:
    """
    account_df = account_df[account_df.SOLDTO_TRACK_CD == track_code].copy()

    account_df['has_slash'] = account_df.ACCOUNT_NAME.str.contains('\\/')
    account_df['TOTAL_ORDER_DOLLARS'] = account_df['TOTAL_ORDER_DOLLARS'].astype('float')
    account_df['revenue_bin'] = pd.cut(account_df.TOTAL_ORDER_DOLLARS, bins=num_bins, right=False, labels=False)

    return account_df


def split_slash_account_name_columns(account_df : pd.DataFrame):
    """ Given a DataFrame of Grainger Sold-To accounts, returns the same DataFrame with 2 columns
        representing the left and right half of account name in the event of a slash (/).
    :param account_df: a Data Frame of Grainger SAP Sold-To accounts (with account names)
    :return: split_slash_df -
    """
    account_df['AccountName_Org'] = account_df['ACCOUNT_NAME'].apply(lambda x: x.split('/')[0])

    slash_name_df = account_df[account_df.has_slash == True].copy()
    slash_name_df['AccountSlash_Org'] = slash_name_df['ACCOUNT_NAME'].apply(lambda x: x.split('/')[1])
    slash_name_df = slash_name_df[['SOLDTO_ACCOUNT', 'AccountSlash_Org']]

    split_slash_df = account_df.merge(slash_name_df, how='left', on='SOLDTO_ACCOUNT')
    return split_slash_df


def mode_of_email_domain(account_df: pd.DataFrame, exclusions: List[str]):
    """ Given a DataFrame of Grainger Sold-To accounts, returns the most common email domain
        in each account's Salesforce contacts.
    :param account_df: a Data Frame of Grainger SAP Sold-To accounts (with account names)
    :param exclusions: a list of email domains to exclude from the mode calculations
    :return: email_domain_df - the input DataFrame but with an added column for the most common
             contact email domain
    """
    qry = query.sf_contacts_email_domain_frequency(account_df['SOLDTO_ACCOUNT'].astype('str').unique(), exclusions)
    email_domain_count_df = persistence.get_df(qry)

    # whittle down the query to the highest distinct count for each unique email domain, then merge
    idx = email_domain_count_df.groupby(['SOLDTO_ACCOUNT'])['EMAIL_CT'].transform(max) == \
          email_domain_count_df['EMAIL_CT']
    email_domain_mode_df = email_domain_count_df[idx].copy()
    email_domain_df = account_df.merge(email_domain_mode_df, how='left',
                                       on='SOLDTO_ACCOUNT').drop_duplicates(subset=['SOLDTO_ACCOUNT'])

    return email_domain_df


def similar_account_names(account_number : str, account_df : pd.DataFrame, threshold: int = 0):
    """ Given an account with id account_number, a DataFrame of accounts that contains said account,
        and a fuzzy match threshold, this returns a DataFrame with all of the accounts besides the
        given account whose ACCOUNT_NAMES fuzzy match to the given account_number's ACCOUNT_NAME.

    :param account_number: a Grainger SAP Sold-To account number (starts with 08)
    :param account_df: a Data Frame of Grainger SAP Sold-To accounts (with account names)
    :param threshold: the fuzzy match threshold for gauging similarity of account names
    :return: a DataFrame of all accounts that are similar enough to the given account, with the same
             columns as the provided account_df
    """
    similarity_df = account_df.copy()
    try:
        account_name = str(similarity_df.loc[similarity_df.SOLDTO_ACCOUNT == account_number, 'ACCOUNT_NAME'].iat[0])
    except KeyError:
        raise KeyError('Account %s not found in account_df.' % account_number)

    similarity_df['FUZZY_RATIO'] = similarity_df.ACCOUNT_NAME.apply(lambda x: fuzz.ratio(x, account_name))
    similarity_df = similarity_df.sort_values('FUZZY_RATIO', ascending=False)

    return similarity_df[(similarity_df.FUZZY_RATIO >= threshold) & (similarity_df.SOLDTO_ACCOUNT != account_number)]


def amazon_account_name_regex_captures(account_df: pd.DataFrame):
    """ This adds proposed Org Name and Location Name columns for Amazon accounts with the most common
        account name format (used for 95% of Amazon accounts)
    :param account_df:
    :return:
    """
    regex_str = '((?:AMAZON)|(?:GOLDEN STATE)).*?((?:[A-Z]{3}[0-9]{1,3})|(?:K[A-Z]{3}))[\\s].*?(?:NON[-\\s]INV)'
    account_df[['AmazonRegexOrgNode', 'AmazonRegexLocationName']] = account_df['ACCOUNT_NAME'].str.extract(regex_str)

    account_df.loc[account_df['AmazonRegexLocationName'].isna(), 'AmazonRegexLocationName'] = ''
    account_df.loc[account_df['AmazonRegexLocationName'].str.contains('WFM'), 'AmazonRegexOrgNode'] = 'WHOLE FOODS'

    return account_df


if __name__ == "__main__":

    OUTPUT_FILEPATH = '~/Desktop/amazon_org_account_curation.xlsx'
    OUTPUT_COLS = ['SOLDTO_ACCOUNT', 'ACCOUNT_NAME', 'ACCOUNT_NAME2', 'AccountName_Org', 'AccountSlash_Org',
                   'SOLDTO_TRACK_CD_NAME', 'SOLDTO_SUBTRACK_CD_NAME', 'EMAIL_DOMAIN',
                   'SOLDTO_CITY', 'SOLDTO_STATE', 'SOLDTO_ZIP5', 'TOTAL_ORDER_DOLLARS', 'revenue_bin',
                   'AmazonRegexOrgNode', 'AmazonRegexLocationName']
    EXCLUSION_LIST = ['gmail.com', 'yahoo.com', 'grainger.com']

    curation_df = persistence.get_df(query.soldto_accounts_by_revenue())
    amazon_df = filter_soldto_accounts_by_track_code(curation_df, 'X5QAA')
    curation_df = split_slash_account_name_columns(amazon_df)
    curation_df = mode_of_email_domain(curation_df, EXCLUSION_LIST)
    curation_df = amazon_account_name_regex_captures(curation_df)

    curation_df = curation_df[OUTPUT_COLS]
    curation_df.drop_duplicates().to_excel(OUTPUT_FILEPATH, index=False)
