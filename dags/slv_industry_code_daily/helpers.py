from datetime import datetime


# A rough patch taking advantage of the fact that the table's schema rarely changes
# TODO: Sense schema change instead of execution date
def to_crawl_or_not_to_crawl(
    ex_date: str, start_date, crawl_task: str, placeholder_task: str
):
    """
    Crawls if start date == execution date
    Pass task ids as arguments!
    """
    must_crawl = False
    if datetime.strptime(ex_date, "%Y-%m-%d") == start_date:
        must_crawl = True
    return crawl_task if must_crawl else placeholder_task
