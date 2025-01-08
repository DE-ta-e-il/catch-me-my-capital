def to_crawl_or_not_to_crawl(must_crawl: bool, crawl_task: str, placeholder_task: str):
    """
    Crawls if start date == execution date
    Pass task ids as arguments!
    """
    return crawl_task if must_crawl else placeholder_task
