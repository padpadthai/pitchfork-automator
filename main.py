import logging.handlers
import sys
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from time import sleep
from timeit import default_timer as timer
from traceback import print_exc
from typing import Tuple, Iterator, List, Dict

from pymongo import InsertOne, MongoClient, UpdateOne
from pymongo.collection import Collection
from selenium import webdriver
from selenium.webdriver.firefox.webelement import FirefoxWebElement

from raw_review import parse_review_web_element, RawReview

logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s.%(msecs)03d %(thread)d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S',
                    level=logging.ERROR)

file_handler = logging.handlers.RotatingFileHandler("logs/app_{}.log".format(datetime.now().timestamp()))
formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(thread)d %(levelname)s {%(module)s} [%(funcName)s] %(message)s')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

log = logging.getLogger("main")
log.addHandler(file_handler)
log.setLevel(logging.INFO)

browser_automations = [

    # {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/5734-wheres-black-ben/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/5734-wheres-black-ben/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/5734-wheres-black-ben/": (10, 3)},
    # {"https://pitchfork.com/reviews/albums/5734-wheres-black-ben/": (10, 3)},

    {"https://pitchfork.com/reviews/albums/jason-molina-eight-gates/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/czardust-the-raw-material/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/a-boogie-wit-da-hoodie-hoodie-szn/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/shuta-hasunuma-u-zhaan-2-tone/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/23346-the-age-of-anxiety/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/22327-twin-peaks-ost/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/21208-night-of-your-ascension/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/20258-fortune/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/19209-school-of-language-old-fears/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/18201-john-vanderslice-dagger-beach-diamond-dogs/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/17070-lords-never-worry/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/16002-wale-ambition/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/14951-white-wilderness/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/14074-black-tambourine/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/13088-sketches-of-spain-legacy-edition/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/12099-the-rhumb-line/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/10780-necessary-evil/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/9813-find-shelter/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/262-peregrine/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/8296-22-20s/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/4771-the-libertines/": (1000, 50)},
    {"https://pitchfork.com/reviews/albums/7164-this-is-our-punk-rock-thee-rusted-satellites-gather-sing/": (1000, 50)},
]

log.info("Starting")
executor = ThreadPoolExecutor(8)


def execute_browser_automation(browser_automation: Dict):
    client = MongoClient()
    database = client.get_database("raw_pitchfork")
    collection = database.get_collection("album_reviews")
    raw_reviews = list()
    count = 0
    for value in automate_browser(list(browser_automation.values())[0][0], list(browser_automation.keys())[0]):
        raw_review = parse_review_web_element(value[0], value[1])
        raw_reviews.append(raw_review)
        count += 1
        if count % list(browser_automation.values())[0][1] == 0 and count > 0 and len(raw_reviews) > 0:
            write_to_mongo(raw_reviews, collection)
            raw_reviews.clear()
        log.debug("Processed review at '%s'", raw_review.url)
    if len(raw_reviews) > 0:
        write_to_mongo(raw_reviews, collection)
        raw_reviews.clear()


def write_to_mongo(raw_reviews: List[RawReview], collection: Collection):
    unique_reviews = {review.url: review for review in raw_reviews}
    current_urls = unique_reviews.keys()
    existing_mongo_urls = set(cursor['url'] for cursor in collection.find({"url": {"$in": list(current_urls)}}))
    new_urls = list(current_urls - existing_mongo_urls)
    if len(new_urls) > 0:
        insert_mongo_objects = [InsertOne(raw_review.get_dict_model()) for raw_review in unique_reviews.values() if
                                raw_review.url in new_urls]
        collection.bulk_write(insert_mongo_objects)
        log.debug("Inserted %s reviews in MongoDB", len(insert_mongo_objects))
    elif len(existing_mongo_urls) > 0:
        update_mongo_objects = [UpdateOne({"url": raw_review.url},
                                          {"$set": raw_review.get_dict_model()
                                           }) for raw_review in unique_reviews.values() if raw_review.url in existing_mongo_urls]
        collection.bulk_write(update_mongo_objects)
        log.debug("Updated %s reviews in MongoDB", len(update_mongo_objects))
    else:
        log.warning("No changes required in MongoDB")


def automate_browser(review_count: int, first_scrollable_url: str, browser_render_timeout: float = 5) -> Iterator[
    Tuple[str, FirefoxWebElement]]:
    browser = webdriver.Firefox()
    browser.get(first_scrollable_url)
    current_url = browser.current_url
    reviews_seen = 0
    scroll_start = 0
    while len(browser.find_elements_by_class_name(
            "review-detail")) > reviews_seen and reviews_seen < review_count:
        try:
            browser.execute_script("window.scrollTo(arguments[0], arguments[1]);", scroll_start,
                                   browser.find_element_by_css_selector("body").size["height"] + 50)
            reviews_seen += 1
            sleep(browser_render_timeout)
            review_detail = browser.find_element_by_css_selector(
                ".review-detail:nth-child({})".format(reviews_seen))
            current_url = browser.current_url
            scroll_start = review_detail.location["y"]
            yield current_url, review_detail
            log.debug("Automated review at %s", current_url)
            attempts = 1
            while len(browser.find_elements_by_class_name("review-detail")) <= reviews_seen and attempts < 4:
                timeout = 10 * attempts
                log.warning(
                    "Page not loaded after %d attempts. Timeout at url '%s' for %d seconds",
                    attempts, current_url, timeout)
                start = timer()
                while len(browser.find_elements_by_class_name("review-detail")) <= reviews_seen and (timer() - start) < timeout:
                    browser.execute_script("window.scrollTo(arguments[0], arguments[1]);", scroll_start,
                                           browser.find_element_by_css_selector("body").size["height"] + 50)
                    sleep(1)
                attempts += 1
        except BaseException as e:
            log.error("A problem occurred while automating review at '%s'", browser.current_url)
            print_exc()
    browser.close()
    browser.quit()
    log.info("Automated %s reviews of %s. Started at url '%s'. Finished at url '%s'", reviews_seen, review_count,
             first_scrollable_url, current_url)
    for index, browser_automation in enumerate(browser_automations):
        if list(browser_automation.keys())[0] == first_scrollable_url:
            browser_automations.remove(browser_automations[index])
            if reviews_seen < review_count and first_scrollable_url != current_url:
                log.info("Review count not reached. Resubmit reviews for processing, starting with url %s", current_url)
                browser_automation_new = {current_url: (list(browser_automation.values())[0][0] - reviews_seen - 1, list(browser_automation.values())[0][1])}
                browser_automations.append(browser_automation_new)
                log.debug("Replaced browser automation element %s with %s", browser_automation, browser_automation_new)
            else:
                log.info("Completed automating of %d reviews starting with url %s and ending with %s", reviews_seen, first_scrollable_url, current_url)


while len(browser_automations) > 0:
    future_list = [executor.submit(execute_browser_automation, browser_automation) for browser_automation in browser_automations]
    futures.wait(future_list)
log.info("Stopping")
