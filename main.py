import logging.handlers
import sys
from concurrent.futures.process import ProcessPoolExecutor

from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from time import sleep
from typing import Tuple, Iterator, List

from pymongo import InsertOne, MongoClient, UpdateOne
from pymongo.collection import Collection

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.webelement import FirefoxWebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions

from raw_review import parse_review_web_element, RawReview

from traceback import print_exc

logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S',
                    level=logging.ERROR)

file_handler = logging.handlers.TimedRotatingFileHandler("logs/app_{}.log".format(datetime.now()))
formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s')
file_handler.setFormatter(formatter)

log = logging.getLogger("main")
log.addHandler(file_handler)
log.setLevel(logging.DEBUG)

browser_automation = [
    (10, "https://pitchfork.com/reviews/albums/jason-molina-eight-gates/", 5),
    (10, "https://pitchfork.com/reviews/albums/jason-molina-eight-gates/", 5),
    # (1000, "https://pitchfork.com/reviews/albums/jason-molina-eight-gates/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/czardust-the-raw-material/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/a-boogie-wit-da-hoodie-hoodie-szn/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/shuta-hasunuma-u-zhaan-2-tone/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/23346-the-age-of-anxiety/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/22327-twin-peaks-ost/", 50),
    # (1000, "https://pitchfork.com/artists/31484-wrekmeister-harmonies/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/20258-fortune/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/19209-school-of-language-old-fears/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/18201-john-vanderslice-dagger-beach-diamond-dogs/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/17070-lords-never-worry/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/16002-wale-ambition/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/14951-white-wilderness/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/14074-black-tambourine/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/13088-sketches-of-spain-legacy-edition/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/12099-the-rhumb-line/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/10780-necessary-evil/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/9813-find-shelter/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/262-peregrine/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/5734-wheres-black-ben/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/4771-the-libertines/", 50),
    # (1000, "https://pitchfork.com/reviews/albums/7164-this-is-our-punk-rock-thee-rusted-satellites-gather-sing/", 50),
]


def main():
    with ThreadPoolExecutor(6) as executor:
        executor.map(execute_browser_automation, browser_automation)


def execute_browser_automation(browser_automation_tuple: Tuple[int, str, int]):
    client = MongoClient()
    database = client.get_database("raw_pitchfork")
    collection = database.get_collection("album_reviews")
    raw_reviews = list()
    count = 0
    for value in automate_browser(browser_automation_tuple[0], browser_automation_tuple[1]):
        raw_review = parse_review_web_element(value[0], value[1])
        raw_reviews.append(raw_review)
        count += 1
        if count % browser_automation_tuple[2] == 0 and count > 0 and len(raw_reviews) > 0:
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


def automate_browser(review_count: int, first_scrollable_url: str, browser_render_timeout: float = 2) -> Iterator[
    Tuple[str, FirefoxWebElement]]:
    browser = webdriver.Firefox()
    browser.get(first_scrollable_url)
    reviews_processed = 0
    scroll_start = 0
    while len(browser.find_elements_by_class_name(
            "review-detail")) > reviews_processed and reviews_processed < review_count:
        try:
            browser.execute_script("window.scrollTo(arguments[0], arguments[1]);", scroll_start,
                                   browser.find_element_by_css_selector("body").size["height"])
            reviews_processed += 1
            WebDriverWait(browser, 30).until(
                expected_conditions.visibility_of_all_elements_located(
                    (By.CSS_SELECTOR, ".review-detail:nth-child({})".format(reviews_processed)))
            )
            sleep(browser_render_timeout)
            review_detail = browser.find_element_by_css_selector(
                ".review-detail:nth-child({})".format(reviews_processed))
            yield browser.current_url, review_detail
            scroll_start = review_detail.location["y"]
            log.debug("Automated review at %s", browser.current_url)
            if len(browser.find_elements_by_class_name("review-detail")) <= reviews_processed:
                log.warning("Next page not loaded - waiting at url '%s' for 15 seconds and attempting to trigger another reload",
                            browser.current_url)
                browser.execute_script("window.scrollTo(arguments[0], arguments[1]);", scroll_start,
                                       browser.find_element_by_css_selector("body").size["height"])
                sleep(15)
        except BaseException as e:
            log.error("A problem occurred while automating review at '%s'", browser.current_url)
            print_exc()
    log.info("Automated %s reviews of %s", reviews_processed, review_count)
    browser.close()
    browser.quit()


main()
